package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/Dominator/lib/mdb/mdbd"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/apps/scotty/splash"
	"github.com/Symantec/scotty/consul"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/influx"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/scotty/pstore/tsdb"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/sysmemory"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/scotty/tsdbexec"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"log"
	"math"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"path"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	kPStoreIteratorName       = "pstore"
	kCollectorIteratorName    = "collector"
	kLookAheadWritingToPStore = 5
	kLeaseSpan                = 60
)

var (
	fPort = flag.Int(
		"portNum",
		6980,
		"Port number for scotty.")
	fTsdbPort = flag.Int(
		"tsdbPortNum",
		4242,
		"OpenTSDB Port number for scotty.")
	fBytesPerPage = flag.Uint(
		"bytesPerPage",
		1024,
		"Space for new metrics for each endpoint in records")
	fPageCount = flag.Uint(
		"pageCount",
		30*1000*1000,
		"Total page count")
	fMdbFile = flag.String(
		"mdbFile",
		"/var/lib/scotty/mdb",
		"Name of file from which to read mdb data.")
	fPollCount = flag.Uint(
		"concurrentPolls",
		0,
		"Maximum number of concurrent polls. 0 means no limit.")
	fConnectionCount = flag.Uint(
		"connectionCount",
		collector.ConcurrentConnects(),
		"Maximum number of concurrent connections")
	fCollectionFrequency = flag.Duration(
		"collectionFrequency",
		30*time.Second,
		"Amount of time between metric collections")
	fPStoreUpdateFrequency = flag.Duration(
		"pstoreUpdateFrequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
	fPersistentStoreType = flag.String(
		"persistentStoreType",
		"kafka",
		"Type of persistent store must be either 'kafka','influx','tsdb', or 'none'")
	fPidFile = flag.String(
		"pidfile", "", "Name of file to write my PID to")
	fThreshhold = flag.Float64(
		"inactiveThreshhold", 0.1, "Ratio of inactive pages needed to begin purging inactive pages")
	fDegree = flag.Uint(
		"degree", 10, "Degree of btree")
	fConfigDir = flag.String(
		"configDir", "/etc/scotty", "Directory for scotty config files.")
	fCoord = flag.String(
		"coordinator", "", "Leadership election specifications")
)

// nameSetType represents a set of strings. Instances of this type
// are mutable.
type nameSetType map[string]bool

type byHostName messages.ErrorList

func (b byHostName) Len() int {
	return len(b)
}

func (b byHostName) Less(i, j int) bool {
	return b[i].HostName < b[j].HostName
}

func (b byHostName) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

type connectionErrorsType struct {
	lock     sync.Mutex
	errorMap map[*collector.Endpoint]*messages.Error
}

func newConnectionErrorsType() *connectionErrorsType {
	return &connectionErrorsType{
		errorMap: make(map[*collector.Endpoint]*messages.Error),
	}
}

func (e *connectionErrorsType) Set(
	m *collector.Endpoint, err error, timestamp time.Time) {
	newError := &messages.Error{
		HostName:  m.HostName(),
		Timestamp: duration.SinceEpoch(timestamp).String(),
		Error:     err.Error(),
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.errorMap[m] = newError
}

func (e *connectionErrorsType) Clear(m *collector.Endpoint) {
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.errorMap, m)
}

func (e *connectionErrorsType) GetErrors() (result messages.ErrorList) {
	e.lock.Lock()
	result = make(messages.ErrorList, len(e.errorMap))
	idx := 0
	for endpoint := range e.errorMap {
		result[idx] = e.errorMap[endpoint]
		idx++
	}
	e.lock.Unlock()
	sort.Sort(byHostName(result))
	return
}

// a totalCountType instance updates the total count of values to write in a
// parituclar pstoreHandlerType instance.
// Unlike pstoreHandlerType, these instances are safe to use with
// multiple goroutines with one caveat. See documentation for Update.
type totalCountType struct {
	metrics    *pstore.ConsumerMetricsStore
	name       string
	rollUpSpan time.Duration
}

func (t *totalCountType) iteratorName() string {
	return fmt.Sprintf("%s/%s", kCollectorIteratorName, t.name)
}

// Update adds any new, uncounted values in for given endpointId in given
// store to this total count.
//
// Although these instances are safe to use with multiple gorutines,
// multiple goroutines should not call Update on the same instance for
// the same store and endpointId as this would cause data races over what
// values are new and uncounted. However, it is fine for multiple goroutines
// to call Update on the same instance for different endpointIds.
func (t *totalCountType) Update(
	theStore *store.Store, endpointId interface{}) {
	var r store.Record
	var tempCount uint64
	iterator, _ := createNamedIterator(
		theStore, endpointId, t.iteratorName(), t.rollUpSpan)
	for iterator.Next(&r) {
		if t.metrics.Filter(&r) {
			tempCount++
		}
	}
	iterator.Commit()
	t.metrics.AddToRecordCount(tempCount)
}

type visitorMetricsType struct {
	TimeLeft time.Duration
	Blocked  bool
}

type visitorMetricsStoreType struct {
	lock    sync.Mutex
	metrics visitorMetricsType
}

func (v *visitorMetricsStoreType) Metrics(metrics *visitorMetricsType) {
	v.lock.Lock()
	defer v.lock.Unlock()
	*metrics = v.metrics
}

func (v *visitorMetricsStoreType) SetTimeLeft(timeLeft time.Duration) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.metrics.TimeLeft = timeLeft
}

func (v *visitorMetricsStoreType) MaybeIncreaseTimeLeft(
	timeLeft time.Duration) {
	v.lock.Lock()
	defer v.lock.Unlock()
	if timeLeft > v.metrics.TimeLeft {
		v.metrics.TimeLeft = timeLeft
	}
}

func (v *visitorMetricsStoreType) SetBlocked(b bool) {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.metrics.Blocked = b
}

// pstoreHandlerType implements store.Visitor.
// Its Visit method writes the latest values for each endpoint to
// persistent storage. It uses a pstore.Consumer to buffer writes to
// persistnt storage and to avoid writing the same value twice.
// pstoreHandlerType is NOT threadsafe.
type pstoreHandlerType struct {
	consumer            *pstore.ConsumerWithMetrics
	appList             *datastructs.ApplicationList
	startTime           time.Time
	totalTimeSpentDist  *tricorder.CumulativeDistribution
	perMetricWriteTimes *tricorder.CumulativeDistribution
	visitorMetricsStore *visitorMetricsStoreType
	maybeNilCoord       store.Coordinator
}

type coordinatorBuilderType interface {
	WithStateListener(listener func(blocked bool)) store.Coordinator
}

func newPStoreHandler(
	appList *datastructs.ApplicationList,
	consumer *pstore.ConsumerWithMetricsBuilder,
	maybeNilCoordBuilder coordinatorBuilderType) *pstoreHandlerType {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 1000.0)
	perMetricWriteTimes := bucketer.NewCumulativeDistribution()
	consumer.SetPerMetricWriteTimeDist(perMetricWriteTimes)
	visitorMetricsStore := &visitorMetricsStoreType{}
	var maybeNilCoord store.Coordinator
	if maybeNilCoordBuilder != nil {
		maybeNilCoord = maybeNilCoordBuilder.WithStateListener(
			visitorMetricsStore.SetBlocked)
	}
	return &pstoreHandlerType{
		consumer:            consumer.Build(),
		appList:             appList,
		totalTimeSpentDist:  bucketer.NewCumulativeDistribution(),
		perMetricWriteTimes: perMetricWriteTimes,
		visitorMetricsStore: visitorMetricsStore,
		maybeNilCoord:       maybeNilCoord,
	}
}

func (p *pstoreHandlerType) Name() string {
	return p.consumer.Name()
}

func (p *pstoreHandlerType) StartVisit() {
	p.startTime = time.Now()
}

func (p *pstoreHandlerType) EndVisit(theStore *store.Store) {
	p.consumer.Flush()
	p.visitorMetricsStore.SetTimeLeft(
		duration.FromFloat(theStore.TimeLeft(p.iteratorName())))
	totalTime := time.Now().Sub(p.startTime)
	p.totalTimeSpentDist.Add(totalTime)

}

func (p *pstoreHandlerType) Visit(
	theStore *store.Store, endpointId interface{}) error {

	hostName := endpointId.(*collector.Endpoint).HostName()
	port := endpointId.(*collector.Endpoint).Port()
	appName := p.appList.ByPort(port).Name()
	iterator, timeLeft := p.namedIterator(theStore, endpointId)
	if p.maybeNilCoord != nil {
		// aMetricStore from the consumer exposes the same filtering that
		// the consumer does internally
		aMetricStore := p.ConsumerMetricsStore()
		// Even though, the consumer filters out the records it can't write,
		// we have to do the same filtering here so that we get an accurate
		// count of skipped records with timestamps coming before the start
		// of our lease.
		iterator = store.NamedIteratorFilter(iterator, aMetricStore)
		// RemoveFromRecordCount removes skipped records from the total
		// count of records to write.
		iterator = store.NamedIteratorCoordinate(
			iterator,
			p.maybeNilCoord,
			kLeaseSpan,
			aMetricStore.RemoveFromRecordCount)
	}
	p.consumer.Write(iterator, hostName, appName)
	p.visitorMetricsStore.MaybeIncreaseTimeLeft(
		duration.FromFloat(timeLeft))
	return nil
}

func (p *pstoreHandlerType) Attributes(attributes *pstore.ConsumerAttributes) {
	p.consumer.Attributes(attributes)
}

func (p *pstoreHandlerType) TotalCount() *totalCountType {
	var attributes pstore.ConsumerAttributes
	p.Attributes(&attributes)
	return &totalCountType{
		metrics:    p.ConsumerMetricsStore(),
		name:       p.Name(),
		rollUpSpan: attributes.RollUpSpan,
	}
}

func (p *pstoreHandlerType) ConsumerMetricsStore() *pstore.ConsumerMetricsStore {
	return p.consumer.MetricsStore()
}

func (p *pstoreHandlerType) RegisterMetrics() (err error) {
	var attributes pstore.ConsumerAttributes
	p.consumer.Attributes(&attributes)
	var data pstore.ConsumerMetrics
	var visitorData visitorMetricsType
	metricsStore := p.ConsumerMetricsStore()
	visitorMetricsStore := p.visitorMetricsStore
	group := tricorder.NewGroup()
	group.RegisterUpdateFunc(
		func() time.Time {
			metricsStore.Metrics(&data)
			visitorMetricsStore.Metrics(&visitorData)
			return time.Now()
		})
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/totalTimeSpent", p.Name()),
		p.totalTimeSpentDist,
		units.Second,
		"total time spent per sweep"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/writeTimePerMetric", p.Name()),
		p.perMetricWriteTimes,
		units.Millisecond,
		"Time spent writing each metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/maxRecordsPerSecond", p.Name()),
		attributes.TotalRecordsPerSecond,
		units.None,
		"Max records per second to write. 0 means unlimited"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/concurrency", p.Name()),
		&attributes.Concurrency,
		units.None,
		"Number of writing goroutines"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/batchSize", p.Name()),
		&attributes.BatchSize,
		units.None,
		"This many records written each time"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/rollUpSpan", p.Name()),
		&attributes.RollUpSpan,
		units.None,
		"Time period length for rolled up values. 0 means no roll up."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/valuesWritten", p.Name()),
		&data.ValuesWritten,
		group,
		units.None,
		"Number of values written to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/valuesNotWritten", p.Name()),
		&data.ValuesNotWritten,
		group,
		units.None,
		"Number of values not written to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/timeLeft", p.Name()),
		&visitorData.TimeLeft,
		group,
		units.None,
		"approximate time writer is behind"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/blocked", p.Name()),
		&visitorData.Blocked,
		group,
		units.None,
		"true if writer is awaiting a lease"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/writeAttempts", p.Name()),
		&data.WriteAttempts,
		group,
		units.None,
		"Number of attempts to write to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/successfulWrites", p.Name()),
		&data.SuccessfulWrites,
		group,
		units.None,
		"Number of successful writes to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/successfulWriteRatio", p.Name()),
		data.SuccessfulWriteRatio,
		group,
		units.None,
		"Ratio of successful writes to write attempts"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/lastWriteError", p.Name()),
		&data.LastWriteError,
		group,
		units.None,
		"Last write error"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/lastWriteErrorTime", p.Name()),
		&data.LastWriteErrorTS,
		group,
		units.None,
		"Time of last write error"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/lastSuccessfulWrite", p.Name()),
		&data.LastSuccessfulWriteTS,
		group,
		units.None,
		"Time of last successful write"); err != nil {
		return
	}
	return
}

func (p *pstoreHandlerType) iteratorName() string {
	return fmt.Sprintf("%s/%s", kPStoreIteratorName, p.Name())
}

func (p *pstoreHandlerType) namedIterator(
	theStore *store.Store,
	endpointId interface{}) (store.NamedIterator, float64) {
	var attributes pstore.ConsumerAttributes
	p.Attributes(&attributes)
	return createNamedIterator(
		theStore,
		endpointId,
		p.iteratorName(),
		attributes.RollUpSpan)
}

func createNamedIterator(
	theStore *store.Store,
	endpointId interface{},
	iteratorName string,
	rollUpSpan time.Duration) (store.NamedIterator, float64) {
	if rollUpSpan == 0 {
		return theStore.NamedIteratorForEndpoint(
			iteratorName,
			endpointId,
			kLookAheadWritingToPStore,
		)
	}
	// TODO: Strategy hard coded for now, but really the pstore writer
	// in use should dictate the grouping strategy. For now, all our
	// pstore writers convert numeric metrics to float64 which is why
	// the store.GroupByPathAndNumeric strategy works for now.
	return theStore.NamedIteratorForEndpointRollUp(
		iteratorName,
		endpointId,
		rollUpSpan,
		kLookAheadWritingToPStore,
		store.GroupMetricByPathAndNumeric,
	)
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store               *store.Store
	AppList             *datastructs.ApplicationList
	AppStats            *datastructs.ApplicationStatuses
	ConnectionErrors    *connectionErrorsType
	CollectionTimesDist *tricorder.CumulativeDistribution
	ByProtocolDist      map[string]*tricorder.CumulativeDistribution
	ChangedMetricsDist  *tricorder.CumulativeDistribution
	NamesSentToSuggest  nameSetType
	MetricNameAdder     suggest.Adder
	TotalCounts         []*totalCountType
}

func (l *loggerType) LogStateChange(
	e *collector.Endpoint, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		timeTaken := newS.TimeSpentConnecting()
		timeTaken += newS.TimeSpentPolling()
		timeTaken += newS.TimeSpentWaitingToConnect()
		timeTaken += newS.TimeSpentWaitingToPoll()
		l.CollectionTimesDist.Add(timeTaken)
		dist := l.ByProtocolDist[e.ConnectorName()]
		if dist != nil {
			dist.Add(timeTaken)
		}
	}
	l.AppStats.Update(e, newS)
}

func (l *loggerType) LogError(e *collector.Endpoint, err error, state *collector.State) {
	if err == nil {
		l.ConnectionErrors.Clear(e)
	} else {
		l.ConnectionErrors.Set(e, err, state.Timestamp())
	}
	l.AppStats.ReportError(e, err, state.Timestamp())
}

func (l *loggerType) LogResponse(
	e *collector.Endpoint,
	list metrics.List,
	timestamp time.Time) error {
	ts := duration.TimeToFloat(timestamp)
	added, err := l.Store.AddBatch(
		e,
		ts,
		list)
	if err == nil {
		l.reportNewNamesForSuggest(list)
		l.AppStats.LogChangedMetricCount(e, added)
		l.ChangedMetricsDist.Add(float64(added))
		if l.TotalCounts != nil {
			for i := range l.TotalCounts {
				l.TotalCounts[i].Update(l.Store, e)
			}
		}
	}
	// This error just means that the endpoint was marked inactive
	// during polling.
	if err == store.ErrInactive {
		return nil
	}
	return err
}

func (l *loggerType) reportNewNamesForSuggest(
	list metrics.List) {
	length := list.Len()
	for i := 0; i < length; i++ {
		var value metrics.Value
		list.Index(i, &value)
		if types.FromGoValue(value.Value).CanToFromFloat() {
			if !l.NamesSentToSuggest[value.Path] {
				l.MetricNameAdder.Add(value.Path)
				l.NamesSentToSuggest[value.Path] = true
			}
		}
	}
}

type gzipResponseWriter struct {
	http.ResponseWriter
	W io.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.W.Write(b)
}

type gzipHandler struct {
	H http.Handler
}

func (h gzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		h.H.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	defer gz.Close()
	gzr := &gzipResponseWriter{ResponseWriter: w, W: gz}
	h.H.ServeHTTP(gzr, r)
}

type latestMetricsAppenderJSON struct {
	result   *[]*messages.LatestMetric
	hostName string
	appName  string
}

func (a *latestMetricsAppenderJSON) Append(r *store.Record) bool {
	// Skip inactive records
	if !r.Active {
		return true
	}
	jsonValue, jsonKind, jsonSubType := asJsonWithSubType(
		r.Value,
		r.Info.Kind(),
		r.Info.SubType(),
		r.Info.Unit())
	var upperLimits []float64
	if r.Info.Kind() == types.Dist {
		upperLimits = r.Info.Ranges().UpperLimits
	}
	aMetric := &messages.LatestMetric{
		HostName:        a.hostName,
		AppName:         a.appName,
		Path:            r.Info.Path(),
		Kind:            jsonKind,
		SubType:         jsonSubType,
		Description:     r.Info.Description(),
		Bits:            r.Info.Bits(),
		Unit:            r.Info.Unit(),
		Value:           jsonValue,
		Timestamp:       duration.SinceEpochFloat(r.TimeStamp).String(),
		IsNotCumulative: r.Info.IsNotCumulative(),
		UpperLimits:     upperLimits,
	}
	*a.result = append(*a.result, aMetric)
	return true
}

type latestMetricsAppender struct {
	result   *[]*messages.LatestMetric
	hostName string
	appName  string
}

func (a *latestMetricsAppender) Append(r *store.Record) bool {
	// Skip inactive records
	if !r.Active {
		return true
	}
	var upperLimits []float64
	if r.Info.Kind() == types.Dist {
		upperLimits = r.Info.Ranges().UpperLimits
	}
	aMetric := &messages.LatestMetric{
		HostName:        a.hostName,
		AppName:         a.appName,
		Path:            r.Info.Path(),
		Kind:            r.Info.Kind(),
		SubType:         r.Info.SubType(),
		Description:     r.Info.Description(),
		Bits:            r.Info.Bits(),
		Unit:            r.Info.Unit(),
		Value:           fixForGoRPC(r.Value, r.Info.Kind()),
		Timestamp:       duration.FloatToTime(r.TimeStamp),
		IsNotCumulative: r.Info.IsNotCumulative(),
		UpperLimits:     upperLimits,
	}
	*a.result = append(*a.result, aMetric)
	return true
}

// endpointMetricsAppender is an implementation of store.Appender that
// appends to a messages.EndpointMetricsList.
// endpointMetricsAppender is NOT threadsafe.
type endpointMetricsAppender struct {
	endpointMetrics *messages.EndpointMetricList
	lastInfo        *store.MetricInfo
	lastMetric      *messages.EndpointMetric
}

// newEndpointMetricsAppender creates a endpointMetricsAppender that appends
// to result.
func newEndpointMetricsAppender(result *messages.EndpointMetricList) *endpointMetricsAppender {
	return &endpointMetricsAppender{endpointMetrics: result}
}

func asDistribution(distTotals *store.DistributionTotals) (
	result *messages.Distribution) {
	if distTotals != nil {
		result = &messages.Distribution{
			Sum:           distTotals.Sum,
			Count:         distTotals.Count(),
			Counts:        distTotals.Counts,
			RollOverCount: distTotals.RollOverCount,
		}
	}
	return
}

func fixForGoRPC(value interface{}, kind types.Type) interface{} {
	if kind == types.Dist {
		return asDistribution(value.(*store.DistributionTotals))
	}
	return value
}

func asJsonWithSubType(
	value interface{}, kind, subType types.Type, unit units.Unit) (
	jsonValue interface{}, jsonKind, jsonSubType types.Type) {
	if kind == types.Dist {
		jsonValue = asDistribution(value.(*store.DistributionTotals))
		jsonKind = kind
		jsonSubType = subType
		return
	}
	return trimessages.AsJsonWithSubType(value, kind, subType, unit)
}

func (a *endpointMetricsAppender) Append(r *store.Record) bool {
	if !store.GroupMetricByKey.Equal(r.Info, a.lastInfo) {
		jsonValue, jsonKind, jsonSubType := asJsonWithSubType(
			r.Value,
			r.Info.Kind(),
			r.Info.SubType(),
			r.Info.Unit())
		var upperLimits []float64
		if r.Info.Kind() == types.Dist {
			upperLimits = r.Info.Ranges().UpperLimits
		}
		a.lastMetric = &messages.EndpointMetric{
			Path:        r.Info.Path(),
			Kind:        jsonKind,
			SubType:     jsonSubType,
			Description: r.Info.Description(),
			Bits:        r.Info.Bits(),
			Unit:        r.Info.Unit(),
			Values: []*messages.TimestampedValue{{
				Timestamp: duration.SinceEpochFloat(
					r.TimeStamp).String(),
				Value:  jsonValue,
				Active: r.Active,
			}},
			IsNotCumulative: r.Info.IsNotCumulative(),
			UpperLimits:     upperLimits,
		}
		*a.endpointMetrics = append(*a.endpointMetrics, a.lastMetric)
	} else {
		jsonValue, _, _ := asJsonWithSubType(
			r.Value,
			r.Info.Kind(),
			r.Info.SubType(),
			r.Info.Unit())
		newTimestampedValue := &messages.TimestampedValue{
			Timestamp: duration.SinceEpochFloat(
				r.TimeStamp).String(),
			Value:  jsonValue,
			Active: r.Active,
		}
		a.lastMetric.Values = append(
			a.lastMetric.Values, newTimestampedValue)
	}
	a.lastInfo = r.Info
	return true
}

func latestMetricsForEndpoint(
	metricStore *store.Store,
	app *datastructs.ApplicationStatus,
	canonicalPath string,
	json bool) (result []*messages.LatestMetric) {
	var appender store.Appender
	if json {
		appender = &latestMetricsAppenderJSON{
			result:   &result,
			hostName: app.EndpointId.HostName(),
			appName:  app.Name,
		}
	} else {
		appender = &latestMetricsAppender{
			result:   &result,
			hostName: app.EndpointId.HostName(),
			appName:  app.Name,
		}
	}
	metricStore.LatestByPrefixAndEndpointStrategy(
		canonicalPath,
		app.EndpointId,
		store.GroupMetricByPathAndNumeric,
		store.AppenderFilterFunc(
			appender,
			func(r *store.Record) bool {
				return r.Info.Path() == canonicalPath || strings.HasPrefix(
					r.Info.Path(), canonicalPath+"/")
			},
		),
	)
	sort.Sort(latestByPath(result))
	return
}

// gatherDataForEndpoint serves api/hosts pages.
// metricStore is the metric store.
// endpoint is the endpoint from which we are getting historical metrics.
// canonicalPath is the path of the metrics or the empty string for all
// metrics. canonicalPath is returned from canonicalisePath().
// history is the amount of time to go back in minutes.
// If isSingleton is true, fetched metrics have to match canonicalPath
// exactly.
// Otherwise fetched metrics have to be found underneath canonicalPath.
// On no match, gatherDataForEndpoint returns an empty
// messages.EndpointMetricsList instance
func gatherDataForEndpoint(
	metricStore *store.Store,
	endpoint *collector.Endpoint,
	canonicalPath string,
	history int,
	isSingleton bool) (result messages.EndpointMetricList) {
	result = make(messages.EndpointMetricList, 0)
	now := duration.TimeToFloat(time.Now())
	appender := newEndpointMetricsAppender(&result)
	if canonicalPath == "" {
		metricStore.ByEndpointStrategy(
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			store.GroupMetricByKey,
			appender)
	} else {
		metricStore.ByNameAndEndpointStrategy(
			canonicalPath,
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			store.GroupMetricByKey,
			appender)
		if len(result) == 0 && !isSingleton {
			metricStore.ByPrefixAndEndpointStrategy(
				canonicalPath+"/",
				endpoint,
				now-60.0*float64(history),
				math.Inf(1),
				store.GroupMetricByKey,
				appender)
		}

	}
	sortMetricsByPath(result)
	return
}

// byPath sorts metrics by path lexographically
type latestByPath []*messages.LatestMetric

func (b latestByPath) Len() int {
	return len(b)
}

func (b latestByPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b latestByPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

// byPath sorts metrics by path lexographically
type byPath messages.EndpointMetricList

func (b byPath) Len() int {
	return len(b)
}

func (b byPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b byPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortMetricsByPath(result messages.EndpointMetricList) {
	sort.Sort(byPath(result))
}

func encodeJson(w io.Writer, data interface{}, pretty bool) {
	if pretty {
		content, _ := json.Marshal(data)
		var buffer bytes.Buffer
		json.Indent(&buffer, content, "", "\t")
		buffer.WriteTo(w)
	} else {
		encoder := json.NewEncoder(w)
		encoder.Encode(data)
	}
}

// errorHandler provides the api/errors requests.
type errorHandler struct {
	ConnectionErrors *connectionErrorsType
}

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	encodeJson(w, h.ConnectionErrors.GetErrors(), r.Form.Get("format") == "text")
}

// canonicalisePath removes any trailing slashes from path and ensures it has
// exactly one leading slash. The one exception to this is that if
// path is empty, canonicalisePath returns the empty string.
func canonicalisePath(path string) string {
	if path == "" {
		return ""
	}
	return "/" + strings.Trim(path, "/")
}

func httpError(w http.ResponseWriter, status int) {
	http.Error(
		w,
		fmt.Sprintf(
			"%d %s",
			status,
			http.StatusText(status)),
		status)
}

// byEndpointHandler handles serving api/hosts requests
type latestHandler struct {
	AS *datastructs.ApplicationStatuses
}

func (h latestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	path := canonicalisePath(r.URL.Path)
	apps, metricStore := h.AS.AllActiveWithStore()
	datastructs.ByHostAndName(apps)
	data := make([]*messages.LatestMetric, 0)
	for _, app := range apps {
		data = append(
			data,
			latestMetricsForEndpoint(metricStore, app, path, true)...)
	}
	encodeJson(w, data, r.Form.Get("format") == "text")
}

// byEndpointHandler handles serving api/hosts requests
type byEndpointHandler struct {
	AS *datastructs.ApplicationStatuses
}

func (h byEndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	hostNameAndPath := strings.SplitN(r.URL.Path, "/", 3)
	var host string
	var name string
	var path string
	if len(hostNameAndPath) == 1 {
		httpError(w, 404)
		return
	} else if len(hostNameAndPath) == 2 {
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], ""
	} else {
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], canonicalisePath(hostNameAndPath[2])
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	endpoint, metricStore := h.AS.EndpointIdByHostAndName(host, name)
	if endpoint == nil {
		httpError(w, 404)
		return
	}
	data := gatherDataForEndpoint(
		metricStore, endpoint, path, history, isSingleton)
	encodeJson(w, data, r.Form.Get("format") == "text")
}

func newPStoreConsumers(maybeNilMemoryManager *memoryManagerType) (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	switch *fPersistentStoreType {
	case "kafka":
		result, err = kafka.ConsumerBuildersFromFile(
			path.Join(*fConfigDir, "kafka.yaml"))
	case "influx":
		result, err = influx.ConsumerBuildersFromFile(
			path.Join(*fConfigDir, "influx.yaml"))
	case "tsdb":
		result, err = tsdb.ConsumerBuildersFromFile(
			path.Join(*fConfigDir, "tsdb.yaml"))
	case "none":
		// Do nothing
	default:
		log.Fatal("persistentStoreType flag must be kafka,influx,tsdb, or none.")
	}
	if maybeNilMemoryManager != nil {
		memoryManager := maybeNilMemoryManager
		// Add hook to check memory after each write
		for i := range result {
			result[i].AddHook(
				writeHookerType{wrapped: memoryManager})
		}
	}
	return
}

func createApplicationList() *datastructs.ApplicationList {
	f, err := os.Open(path.Join(*fConfigDir, "apps.yaml"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	builder := datastructs.NewApplicationListBuilder()
	if err := builder.ReadConfig(f); err != nil {
		log.Fatal(err)
	}
	return builder.Build()
}

func computePageCount() uint {
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	if totalMemoryToUse > 0 {
		return uint(totalMemoryToUse / uint64(*fBytesPerPage))
	}
	return *fPageCount
}

func hostNames(machines []mdb.Machine) (result []string) {
	result = make([]string, len(machines))
	for i := range machines {
		result[i] = machines[i].Hostname
	}
	return
}

func totalMemoryUsed() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}

// Returns alloc memory needed to trigger gc along with gc cycle.
func gcCollectThresh() (uint64, uint32) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.NextGC, memStats.NumGC
}

func totalSystemMemory() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Sys
}

// store.Store implements this interface. This interface is here to decouple
// the store package from memory management code.
type memoryType interface {
	// Returns true if we allocate new memory rather than recycling it.
	IsExpanding() bool
	// Pass false to recycle memory; pass true to allocate new memory as
	// needed.
	SetExpanding(b bool)
	// Sends request to free up bytesToFree bytes on the spot.
	FreeUpBytes(bytesToFree uint64)
}

// Metrics for memory management
type memoryManagerMetricsType struct {
	// Total number of GC cycles
	TotalCycleCount uint64
	// Number of GC cycles that we inspected. Will be less than total cycle
	// count. Regrettably, there is no way to be called every time a new
	// GC cycle begins or ends. The best we can do is check the GC at
	// opportune times.
	InspectedCycleCount uint64
	// Number of forced, stop-the-world, garbage collections.
	STWGCCount uint64
	// Number of cycles where we needed to ensure expanding is turned off
	// as in memoryType.SetExpanding(false)
	NoExpandCount uint64
	// Total number of bytes freed with memoryType.FreeUpBytes
	PageFreeInBytes uint64
	// Number of times memoryType.FreeUpBytes was called
	PageFreeCount uint64
	// Number of alloced bytes on heap needed to trigger next GC collection
	AllocBytesNeededForGC uint64
	// Largest AllocBytesNeededForGC so far
	LargestAllocBytesNeededForGC uint64
}

// Manages scotty's memory
type memoryManagerType struct {
	hardLimit     uint64
	highWaterMark uint64
	lowWaterMark  uint64
	logger        *log.Logger
	gcPercent     int
	gcCh          chan bool
	memoryCh      chan memoryType
	lock          sync.Mutex
	stats         memoryManagerMetricsType
}

func allocateMemory(totalMemoryToUse uint64) (allocatedMemory [][]byte) {
	chunkSize := totalMemoryToUse/1000 + 1
	for totalSystemMemory() < (totalMemoryToUse-chunkSize+1)/100*99 {
		chunk := make([]byte, chunkSize)
		allocatedMemory = append(allocatedMemory, chunk)
	}
	return
}

// Returns the GC percent See https://golang.org/pkg/runtime/#pkg-overview.
// A negative value means GC is turned off.
func getGcPercent() (result int) {
	// Have to do it this way as there is no GetGCPercent
	result = debug.SetGCPercent(-1)
	debug.SetGCPercent(result)
	return
}

// Creates the memory manager if command line args request it. Otherwise
// returns nil.
func maybeCreateMemoryManager(logger *log.Logger) *memoryManagerType {
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	if totalMemoryToUse > 0 {
		gcPercent := getGcPercent()
		if gcPercent <= 0 {
			log.Fatal("To use dynamic memory management, GC must be enabled")
		}
		allocatedMemory := allocateMemory(totalMemoryToUse)
		// Adjust totalMemoryToUse with actual memory used at
		// this point which is slightly smaller because of
		// system overhead.
		totalMemoryToUse = totalMemoryUsed()
		// Prevent compiler complaining about unused allocated memory
		logger.Printf(
			"totalMemoryInUse: %d\n",
			totalMemoryToUse+uint64(len(allocatedMemory)*0))
		allocatedMemory = nil
		now := time.Now()
		runtime.GC()
		logger.Printf("GCTime: %v; totalMemoryInUse: %d\n", time.Since(now), totalMemoryUsed())
		result := &memoryManagerType{
			hardLimit:     totalMemoryToUse,
			highWaterMark: uint64(float64(totalMemoryToUse) * 0.90),
			lowWaterMark:  uint64(float64(totalMemoryToUse) * 0.80),
			logger:        logger,
			gcPercent:     gcPercent,
			gcCh:          make(chan bool),
			memoryCh:      make(chan memoryType, 10),
		}
		go result.loop()
		return result
	}
	return nil
}

// Check tells the memory manager to inspect the current GC cycle and take
// necessary actions.
func (m *memoryManagerType) Check() {
	select {
	case m.gcCh <- true:
	default:
	}
}

// SetMemory tells the memory manager what is implementing memoryType.
func (m *memoryManagerType) SetMemory(memory memoryType) {
	m.memoryCh <- memory
}

// Metrics returns the memory manager metrics at stats
func (m *memoryManagerType) Metrics(stats *memoryManagerMetricsType) {
	m.lock.Lock()
	defer m.lock.Unlock()
	*stats = m.stats
}

// RegisterMetrics registers the memory manager metrics
func (m *memoryManagerType) RegisterMetrics() (err error) {
	var data memoryManagerMetricsType
	group := tricorder.NewGroup()
	group.RegisterUpdateFunc(
		func() time.Time {
			m.Metrics(&data)
			return time.Now()
		})
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/alloc-bytes-needed-for-gc",
		&data.AllocBytesNeededForGC,
		group,
		units.Byte,
		"Number of allocated bytes needed to trigger GC"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/largest-alloc-bytes-needed-for-gc",
		&data.LargestAllocBytesNeededForGC,
		group,
		units.Byte,
		"Number of allocated bytes needed to trigger GC"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/inspected-cycle-count",
		&data.InspectedCycleCount,
		group,
		units.None,
		"Number of gc cycles inspected"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/total-cycle-count",
		&data.TotalCycleCount,
		group,
		units.None,
		"Number of total gc cycles"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/stw-gc-count",
		&data.STWGCCount,
		group,
		units.None,
		"Number of stop the world GCs"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/no-expand-count",
		&data.NoExpandCount,
		group,
		units.None,
		"Inspected cycle counts where we disabled expanding"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/page-free-bytes",
		&data.PageFreeInBytes,
		group,
		units.Byte,
		"Total number of pages freed in bytes"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/proc/memory-manager/page-free-count",
		&data.PageFreeCount,
		group,
		units.None,
		"Number of times pages freed"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/hard-limit",
		&m.hardLimit,
		units.Byte,
		"Target memory usage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/high-water-mark",
		&m.highWaterMark,
		units.Byte,
		"Memory usage that triggers a GC"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/low-water-mark",
		&m.lowWaterMark,
		units.Byte,
		"Desired memory usage after GC happens"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/gc-percent",
		&m.gcPercent,
		units.None,
		"GC percentage"); err != nil {
		return
	}
	return
}

func (m *memoryManagerType) setMetrics(stats *memoryManagerMetricsType) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.stats = *stats
}

// This function is the workhorse of memory management.
// Changing memory management strategies means changing this function
func (m *memoryManagerType) loop() {
	var lastCycleCount uint32
	// The memory object
	var memory memoryType
	// Are we allocating new memory (true) or are we trying to recycle (false)?
	var isExpanding bool
	// Our statistics for metrics.
	var stats memoryManagerMetricsType
	for {
		// Block until we are asked to check GC cycle.
		select {
		// Some goroutine called Check()
		case <-m.gcCh:
			// Some goroutine gave us the memory to monitor, in particular
			// the store.Store instance which uses most of scotty's
			// memory.
		case memory = <-m.memoryCh:
			isExpanding = memory.IsExpanding()
		}
		// Find out the max memory usage needed to trigger the next GC.
		// If we had already processed this cycle, just skip it.
		maxMemory, cycleCount := gcCollectThresh()
		if cycleCount > lastCycleCount {
			stats.AllocBytesNeededForGC = maxMemory
			if stats.AllocBytesNeededForGC > stats.LargestAllocBytesNeededForGC {
				stats.LargestAllocBytesNeededForGC = stats.AllocBytesNeededForGC
			}
			stats.TotalCycleCount = uint64(cycleCount)
			stats.InspectedCycleCount++
			// If the allocated memory needed for a GC exceeds the low
			// watermark, turn off expanding once and for all so that we
			// recycle pages in scotty instead of allocating new ones. Once
			// we turn off expanding, we expect total memory usage to remain
			// constant.
			if maxMemory >= m.lowWaterMark {
				stats.NoExpandCount++
				if memory != nil && isExpanding {
					memory.SetExpanding(false)
					isExpanding = false
				}
			}
			m.setMetrics(&stats)
			lastCycleCount = cycleCount
		}
	}
}

type writeHookerType struct {
	wrapped *memoryManagerType
}

func (w writeHookerType) WriteHook(
	unusedRecords []pstore.Record, unusedError error) {
	w.wrapped.Check()
}

func createApplicationStats(
	appList *datastructs.ApplicationList,
	logger *log.Logger,
	tagvAdder suggest.Adder,
	maybeNilMemoryManager *memoryManagerType) *datastructs.ApplicationStatuses {
	var astore *store.Store
	fmt.Println("Initialization started.")
	if maybeNilMemoryManager != nil {
		memoryManager := maybeNilMemoryManager
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage,
			1,
			*fThreshhold,
			*fDegree)
		astore.SetExpanding(true)
		memoryManager.SetMemory(astore)
		if err := memoryManager.RegisterMetrics(); err != nil {
			log.Fatal(err)
		}
	} else {
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage, *fPageCount, *fThreshhold, *fDegree)
	}
	dirSpec, err := tricorder.RegisterDirectory("/store")
	if err != nil {
		log.Fatal(err)
	}
	if err := astore.RegisterMetrics(dirSpec); err != nil {
		log.Fatal(err)
	}
	stats := datastructs.NewApplicationStatuses(appList, astore)
	mdbChannel := mdbd.StartMdbDaemon(*fMdbFile, logger)
	machines := <-mdbChannel
	theHostNames := hostNames(machines.Machines)
	for _, aName := range theHostNames {
		tagvAdder.Add(aName)
	}
	stats.MarkHostsActiveExclusively(
		duration.TimeToFloat(time.Now()), theHostNames)
	fmt.Println("Initialization complete.")
	// Endpoint refresher goroutine
	go func() {
		for {
			machines := <-mdbChannel
			stats.MarkHostsActiveExclusively(
				duration.TimeToFloat(time.Now()),
				hostNames(machines.Machines))
		}
	}()
	return stats
}

func startCollector(
	appStats *datastructs.ApplicationStatuses,
	connectionErrors *connectionErrorsType,
	totalCounts []*totalCountType,
	metricNameAdder suggest.Adder,
	maybeNilMemoryManager *memoryManagerType) {
	collector.SetConcurrentPolls(*fPollCount)
	collector.SetConcurrentConnects(*fConnectionCount)

	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	collectionBucketer := tricorder.NewGeometricBucketer(1e-4, 100.0)
	collectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	tricorderCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	snmpCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	jsonCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	changedMetricsPerEndpointDist := tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()

	if err := tricorder.RegisterMetric(
		"collector/collectionTimes",
		collectionTimesDist,
		units.Second,
		"Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_tricorder",
		tricorderCollectionTimesDist,
		units.Second,
		"Tricorder Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_snmp",
		snmpCollectionTimesDist,
		units.Second,
		"SNMP Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_json",
		jsonCollectionTimesDist,
		units.Second,
		"JSON Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/changedMetricsPerEndpoint",
		changedMetricsPerEndpointDist,
		units.None,
		"Changed metrics per sweep"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration"); err != nil {
		log.Fatal(err)
	}
	programStartTime := time.Now()
	if err := tricorder.RegisterMetric(
		"collector/elapsedTime",
		func() time.Duration {
			return time.Now().Sub(programStartTime)
		},
		units.Second,
		"elapsed time"); err != nil {
		log.Fatal(err)
	}

	byProtocolDist := map[string]*tricorder.CumulativeDistribution{
		"tricorder": tricorderCollectionTimesDist,
		"snmp":      snmpCollectionTimesDist,
		"json":      jsonCollectionTimesDist,
	}

	// Metric collection goroutine. Collect metrics periodically.
	go func() {
		// We assign each endpoint its very own nameSetType instance
		// to store metric names already sent to suggest.
		// Only that endpoint's fetch goroutine reads and modifies
		// the contents of its nameSetType instance. Although
		// this goroutine creates nameSetType instances and manages
		// the references to them, it never reads or modifies the
		// contents of any nameSetType instance after creating it.
		endpointToNamesSentToSuggest := make(
			map[*collector.Endpoint]nameSetType)
		for {
			endpoints, metricStore := appStats.ActiveEndpointIds()
			sweepTime := time.Now()
			for _, endpoint := range endpoints {
				namesSentToSuggest := endpointToNamesSentToSuggest[endpoint]
				if namesSentToSuggest == nil {
					namesSentToSuggest = make(nameSetType)
					endpointToNamesSentToSuggest[endpoint] = namesSentToSuggest
				}
				logger := &loggerType{
					Store:               metricStore,
					AppStats:            appStats,
					ConnectionErrors:    connectionErrors,
					CollectionTimesDist: collectionTimesDist,
					ByProtocolDist:      byProtocolDist,
					ChangedMetricsDist:  changedMetricsPerEndpointDist,
					NamesSentToSuggest:  namesSentToSuggest,
					MetricNameAdder:     metricNameAdder,
					TotalCounts:         totalCounts,
				}

				endpoint.Poll(sweepTime, logger)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			if maybeNilMemoryManager != nil {
				memoryManager := maybeNilMemoryManager
				memoryManager.Check()
			}
			if sweepDuration < *fCollectionFrequency {
				time.Sleep((*fCollectionFrequency) - sweepDuration)
			}
		}
	}()
}

func startPStoreLoops(
	stats *datastructs.ApplicationStatuses,
	consumerBuilders []*pstore.ConsumerWithMetricsBuilder,
	logger *log.Logger,
	maybeNilCoordBuilder coordinatorBuilderType) []*totalCountType {
	result := make([]*totalCountType, len(consumerBuilders))
	for i := range result {
		pstoreHandler := newPStoreHandler(
			stats.ApplicationList(),
			consumerBuilders[i],
			maybeNilCoordBuilder)
		result[i] = pstoreHandler.TotalCount()
		var attributes pstore.ConsumerAttributes
		pstoreHandler.Attributes(&attributes)
		refreshRate := *fPStoreUpdateFrequency
		if attributes.RollUpSpan > 0 {
			refreshRate = attributes.RollUpSpan
		}
		if err := pstoreHandler.RegisterMetrics(); err != nil {
			log.Fatal(err)
		}
		go func(handler *pstoreHandlerType, refreshRate time.Duration) {
			// persistent storage writing goroutine. Write every 30s by default.
			// Notice that this single goroutine handles all the persistent
			// storage writing as multiple goroutines must not access the
			// pstoreHandler instance. accessing pstoreHandler metrics is the
			// one exception to this rule.
			for {
				metricStore := stats.Store()
				writeTime := time.Now()
				handler.StartVisit()
				metricStore.VisitAllEndpoints(handler)
				handler.EndVisit(metricStore)
				writeDuration := time.Now().Sub(writeTime)
				if writeDuration < refreshRate {
					time.Sleep(refreshRate - writeDuration)
				}
			}
		}(pstoreHandler, refreshRate)
	}
	return result
}

func gracefulCleanup() {
	os.Remove(*fPidFile)
	os.Exit(1)
}

func writePidfile() {
	file, err := os.Create(*fPidFile)
	if err != nil {
		return
	}
	defer file.Close()
	fmt.Fprintln(file, os.Getpid())
}

func handleSignals(logger *log.Logger) {
	if *fPidFile == "" {
		return
	}
	sigtermChannel := make(chan os.Signal)
	signal.Notify(sigtermChannel, syscall.SIGTERM, syscall.SIGINT)
	writePidfile()
	go func() {
		for {
			select {
			case <-sigtermChannel:
				gracefulCleanup()
			}
		}
	}()
}

type tsdbAdderType struct {
	wrapped suggest.Adder
}

func (a *tsdbAdderType) Add(s string) {
	a.wrapped.Add(tsdbjson.Escape(s))
}

func newTsdbAdder(adder suggest.Adder) suggest.Adder {
	return &tsdbAdderType{wrapped: adder}
}

type rpcType struct {
	AS *datastructs.ApplicationStatuses
}

func (t *rpcType) Latest(
	path string, response *[]*messages.LatestMetric) error {
	apps, metricStore := t.AS.AllActiveWithStore()
	datastructs.ByHostAndName(apps)
	path = canonicalisePath(path)
	for _, app := range apps {
		*response = append(
			*response,
			latestMetricsForEndpoint(metricStore, app, path, false)...)
	}
	return nil
}

type blockingCoordinatorType struct {
	listener func(blocked bool)
}

func (b *blockingCoordinatorType) Lease(float64, float64) (
	float64, float64) {
	if b.listener != nil {
		b.listener(true)
	}
	select {}
}

func (b *blockingCoordinatorType) WithStateListener(
	listener func(blocked bool)) store.Coordinator {
	result := *b
	result.listener = listener
	return &result
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	circularBuffer := logbuf.New()
	logger := log.New(circularBuffer, "", log.LstdFlags)
	handleSignals(logger)
	// Read configs early so that we will fail fast.
	maybeNilMemoryManager := maybeCreateMemoryManager(logger)
	consumerBuilders, err := newPStoreConsumers(maybeNilMemoryManager)
	if err != nil {
		log.Println("Pstore config file error:", err)
		logger.Println("Pstore config file error:", err)
	}
	metricNameEngine := suggest.NewEngine()
	metricNameAdder := newTsdbAdder(metricNameEngine)
	tagkEngine := suggest.NewSuggester("appname", "HostName")
	tagvEngine := suggest.NewEngine()
	tagvAdder := newTsdbAdder(tagvEngine)

	appList := createApplicationList()
	for _, app := range appList.All() {
		tagvAdder.Add(app.Name())
	}
	applicationStats := createApplicationStats(
		appList, logger, tagvAdder, maybeNilMemoryManager)
	rpc.RegisterName(
		"Scotty",
		&rpcType{AS: applicationStats},
	)
	rpc.HandleHTTP()
	connectionErrors := newConnectionErrorsType()
	if consumerBuilders == nil {
		startCollector(
			applicationStats,
			connectionErrors,
			nil,
			metricNameAdder,
			maybeNilMemoryManager)
	} else {
		var coord coordinatorBuilderType
		if *fCoord != "" {
			var err error
			coord, err = consul.NewCoordinator(logger)
			if err != nil {
				logger.Println(err)
				coord = &blockingCoordinatorType{}
			}
		}
		totalCounts := startPStoreLoops(
			applicationStats,
			consumerBuilders,
			logger,
			coord)
		startCollector(
			applicationStats,
			connectionErrors,
			totalCounts,
			metricNameAdder,
			maybeNilMemoryManager)
	}

	http.Handle(
		"/",
		gzipHandler{&splash.Handler{
			AS:  applicationStats,
			Log: circularBuffer,
		}})
	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			AS:             applicationStats,
			CollectionFreq: *fCollectionFrequency,
		}})
	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{&byEndpointHandler{
				AS: applicationStats,
			}}))
	http.Handle(
		"/api/latest/",
		http.StripPrefix(
			"/api/latest/",
			gzipHandler{&latestHandler{
				AS: applicationStats,
			}}))

	http.Handle(
		"/api/errors/",
		gzipHandler{&errorHandler{
			ConnectionErrors: connectionErrors,
		}},
	)

	tsdbServeMux := http.NewServeMux()

	tsdbServeMux.Handle(
		"/api/query",
		tsdbexec.NewHandler(
			func(r *tsdbjson.QueryRequest) ([]tsdbjson.TimeSeries, error) {
				return tsdbexec.Query(
					r, applicationStats, *fCollectionFrequency)
			}))
	tsdbServeMux.Handle(
		"/api/suggest",
		tsdbexec.NewHandler(
			func(req url.Values) ([]string, error) {
				return tsdbexec.Suggest(
					req,
					map[string]suggest.Suggester{
						"metrics": metricNameEngine,
						"tagk":    tagkEngine,
						"tagv":    tagvEngine,
					})
			},
		))
	tsdbServeMux.Handle(
		"/api/aggregators",
		tsdbexec.NewHandler(
			func(req url.Values) ([]string, error) {
				return aggregators.Names(), nil
			},
		))
	tsdbServeMux.Handle(
		"/api/version",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"version": "1.0",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api/config",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"tsd.ore.auto_create_metrics": "true",
					"tsd.ore.auto_create_tagks":   "true",
					"tsd.ore.auto_create_tagvs":   "true",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api/config/filters",
		tsdbexec.NewHandler(
			func(req url.Values) (interface{}, error) {
				return tsdbjson.AllFilterDescriptions(), nil
			},
		))
	tsdbServeMux.Handle(
		"/api/dropcaches",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"message": "Caches dropped",
					"status":  "200",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api",
		tsdbexec.NotFoundHandler,
	)

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *fTsdbPort), tsdbServeMux); err != nil {
			log.Fatal(err)
		}
	}()

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}
