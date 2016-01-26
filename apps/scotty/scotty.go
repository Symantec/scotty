package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/dom/mdbd"
	"github.com/Symantec/Dominator/lib/mdb"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/sysmemory"
	"github.com/Symantec/tricorder/go/tricorder"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	totalNumberOfEndpoints = 10000
)

var (
	fBytesPerPage          int
	fPageCount             int
	fHostFile              string
	fAppFile               string
	fMdbFile               string
	fPollCount             int
	fConnectionCount       int
	fCollectionFrequency   time.Duration
	fPStoreUpdateFrequency time.Duration
	fPStoreBatchSize       int
	fKafkaConfigFile       string
	fCluster               string
)

var (
	gHostsPortsAndStore            datastructs.HostsPortsAndStore
	gCollectionTimesDist           = tricorder.NewGeometricBucketer(1e-4, 100.0).NewCumulativeDistribution()
	gChangedMetricsPerEndpointDist = tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()
	gStatusCounts                  = newStatusCountType()
	gConnectionErrors              = newConnectionErrorsType()
	gApplicationStats              = datastructs.NewApplicationStatuses()
	gApplicationList               *datastructs.ApplicationList
)

type statusCountSnapshotType struct {
	WaitingToConnectCount int
	ConnectingCount       int
	WaitingToPollCount    int
	PollingCount          int
	SyncedCount           int
	FailedToConnectCount  int
	FailedToPollCount     int
}

type statusCountType struct {
	lock   sync.Mutex
	counts map[collector.Status]int
}

func newStatusCountType() *statusCountType {
	return &statusCountType{
		counts: make(map[collector.Status]int),
	}
}

func (s *statusCountType) Update(oldStatus, newStatus collector.Status) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if oldStatus != collector.Unknown {
		s.counts[oldStatus]--
	}
	if newStatus != collector.Unknown {
		s.counts[newStatus]++
	}
}

func (s *statusCountType) Snapshot(data *statusCountSnapshotType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	data.WaitingToConnectCount =
		s.counts[collector.WaitingToConnect]
	data.ConnectingCount = s.counts[collector.Connecting]
	data.WaitingToPollCount = s.counts[collector.WaitingToPoll]
	data.PollingCount = s.counts[collector.Polling]
	data.SyncedCount = s.counts[collector.Synced]
	data.FailedToConnectCount =
		s.counts[collector.FailedToConnect]
	data.FailedToPollCount = s.counts[collector.FailedToPoll]
}

func (s *statusCountType) RegisterMetrics() {
	var data statusCountSnapshotType
	region := tricorder.RegisterRegion(func() {
		s.Snapshot(&data)
	})
	tricorder.RegisterMetricInRegion(
		"collector/waitingToConnect",
		&data.WaitingToConnectCount,
		region,
		units.None,
		"Number of goroutines waiting to connect")
	tricorder.RegisterMetricInRegion(
		"collector/connecting",
		&data.ConnectingCount,
		region,
		units.None,
		"Number of goroutines connecting")
	tricorder.RegisterMetricInRegion(
		"collector/waitingToPoll",
		&data.WaitingToPollCount,
		region,
		units.None,
		"Number of goroutines waiting to poll")
	tricorder.RegisterMetricInRegion(
		"collector/polling",
		&data.PollingCount,
		region,
		units.None,
		"Number of goroutines polling")
	tricorder.RegisterMetricInRegion(
		"collector/synced",
		&data.SyncedCount,
		region,
		units.None,
		"Number of endpoints synced")
	tricorder.RegisterMetricInRegion(
		"collector/connectFailures",
		&data.FailedToConnectCount,
		region,
		units.None,
		"Number of connection failures")
	tricorder.RegisterMetricInRegion(
		"collector/pollFailures",
		&data.FailedToPollCount,
		region,
		units.None,
		"Number of poll failures")
}

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
		Timestamp: trimessages.SinceEpoch(timestamp).String(),
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

var (
	aWriteError = errors.New("A bad write error.")
)

func endpointNameAndPort(
	endpointNames []string,
	endpointPorts []int,
	i int) (name string, port int) {
	length := len(endpointNames)
	return fmt.Sprintf(
			"%s*%d",
			endpointNames[i%length],
			i),
		endpointPorts[i%length]
}

// metrics for pstoreHandlerType instances
type pstoreHandlerData struct {
	MetricsWritten   int64
	WriteAttempts    int64
	SuccessfulWrites int64
	LastWriteError   string
	EndpointsVisited int64
}

// pstoreHandlerType implements store.Visitor.
// Its Visit method writes the latest value for each metric in the store to
// persistent storage. It stores the last value for each endpoint and
// metric it wrote to persistent storage so
// that it can avoid writing the same value twice.
// an pstoreHandlerType instance buffers writes so that each write to
// persistent storage includes at least fPStoreBatchSize metrics.
// pstoreHandlerType is NOT threadsafe.
type pstoreHandlerType struct {
	writer                  pstore.Writer
	lastValues              map[*collector.Endpoint]map[*store.MetricInfo]interface{}
	lastValuesThisEndpoint  map[*store.MetricInfo]interface{}
	toBeWritten             []pstore.Record
	timeSpentWritingDist    *tricorder.CumulativeDistribution
	timeSpentCollectingDist *tricorder.CumulativeDistribution
	totalTimeSpentDist      *tricorder.CumulativeDistribution
	perBatchWriteDist       *tricorder.CumulativeDistribution
	ttWriter                *timeTakenWriter
	startTime               time.Time
	// Protects all fields below
	lock  sync.Mutex
	stats pstoreHandlerData
}

func newPStoreHandler(w pstore.Writer) *pstoreHandlerType {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 1000.0)
	return &pstoreHandlerType{
		writer:                  w,
		lastValues:              make(map[*collector.Endpoint]map[*store.MetricInfo]interface{}),
		timeSpentWritingDist:    bucketer.NewCumulativeDistribution(),
		timeSpentCollectingDist: bucketer.NewCumulativeDistribution(),
		totalTimeSpentDist:      bucketer.NewCumulativeDistribution(),
		perBatchWriteDist:       bucketer.NewCumulativeDistribution(),
	}
}

func appName(port int) string {
	return gApplicationList.ByPort(port).Name()
}

// Do not call this directly!
func (p *pstoreHandlerType) Append(r *store.Record) {
	kind := r.Info.Kind()
	if !p.ttWriter.IsTypeSupported(kind) {
		return
	}
	if p.lastValuesThisEndpoint[r.Info] == r.Value {
		return
	}
	p.lastValuesThisEndpoint[r.Info] = r.Value
	p.toBeWritten = append(
		p.toBeWritten, pstore.Record{
			HostName:  r.ApplicationId.HostName(),
			AppName:   appName(r.ApplicationId.Port()),
			Path:      r.Info.Path(),
			Kind:      r.Info.Kind(),
			Unit:      r.Info.Unit(),
			Value:     r.Value,
			Timestamp: r.TimeStamp})
}

func (p *pstoreHandlerType) StartVisit() {
	p.ttWriter = &timeTakenWriter{Writer: p.writer}
	p.startTime = time.Now()
}

func (p *pstoreHandlerType) EndVisit() {
	p.flush()
	totalTimeSpent := time.Now().Sub(p.startTime)
	p.timeSpentWritingDist.Add(p.ttWriter.TimeTaken)
	p.totalTimeSpentDist.Add(totalTimeSpent)
	p.timeSpentCollectingDist.Add(totalTimeSpent - p.ttWriter.TimeTaken)
}

func (p *pstoreHandlerType) Visit(
	theStore *store.Store, endpointId *collector.Endpoint) error {

	// Get the last known values for this endpoint creating the map
	// if necessary.
	p.lastValuesThisEndpoint = p.lastValues[endpointId]
	if p.lastValuesThisEndpoint == nil {
		p.lastValuesThisEndpoint = make(
			map[*store.MetricInfo]interface{})
		p.lastValues[endpointId] = p.lastValuesThisEndpoint
	}

	// Get the latest values to write, but get only the
	// values that changed.
	theStore.LatestByEndpoint(endpointId, p)

	// If we have enough values to write,
	// write them out to persistent storage.
	if len(p.toBeWritten) >= fPStoreBatchSize {
		p.flush()
	}
	p.logVisit()
	return nil
}

func (p *pstoreHandlerType) flush() {
	if len(p.toBeWritten) == 0 {
		return
	}
	startTime := time.Now()
	err := p.ttWriter.Write(p.toBeWritten)
	p.perBatchWriteDist.Add(time.Now().Sub(startTime))

	if err != nil {
		p.logWriteError(err)
	} else {
		p.logWrite(len(p.toBeWritten))
	}
	p.perBatchWriteDist.Add(time.Now().Sub(startTime))

	// Make toBeWritten be empty while saving on memory allocation
	p.toBeWritten = p.toBeWritten[:0]
}

func (p *pstoreHandlerType) RegisterMetrics() {
	tricorder.RegisterMetric(
		"writer/writeTimePerBatch",
		p.perBatchWriteDist,
		units.Millisecond,
		"Time spent writing each batch")
	tricorder.RegisterMetric(
		"writer/timeSpentCollecting",
		p.timeSpentCollectingDist,
		units.Second,
		"Time spent collecting metrics to write to persistent storage per sweep")
	tricorder.RegisterMetric(
		"writer/timeSpentWriting",
		p.timeSpentWritingDist,
		units.Second,
		"Time spent writing metrics to persistent storage per sweep")
	tricorder.RegisterMetric(
		"writer/totalTimeSpent",
		p.totalTimeSpentDist,
		units.Second,
		"total time spent per sweep")
	var data pstoreHandlerData
	region := tricorder.RegisterRegion(func() { p.collectData(&data) })
	tricorder.RegisterMetricInRegion(
		"writer/metricsWritten",
		&data.MetricsWritten,
		region,
		units.None,
		"Number of metrics written to persistent storage")
	tricorder.RegisterMetricInRegion(
		"writer/writeAttempts",
		&data.WriteAttempts,
		region,
		units.None,
		"Number of attempts to write to persistent storage")
	tricorder.RegisterMetricInRegion(
		"writer/successfulWrites",
		&data.SuccessfulWrites,
		region,
		units.None,
		"Number of successful writes to persistent storage")
	tricorder.RegisterMetricInRegion(
		"writer/successfulWriteRatio",
		func() float64 {
			return float64(data.SuccessfulWrites) / float64(data.WriteAttempts)
		},
		region,
		units.None,
		"Ratio of successful writes to write attempts")
	tricorder.RegisterMetricInRegion(
		"writer/lastWriteError",
		&data.LastWriteError,
		region,
		units.None,
		"Last write error")
	tricorder.RegisterMetricInRegion(
		"writer/endpointsVisited",
		&data.EndpointsVisited,
		region,
		units.None,
		"Number of endpoints visited")
	tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerEndpoint",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.EndpointsVisited)
		},
		region,
		units.None,
		"Average metrics written per endpoint per cycle.")
	tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerBatch",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.SuccessfulWrites)
		},
		region,
		units.None,
		"Average metrics written per batch.")
}

func (p *pstoreHandlerType) logWrite(numWritten int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.MetricsWritten += int64(numWritten)
	p.stats.WriteAttempts++
	p.stats.SuccessfulWrites++
}

func (p *pstoreHandlerType) logWriteError(err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.WriteAttempts++
	p.stats.LastWriteError = err.Error()
}

func (p *pstoreHandlerType) logVisit() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.EndpointsVisited++
}

// collectData collects metrics about this instance.
// Although pstoreHandlerType instances are not threadsafe, the collectData
// method is threadsafe.
func (p *pstoreHandlerType) collectData(data *pstoreHandlerData) {
	p.lock.Lock()
	defer p.lock.Unlock()
	*data = p.stats
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store *store.Store
}

func (l *loggerType) LogStateChange(
	e *collector.Endpoint, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		gCollectionTimesDist.Add(
			newS.TimeSpentConnecting() + newS.TimeSpentPolling() + newS.TimeSpentWaitingToConnect() + newS.TimeSpentWaitingToPoll())
	}
	if oldS != nil {
		gStatusCounts.Update(oldS.Status(), newS.Status())
	} else {
		gStatusCounts.Update(collector.Unknown, newS.Status())
	}
	gApplicationStats.Update(e, newS)
}

func (l *loggerType) LogError(e *collector.Endpoint, err error, state *collector.State) {
	if err == nil {
		gConnectionErrors.Clear(e)
	} else {
		gConnectionErrors.Set(e, err, state.Timestamp())
	}
}

func (l *loggerType) LogResponse(
	e *collector.Endpoint, metrics trimessages.MetricList, state *collector.State) {
	ts := trimessages.TimeToFloat(state.Timestamp())
	added := l.Store.AddBatch(
		e,
		ts,
		metrics,
		func(ametric *trimessages.Metric) bool {
			return ametric.Kind != types.Dist
		})
	gApplicationStats.LogChangedMetricCount(e, added)
	gChangedMetricsPerEndpointDist.Add(float64(added))
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

// endpointMetricsAppender is an implementation of store.Appender that
// appends to a messages.EndpointMetricsList.
// endpointMetricsAppender is NOT threadsafe.
type endpointMetricsAppender struct {
	endpointMetrics *messages.EndpointMetricsList
	lastInfo        *store.MetricInfo
	lastMetric      *messages.EndpointMetrics
}

// newEndpointMetricsAppender creates a endpointMetricsAppender that appends
// to result.
func newEndpointMetricsAppender(result *messages.EndpointMetricsList) *endpointMetricsAppender {
	return &endpointMetricsAppender{endpointMetrics: result}
}

func (a *endpointMetricsAppender) Append(r *store.Record) {
	if r.Info != a.lastInfo {
		a.lastInfo = r.Info
		_, jsonKind := trimessages.AsJson(nil, a.lastInfo.Kind(), a.lastInfo.Unit())
		a.lastMetric = &messages.EndpointMetrics{
			Path:        a.lastInfo.Path(),
			Kind:        jsonKind,
			Description: a.lastInfo.Description(),
			Bits:        a.lastInfo.Bits(),
			Unit:        a.lastInfo.Unit()}
		*a.endpointMetrics = append(*a.endpointMetrics, a.lastMetric)
	}
	jsonValue, _ := trimessages.AsJson(r.Value, a.lastInfo.Kind(), a.lastInfo.Unit())
	newTimestampedValue := &messages.TimestampedValue{
		Timestamp: trimessages.SinceEpochFloat(r.TimeStamp).String(),
		Value:     jsonValue,
	}
	a.lastMetric.Values = append(a.lastMetric.Values, newTimestampedValue)
}

// gatherDataForEndpoint serves api/hosts pages.
// metricStore is the metric store.
// endpoint is the endpoint from which we are getting historical metrics.
// path is the path of the metrics or the empty string for all metrics
// history is the amount of time to go back in minutes.
// If isSingleton is true, fetched metrics have to match path exactly.
// Otherwise fetched metrics have to be found underneath path.
// On no match, gatherDataForEndpoint returns an empty
// messages.EndpointMetricsList instance
func gatherDataForEndpoint(
	metricStore *store.Store,
	endpoint *collector.Endpoint,
	path string,
	history int,
	isSingleton bool) (result messages.EndpointMetricsList) {
	result = make(messages.EndpointMetricsList, 0)
	now := trimessages.TimeToFloat(time.Now())
	appender := newEndpointMetricsAppender(&result)
	if path == "" {
		metricStore.ByEndpoint(endpoint, now-60.0*float64(history), math.Inf(1), appender)
	} else {
		metricStore.ByNameAndEndpoint(
			path,
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			appender)
		if !isSingleton {
			metricStore.ByPrefixAndEndpoint(
				path+"/",
				endpoint,
				now-60.0*float64(history),
				math.Inf(1),
				appender)
		}

	}
	sortMetricsByPath(result)

	return
}

// byPath sorts metrics by path lexographically
type byPath messages.EndpointMetricsList

func (b byPath) Len() int {
	return len(b)
}

func (b byPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b byPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortMetricsByPath(result messages.EndpointMetricsList) {
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
}

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	encodeJson(w, gConnectionErrors.GetErrors(), r.Form.Get("format") == "text")
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
type byEndpointHandler struct {
}

func (h byEndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	metricStore, endpoints := gHostsPortsAndStore.Get()
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
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], "/"+hostNameAndPath[2]
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	app := gApplicationList.ByName(name)
	if app == nil {
		httpError(w, 404)
		return
	}
	hostAndPort := fmt.Sprintf("%s:%d", host, app.Port())
	var data messages.EndpointMetricsList
	endpoint := endpoints[hostAndPort]
	if endpoint != nil {
		data = gatherDataForEndpoint(metricStore, endpoint, path, history, isSingleton)
	} else {
		data = make(messages.EndpointMetricsList, 0)
	}
	encodeJson(w, data, r.Form.Get("format") == "text")
}

type timeTakenWriter struct {
	pstore.Writer
	TimeTaken time.Duration
}

func (t *timeTakenWriter) Write(records []pstore.Record) error {
	start := time.Now()
	result := t.Writer.Write(records)
	t.TimeTaken += time.Now().Sub(start)
	return result
}

// A fake pstore.Writer implementation. This fake simply stalls 50ms with each
// write.
type stallWriter struct {
}

func (s stallWriter) IsTypeSupported(kind types.Type) bool {
	return true
}

func (s stallWriter) Write(records []pstore.Record) error {
	time.Sleep(50 * time.Millisecond)
	if rand.Float64() < 0.01 {
		return aWriteError
	}
	return nil
}

func newWriter() (result pstore.Writer, err error) {
	if fKafkaConfigFile == "" {
		return stallWriter{}, nil
	}
	f, err := os.Open(fKafkaConfigFile)
	if err != nil {
		return
	}
	defer f.Close()
	var config kafka.Config
	if err = config.Read(f); err != nil {
		return
	}
	return kafka.NewWriter(&config)
}

func updateEndpoints(machines *mdb.Mdb) {
	_, endpoints := gHostsPortsAndStore.Get()
	newEndpoints := make(datastructs.HostsAndPorts)
	addEndpoints(machines, endpoints, newEndpoints)
	gHostsPortsAndStore.Update(newEndpoints)
}

func addEndpoints(
	machines *mdb.Mdb, origHostsAndPorts, hostsAndPorts datastructs.HostsAndPorts) {
	apps := gApplicationList.All()
	for _, machine := range machines.Machines {
		for _, app := range apps {
			hostsAndPorts.AddIfAbsent(origHostsAndPorts, machine.Hostname, app.Port())
		}
	}
}

func initApplicationList() {
	f, err := os.Open(fAppFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	builder := datastructs.NewApplicationListBuilder()
	if err := builder.ReadConfig(f); err != nil {
		log.Fatal(err)
	}
	gApplicationList = builder.Build()
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	var mdbChannel <-chan *mdb.Mdb
	collector.SetConcurrentPolls(fPollCount)
	collector.SetConcurrentConnects(fConnectionCount)
	firstEndpoints := make(datastructs.HostsAndPorts)
	realEndpoints := (fHostFile == "")
	writer, err := newWriter()
	if err != nil {
		log.Fatal(err)
	}
	pstoreHandler := newPStoreHandler(writer)
	if !realEndpoints {
		f, err := os.Open(fHostFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		var hostAndPort string
		var endpointNames []string
		var endpointPorts []int
		_, err = fmt.Fscanln(f, &hostAndPort)
		for ; err != io.EOF; _, err = fmt.Fscanln(f, &hostAndPort) {
			if err != nil {
				continue
			}
			splits := strings.SplitN(hostAndPort, ":", 2)
			port, _ := strconv.Atoi(splits[1])
			endpointNames = append(endpointNames, splits[0])
			endpointPorts = append(endpointPorts, port)
		}
		for i := 0; i < totalNumberOfEndpoints; i++ {
			name, port := endpointNameAndPort(endpointNames, endpointPorts, i)
			if i%1000 == 37 {
				port = 7776
			}
			firstEndpoints.AddIfAbsent(nil, name, port)
		}
		builder := datastructs.NewApplicationListBuilder()
		builder.Add(6910, "Health Metrics")
		builder.Add(7776, "Non existent App")
		gApplicationList = builder.Build()
	} else {
		initApplicationList()
		mdbChannel = mdbd.StartMdbDaemon(fMdbFile, log.New(os.Stderr, "", log.LstdFlags))
		addEndpoints(<-mdbChannel, nil, firstEndpoints)
	}
	tricorder.RegisterMetric(
		"collector/collectionTimes",
		gCollectionTimesDist,
		units.Second,
		"Collection Times")
	tricorder.RegisterMetric(
		"collector/changedMetricsPerEndpoint",
		gChangedMetricsPerEndpointDist,
		units.None,
		"Changed metrics per sweep")
	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration")
	gStatusCounts.RegisterMetrics()
	pstoreHandler.RegisterMetrics()
	programStartTime := time.Now()
	tricorder.RegisterMetric(
		"collector/elapsedTime",
		func() time.Duration {
			return time.Now().Sub(programStartTime)
		},
		units.Second,
		"elapsed time")
	tricorder.RegisterMetric(
		"collector/activeEndpointCount",
		func() int {
			_, endpoints := gHostsPortsAndStore.Get()
			return len(endpoints)
		},
		units.Second,
		"elapsed time")

	fmt.Println("Initialization started.")
	// Value interface + float64 = 24 bytes
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	if totalMemoryToUse > 0 {
		fPageCount = int(totalMemoryToUse / uint64(fBytesPerPage))
	}
	gHostsPortsAndStore.Init(
		fBytesPerPage/24, fPageCount, firstEndpoints)
	fmt.Println("Initialization complete.")
	firstStore, _ := gHostsPortsAndStore.Get()
	firstStore.RegisterMetrics()
	// Endpoint refresher goroutine
	if realEndpoints {
		go func() {
			for {
				updateEndpoints(<-mdbChannel)
			}
		}()
	}

	// Metric collection goroutine. Collect metrics every minute.
	go func() {
		for {
			metricStore, endpoints := gHostsPortsAndStore.Get()
			logger := &loggerType{Store: metricStore}
			sweepTime := time.Now()
			for _, endpoint := range endpoints {
				endpoint.Poll(sweepTime, logger)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			if sweepDuration < fCollectionFrequency {
				time.Sleep(fCollectionFrequency - sweepDuration)
			}
		}
	}()

	// persistent storage writing goroutine. Write every 30s by default.
	// Notice that this single goroutine handles all the persistent
	// storage writing as multiple goroutines must not access the
	// pstoreHandler instance. accessing pstoreHandler metrics is the
	// one exception to this rule.
	go func() {
		for {
			metricStore, _ := gHostsPortsAndStore.Get()
			writeTime := time.Now()
			pstoreHandler.StartVisit()
			metricStore.VisitAllEndpoints(pstoreHandler)
			pstoreHandler.EndVisit()
			writeDuration := time.Now().Sub(writeTime)
			if writeDuration < fPStoreUpdateFrequency {
				time.Sleep(fPStoreUpdateFrequency - writeDuration)
			}
		}
	}()

	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			AS:  gApplicationStats,
			HPS: &gHostsPortsAndStore,
			AL:  gApplicationList,
		}})

	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{byEndpointHandler{}}))
	http.Handle(
		"/api/errors/",
		gzipHandler{errorHandler{}},
	)
	if err := http.ListenAndServe(":8187", nil); err != nil {
		log.Fatal(err)
	}
}

func init() {
	flag.StringVar(
		&fKafkaConfigFile,
		"kafka_config_file",
		"",
		"kafka configuration file")
	flag.IntVar(
		&fPageCount,
		"page_count",
		30*1000*1000,
		"Buffer size per endpoint in records")
	flag.IntVar(
		&fBytesPerPage,
		"bytes_per_page",
		1024,
		"Space for new metrics for each endpoint in records")
	flag.StringVar(
		&fHostFile,
		"host_file",
		"",
		"File containing all the nodes")
	flag.StringVar(
		&fAppFile,
		"app_file",
		"apps.yaml",
		"File containing mapping of ports to apps")
	flag.StringVar(
		&fMdbFile,
		"mdb_file",
		"/var/lib/Dominator/mdb",
		"Name of file from which to read mdb data.")
	flag.IntVar(
		&fPollCount,
		"poll_count",
		collector.ConcurrentPolls(),
		"Maximum number of concurrent polls")
	flag.IntVar(
		&fConnectionCount,
		"connection_count",
		collector.ConcurrentConnects(),
		"Maximum number of concurrent connections")
	flag.DurationVar(
		&fCollectionFrequency,
		"collection_frequency",
		30*time.Second,
		"Amount of time between metric collections")
	flag.DurationVar(
		&fPStoreUpdateFrequency,
		"pstore_update_frequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
	flag.IntVar(
		&fPStoreBatchSize,
		"pstore_batch_size",
		1000,
		"Batch to write at least this many records to persistent storage")
	flag.StringVar(
		&fCluster,
		"cluster",
		"ash1",
		"The cluster name")
}
