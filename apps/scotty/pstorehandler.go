package main

import (
	"flag"
	"fmt"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/influx"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/scotty/pstore/tsdb"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"log"
	"path"
	"sync"
	"time"
)

const (
	kPStoreIteratorName       = "pstore"
	kCollectorIteratorName    = "collector"
	kLookAheadWritingToPStore = 5
	kLeaseSpan                = 60
)

var (
	fPStoreUpdateFrequency = flag.Duration(
		"pstoreUpdateFrequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
	fPersistentStoreType = flag.String(
		"persistentStoreType",
		"kafka",
		"Type of persistent store must be either 'kafka','influx','tsdb', or 'none'")
)

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
		iterator = store.NamedIteratorCoordinate(
			iterator, p.maybeNilCoord, kLeaseSpan)
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
