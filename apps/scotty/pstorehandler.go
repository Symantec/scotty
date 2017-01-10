package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/fsutil"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/lib/gate"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"io/ioutil"
	"log"
	"os"
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
	kBucketer = tricorder.NewGeometricBucketer(1e-4, 1000.0)
)

var (
	fPStoreUpdateFrequency = flag.Duration(
		"pstoreUpdateFrequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
)

func writerIteratorName(name string) string {
	return fmt.Sprintf("%s/%s", kPStoreIteratorName, name)
}

func collectorIteratorName(name string) string {
	return fmt.Sprintf("%s/%s", kCollectorIteratorName, name)
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
	return collectorIteratorName(t.name)
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
	// listener gets called whenever writing is blocked or unblocked
	// because of consul.
	WithStateListener(listener func(blocked bool)) store.Coordinator
	// WatchPStoreConfig watches the pstore config file in consul and
	// emits the new config file each time it changes. If caller provides
	// a non-nil done, caller can close done to request watching the config
	// file to end.
	WatchPStoreConfig(done <-chan struct{}) <-chan string
}

// appList converts port numbers to application names.
// consumer is what builds the consumer to write to the pstore
// perMetricWriteTimes is the distribution to where write speeds get
// recorded. totalTimeSpentDist is the distribution to where total time spent
// doing one cycle of writing out the scotty store gets recorded. If
// maybeNilCoordBuilder is not nil, it arranges to acquire a lease from
// consul before any writing happens.
func newPStoreHandler(
	appList *datastructs.ApplicationList,
	consumer *pstore.ConsumerWithMetricsBuilder,
	perMetricWriteTimes *tricorder.CumulativeDistribution,
	totalTimeSpentDist *tricorder.CumulativeDistribution,
	maybeNilCoordBuilder coordinatorBuilderType) *pstoreHandlerType {
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
		totalTimeSpentDist:  totalTimeSpentDist,
		perMetricWriteTimes: perMetricWriteTimes,
		visitorMetricsStore: visitorMetricsStore,
		maybeNilCoord:       maybeNilCoord,
	}
}

func (p *pstoreHandlerType) Name() string {
	return p.consumer.Name()
}

// start visiting scotty
func (p *pstoreHandlerType) StartVisit() {
	p.startTime = time.Now()
}

// end visit
func (p *pstoreHandlerType) EndVisit(theStore *store.Store) {
	p.consumer.Flush()
	p.visitorMetricsStore.SetTimeLeft(
		duration.FromFloat(theStore.TimeLeft(p.iteratorName())))
	totalTime := time.Now().Sub(p.startTime)
	p.totalTimeSpentDist.Add(totalTime)

}

// visit a single scotty endpoint
func (p *pstoreHandlerType) Visit(
	theStore *store.Store, endpointId interface{}) error {

	hostName := endpointId.(*collector.Endpoint).HostName()
	port := endpointId.(*collector.Endpoint).Port()
	appName := p.appList.ByPort(port).Name()
	iterator, timeLeft := p.namedIterator(theStore, endpointId)
	if p.maybeNilCoord != nil {
		// aMetricStore from the consumer exposes the same filtering that
		// the consumer does internally.
		aMetricStore := p.ConsumerMetricsStore()
		// Even though the consumer filters out the records it can't write,
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

// store consumer attributes at attribute.
func (p *pstoreHandlerType) Attributes(attributes *pstore.ConsumerAttributes) {
	p.consumer.Attributes(attributes)
}

// Returns an instance for counting the metrics left to write.
func (p *pstoreHandlerType) TotalCount() *totalCountType {
	var attributes pstore.ConsumerAttributes
	p.Attributes(&attributes)
	return &totalCountType{
		metrics:    p.ConsumerMetricsStore(),
		name:       p.Name(),
		rollUpSpan: attributes.RollUpSpan,
	}
}

// Return the statistics for the consumer and other thread-safe parts of
// the consumer
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
		data.ValuesNotWritten,
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
		fmt.Sprintf("writer/%s/paused", p.Name()),
		&data.Paused,
		group,
		units.None,
		"true if writer paused; false otherwise"); err != nil {
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
	return writerIteratorName(p.Name())
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

// All the metrics for a pstore runner
type pstoreRunnerAttributesType struct {
	pstore.ConsumerMetrics
	pstore.ConsumerAttributes
	TotalTimeSpentDist *tricorder.CumulativeDistribution
}

// pstoreRunnerType represents a single goroutine writing metrics to a
// particular data store
type pstoreRunnerType struct {
	stats              *datastructs.ApplicationStatuses
	metrics            *pstore.ConsumerMetricsStore
	attrs              pstore.ConsumerAttributes
	count              *totalCountType
	name               string
	refreshRate        time.Duration
	totalTimeSpentDist *tricorder.CumulativeDistribution
	done               chan struct{}
}

// stats has all the scotty data.
// consumer is what can build the consumer to write to the pstore
// maybeNilAttrs are the statistics from the previous pstore runner. If nil,
// this pstore runner gets created with all 0 statistics.
// If maybeNilCoordBuilder is not nil, it arranges for the pstore runner
// to obtain a lease from consul before writing any data.
func newPStoreRunner(
	stats *datastructs.ApplicationStatuses,
	consumer *pstore.ConsumerWithMetricsBuilder,
	maybeNilInAttrs *pstoreRunnerAttributesType,
	maybeNilCoordBuilder coordinatorBuilderType,
	logger *log.Logger) *pstoreRunnerType {
	var perMetricWriteTimes *tricorder.CumulativeDistribution
	var totalTimeSpentDist *tricorder.CumulativeDistribution
	if maybeNilInAttrs != nil {
		perMetricWriteTimes = maybeNilInAttrs.PerMetricWriteTimes
		totalTimeSpentDist = maybeNilInAttrs.TotalTimeSpentDist
		consumer.SetConsumerMetrics(&maybeNilInAttrs.ConsumerMetrics)
	} else {
		perMetricWriteTimes = kBucketer.NewCumulativeDistribution()
		totalTimeSpentDist = kBucketer.NewCumulativeDistribution()
	}
	handler := newPStoreHandler(
		stats.ApplicationList(),
		consumer,
		perMetricWriteTimes,
		totalTimeSpentDist,
		maybeNilCoordBuilder)
	var attributes pstore.ConsumerAttributes
	handler.Attributes(&attributes)
	refreshRate := *fPStoreUpdateFrequency
	if attributes.RollUpSpan > 0 {
		refreshRate = attributes.RollUpSpan
	}
	if err := handler.RegisterMetrics(); err != nil {
		logger.Println(err)
	}
	result := &pstoreRunnerType{
		stats:              stats,
		metrics:            handler.ConsumerMetricsStore(),
		attrs:              attributes,
		count:              handler.TotalCount(),
		name:               handler.Name(),
		refreshRate:        refreshRate,
		totalTimeSpentDist: totalTimeSpentDist,
		done:               make(chan struct{})}
	go result.loop(handler)
	return result
}

// Attributes stores all the metrics for this instance at attr.
func (r *pstoreRunnerType) Attributes(attr *pstoreRunnerAttributesType) {
	attr.ConsumerAttributes = r.attrs
	r.metrics.Metrics(&attr.ConsumerMetrics)
	attr.TotalTimeSpentDist = r.totalTimeSpentDist
}

// Count returns the instance needed to count metrics left to write for this
// runner
func (r *pstoreRunnerType) Count() *totalCountType {
	return r.count
}

// Close stops this instance from writing to its pstore and unregisters it
// from tricorder. Close waits for any in-progress writes to finish before
// returning.
func (r *pstoreRunnerType) Close() {
	r.metrics.DisableWrites()
	tricorder.UnregisterPath(fmt.Sprintf("writer/%s", r.name))
	close(r.done)
}

// loop is the main loop of the pstore runner. handler is the instance that
// knows how to visit the scotty metric store and does the writing.
func (r *pstoreRunnerType) loop(handler *pstoreHandlerType) {
	for {
		metricStore := r.stats.Store()
		writeTime := time.Now()
		handler.StartVisit()
		metricStore.VisitAllEndpoints(handler)
		handler.EndVisit(metricStore)
		writeDuration := time.Now().Sub(writeTime)
		// Check to see if Close was called to shut down this runner.
		select {
		case <-r.done:
			return
		case <-time.After(r.refreshRate - writeDuration):
		}
	}
}

// newPStoreConsumer creates the consumer builders from the config file.
// reader is the config file. If maybeNilMemoryManager is non-nil, this
// function adds hooks to notify maybeNilMemoryManager each time a write
// to the underlying pstore happens. newPStoreConsumer returns the
// consumer builders keyed by pstore name.
func newPStoreConsumers(
	reader io.Reader, maybeNilMemoryManager *memoryManagerType) (
	result map[string]*pstore.ConsumerWithMetricsBuilder, err error) {
	var builderList []*pstore.ConsumerWithMetricsBuilder
	builderList, err = config.NewConsumerBuilders(reader)
	if err != nil {
		return
	}
	if maybeNilMemoryManager != nil {
		memoryManager := maybeNilMemoryManager
		// Add hook to check memory after each write
		for _, builder := range builderList {
			builder.AddHook(writeHookerType{wrapped: memoryManager})
		}
	}
	result = make(
		map[string]*pstore.ConsumerWithMetricsBuilder, len(builderList))
	for _, builder := range builderList {
		result[builder.Name()] = builder
	}
	return
}

// totalCountCollectionType is a collection of totalCountType instances.
// totalCountCollectionType objects are safe to use with multiple goroutines
// with the same caveat as totalCountType
type totalCountCollectionType struct {
	criticalSection *gate.Gate
	lock            sync.Mutex
	// content of counts must not be modified in place
	counts []*totalCountType
}

func newTotalCountCollectionType() *totalCountCollectionType {
	return &totalCountCollectionType{criticalSection: gate.New()}
}

// Update updates the records left to write for all instances curently in
// this collection
func (t *totalCountCollectionType) Update(
	astore *store.Store, endpointId interface{}) {
	if !t.criticalSection.Enter() {
		return
	}
	defer t.criticalSection.Exit()
	for _, count := range t.get() {
		count.Update(astore, endpointId)
	}
}

// Set makes this collection store all the instances in counts
func (t *totalCountCollectionType) Set(counts []*totalCountType) {
	var countCopy []*totalCountType
	if len(counts) > 0 {
		countCopy = make([]*totalCountType, len(counts))
		copy(countCopy, counts)
	}
	// Wait to finish up with existing group of counts. We end instead of
	// pause because skipping the critical section code won't throw
	// off the counting as the iterator mechanism of counting is self
	// correcting.
	t.criticalSection.End()
	t.lock.Lock()
	t.counts = countCopy
	t.lock.Unlock()
	// Now that we have new counts, start up critical section again
	t.criticalSection.Start()

}

// Returned array must not be mutated
func (t *totalCountCollectionType) get() []*totalCountType {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.counts
}

func setIteratorTo(
	astore *store.Store, endpointIds []interface{}, dest, src string) {
	for _, id := range endpointIds {
		astore.SetIteratorTo(id, dest, src)
	}
}

// pstoreRunnersByNameType is a map of pstore name to the corresponding
// pstore runner writing metrics to that pstore. Instances of this type
// must be treated as immutable.
type pstoreRunnersByNameType map[string]*pstoreRunnerType

// pstoreContextType runs the update from config file loop. It contains the
// common state that doesn't change so that they don't have to be passed as
// parameters to UpdateFromConfigFile.
type pstoreContextType struct {
	Stats                 *datastructs.ApplicationStatuses
	MaybeNilMemoryManager *memoryManagerType
	Logger                *log.Logger
	MaybeNilCoordBuilder  coordinatorBuilderType
}

// UpdateFromConfigFile is called when scotty is starting up and whenever
// the pstore configuration file changes.
//
// UpdateFromConfigFile is expected to update scotty according to the new
// config file by creating new pstore runners to write to the pstores
// prescribed in the config file.
//
// reader has the contents of the updated version of the configuration file
//
// pstoreRunners are the pstoreRunners already writing to pstores.
// pstoreRunners is a map of pstore name to the corresponding pstore runner.
// The first time this function is called, pstores is empty.
//
// counts is responsible for updating the count of metrics left to write for
// each pstore. Since UpdateFromConfigFile processes changes to the
// configuration file, it is this function's responsibility to modify counts
// so that it will update the count of metrics left to write for the new
// config file.
//
// UpdateFromConfigFile returns a brand new pstoreRunnersByName instance
// containing all the pstore runners prescribed in the new config file.
// This same instance is passed to UpdateFromConfigFile the next time it is
// called.
func (c *pstoreContextType) UpdateFromConfigFile(
	reader io.Reader,
	pstoreRunners pstoreRunnersByNameType,
	counts *totalCountCollectionType) pstoreRunnersByNameType {
	// buildersByName has the consumer builders for the new
	// consumers keyed by pstore name as read from new config file
	buildersByName, err := newPStoreConsumers(reader, c.MaybeNilMemoryManager)
	if err != nil {
		c.Logger.Println(err)
		return pstoreRunners
	}

	// Turn off counting new values for existing runners waiting for any
	// in-progress counting to complete.
	counts.Set(nil)

	// shut down all existing runners using separate goroutines so that we can
	// wait for any in-progress writes on all of them to complete in parallel.
	var wg sync.WaitGroup
	for _, v := range pstoreRunners {
		wg.Add(1)
		go func(v *pstoreRunnerType) {
			defer wg.Done()
			v.Close()
		}(v)
	}
	wg.Wait()

	// With counting new values turned off and all writers shut down,
	// it is safe to grab the metrics of all the existing runners
	// We store the metrics keyed by pstore name
	existingAttributesByName := make(
		map[string]pstoreRunnerAttributesType, len(pstoreRunners))
	for k, v := range pstoreRunners {
		var attributes pstoreRunnerAttributesType
		v.Attributes(&attributes)
		existingAttributesByName[k] = attributes
	}

	// Figure out the iterator names whose state needs to be cleared.
	// These are the iterators names for each pstore that was
	// in the last version of the config file but not this version.
	var iteratorNamesStartingAtBeginning []string
	for k := range pstoreRunners {
		if _, ok := buildersByName[k]; !ok {
			iteratorNamesStartingAtBeginning = append(
				iteratorNamesStartingAtBeginning,
				writerIteratorName(k),
				collectorIteratorName(k))
		}
	}
	// Rewind / clear state for the iterator names for each pstore that is
	// going away
	astore := c.Stats.Store()
	endpointIds := astore.Endpoints()
	for _, e := range endpointIds {
		astore.StartAtBeginning(e, iteratorNamesStartingAtBeginning...)
	}

	// For each entry in the config file, start a new runner.
	// Be sure to store each newly created runner in the map we will return
	result := make(pstoreRunnersByNameType)

	// We also have to update counts
	newCounts := make([]*totalCountType, 0, len(buildersByName))
	for k, v := range buildersByName {
		var runner *pstoreRunnerType
		attrs, ok := existingAttributesByName[k]
		if ok {
			// If we get in here, the pstore runner we are crating existed
			// before.

			// If roll up span changed, we have to re-count how many
			// records we have left to write by positioning the collector
			// iterator to the same place as the write iterator. We also
			// have to zero out the left to write count.
			// Eventually the collector iterator will re-count records
			// left to write.
			if attrs.RollUpSpan != v.RollUpSpan() {
				setIteratorTo(
					astore,
					endpointIds,
					collectorIteratorName(k),
					writerIteratorName(k))
				attrs.ConsumerMetrics.ZeroValuesNotWritten()
			}
			// Create a new pstore runner using the statistics from the
			// previous one.
			runner = newPStoreRunner(
				c.Stats,
				v,
				&attrs,
				c.MaybeNilCoordBuilder,
				c.Logger)
		} else {
			// Otherwise, just create a new pstore runner.
			runner = newPStoreRunner(
				c.Stats,
				v,
				nil,
				c.MaybeNilCoordBuilder,
				c.Logger)
		}
		// Compute left-to-write counts for new runner
		newCounts = append(newCounts, runner.Count())
		result[k] = runner
	}
	// Tell counts object to collect left-to-write counts for these new
	// runners
	counts.Set(newCounts)
	return result
}

func configFileLoop(
	context *pstoreContextType,
	ch <-chan io.ReadCloser,
	pstoreRunners pstoreRunnersByNameType,
	counts *totalCountCollectionType) {
	for readCloser := range ch {
		pstoreRunners = context.UpdateFromConfigFile(
			readCloser, pstoreRunners, counts)
		if err := readCloser.Close(); err != nil {
			context.Logger.Println(err)
		}
	}
}

func stringToReadCloserStream(strings <-chan string) <-chan io.ReadCloser {
	result := make(chan io.ReadCloser)
	go func() {
		defer close(result)
		for str := range strings {
			result <- ioutil.NopCloser(bytes.NewBufferString(str))
		}
	}()
	return result
}

// startPStoreLoops starts up the writing to pstores.
func startPStoreLoops(
	stats *datastructs.ApplicationStatuses,
	maybeNilMemoryManager *memoryManagerType,
	logger *log.Logger,
	maybeNilCoordBuilder coordinatorBuilderType) *totalCountCollectionType {
	result := newTotalCountCollectionType()
	var pstoreRunners pstoreRunnersByNameType
	var changeCh <-chan io.ReadCloser
	context := &pstoreContextType{
		Stats: stats,
		MaybeNilMemoryManager: maybeNilMemoryManager,
		Logger:                logger,
		MaybeNilCoordBuilder:  maybeNilCoordBuilder,
	}
	if maybeNilCoordBuilder == nil {
		// pstore config file on unix file system
		// read and process it then add a watch to it for future changes
		configFile := path.Join(*fConfigDir, "pstore.yaml")
		// TODO: Revisit all this logic when fsutil.WatchFile is improved
		// for better error handling.
		if f, err := os.Open(configFile); err != nil {
			logger.Println("No pstore config file found.")
		} else {
			pstoreRunners = context.UpdateFromConfigFile(
				f, pstoreRunners, result)
			if err := f.Close(); err != nil {
				logger.Println(err)
			}
			changeCh = fsutil.WatchFile(configFile, logger)
		}
	} else {
		// pstore config file in consul
		changeCh = stringToReadCloserStream(
			maybeNilCoordBuilder.WatchPStoreConfig(nil))
	}
	if changeCh != nil {
		go configFileLoop(context, changeCh, pstoreRunners, result)
	}
	return result
}
