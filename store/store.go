package store

import (
	"container/list"
	"github.com/Symantec/scotty"
	"github.com/Symantec/tricorder/go/tricorder"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	kReapingThreshold = 2
)

var (
	kPlusInf = math.Inf(1)
)

var (
	// application wide metrics
	gPagesPerMetricDist = tricorder.NewGeometricBucketer(
		1.0, 1e6).NewNonCumulativeDistribution()
	gLastTimeEvicted  *time.Time
	gMetricValueCount = &countType{}
)

type countType struct {
	lock  sync.Mutex
	value int64
}

func (c *countType) Add(i int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value += i
}

func (c *countType) Get() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}

// metricInfoStoreType keeps a pool of pointers to unique MetricInfo instances
type metricInfoStoreType struct {
	ByInfo map[MetricInfo]*MetricInfo
	ByName map[string][]*MetricInfo
}

func (m *metricInfoStoreType) Init() {
	m.ByInfo = make(map[MetricInfo]*MetricInfo)
	m.ByName = make(map[string][]*MetricInfo)
}

// Register returns the correct MetricInfo instance from the pool for
// passed in metric. Register will always return a non nil value.
func (m *metricInfoStoreType) Register(metric *trimessages.Metric) (
	result *MetricInfo) {
	infoStruct := MetricInfo{
		path:        metric.Path,
		description: metric.Description,
		unit:        metric.Unit,
		kind:        metric.Kind,
		bits:        metric.Bits}
	result, alreadyExists := m.ByInfo[infoStruct]
	if alreadyExists {
		return
	}
	result = &infoStruct
	m.ByInfo[infoStruct] = result
	m.ByName[infoStruct.path] = append(m.ByName[infoStruct.path], result)
	return
}

type tsValueType struct {
	TimeStamp float64
	Value     interface{}
}

type pageType []tsValueType

func (p *pageType) Add(timestamp float64, value interface{}) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length] = tsValueType{TimeStamp: timestamp, Value: value}
}

func (p *pageType) Reap() *pageType {
	return p
}

func (p *pageType) Clear() {
	*p = (*p)[:0]
}

func (p pageType) LatestValue() (float64, interface{}) {
	ptr := &p[len(p)-1]
	return ptr.TimeStamp, ptr.Value
}

func (p pageType) IsFull() bool {
	return len(p) == cap(p)
}

func (p pageType) Fetch(
	applicationId *scotty.Endpoint,
	id *MetricInfo,
	start, end float64,
	result Appender) (keepGoing bool) {
	lastIdx := p.FindGreaterOrEqual(end)
	if lastIdx == 0 {
		return true
	}
	firstIdx := p.FindGreater(start) - 1
	if firstIdx < 0 {
		keepGoing = true
		firstIdx = 0
	}
	record := Record{
		ApplicationId: applicationId,
		Info:          id}
	for i := lastIdx - 1; i >= firstIdx; i-- {
		record.TimeStamp = p[i].TimeStamp
		record.Value = p[i].Value
		result.Append(&record)
	}
	return
}

func (p pageType) FindGreaterOrEqual(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp >= ts })
}

func (p pageType) FindGreater(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp > ts })
}

type timeSeriesType struct {
	id    *MetricInfo
	lock  sync.Mutex
	pages list.List

	// True if time series is closed. No new values may be added
	// to a closed time series, but closed time series may give up pages.
	closed bool
}

func newTimeSeriesType(
	id *MetricInfo,
	page *pageType,
	timestamp float64,
	value interface{}) *timeSeriesType {
	page.Add(timestamp, value)
	result := &timeSeriesType{id: id}
	result.pages.Init().PushBack(page)
	gPagesPerMetricDist.Add(1.0)
	return result
}

func (t *timeSeriesType) PageCount() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.pages.Len()
}

func (t *timeSeriesType) Close() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.closed = true
}

func (t *timeSeriesType) Reap() *pageType {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldLen := float64(t.pages.Len())
	front := t.pages.Front()
	if front == nil {
		return nil
	}
	result := t.pages.Remove(t.pages.Front()).(*pageType)
	// Don't update last time evicted for closed time series.
	// Also closed time series give up all their pages even
	// partially filled ones.
	if !t.closed {
		latestTime, _ := result.LatestValue()
		goLatestTime := trimessages.FloatToTime(latestTime)
		gLastTimeEvicted = &goLatestTime
		if !result.IsFull() {
			panic("Oops, timeSeries is giving up a partially filled page.")
		}
	}
	gMetricValueCount.Add(int64(-len(*result)))
	if oldLen == 1.0 {
		gPagesPerMetricDist.Remove(oldLen)
	} else {
		gPagesPerMetricDist.Update(oldLen, oldLen-1.0)
	}
	return result
}

func (t *timeSeriesType) needToAdd(timestamp float64, value interface{}) (
	needToAdd, needPage bool) {
	if t.closed {
		return
	}
	latestPage := t.latestPage()
	latestTs, latestValue := latestPage.LatestValue()
	if timestamp <= latestTs || value == latestValue {
		return
	}
	return true, latestPage.IsFull()
}

func (t *timeSeriesType) NeedToAdd(timestamp float64, value interface{}) (
	needToAdd, needPage bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.needToAdd(timestamp, value)
}

// Must call NeedToAdd first!
func (t *timeSeriesType) Add(
	timestamp float64, value interface{}, page *pageType) (
	isAddSucceeded, isEligibleForReaping bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.closed {
		return
	}
	needToAdd, needPage := t.needToAdd(timestamp, value)
	if !needToAdd || (needPage && page == nil) || (!needPage && page != nil) {
		panic("Multiple goroutines trying to add to this time series!")
	}
	var latestPage *pageType
	if page == nil {
		latestPage = t.latestPage()
	} else {
		oldLen := float64(t.pages.Len())
		t.pages.PushBack(page)
		gPagesPerMetricDist.Update(oldLen, oldLen+1)
		if t.pages.Len() > kReapingThreshold {
			isEligibleForReaping = true
		}
		latestPage = page
	}
	latestPage.Add(timestamp, value)
	isAddSucceeded = true
	return
}

func (t *timeSeriesType) Fetch(
	applicationId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	t.lock.Lock()
	defer t.lock.Unlock()
	var e *list.Element
	for e = t.pages.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*pageType)
		if !page.Fetch(applicationId, t.id, start, end, result) {
			break
		}
	}
}

func (t *timeSeriesType) latestPage() *pageType {
	return t.pages.Back().Value.(*pageType)
}

type timeSeriesCollectionType struct {
	applicationId   *scotty.Endpoint
	lock            sync.Mutex
	timeSeries      map[*MetricInfo]*timeSeriesType
	metricInfoStore metricInfoStoreType
	closed          bool
}

func newTimeSeriesCollectionType(
	app *scotty.Endpoint) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId: app,
		timeSeries:    make(map[*MetricInfo]*timeSeriesType),
	}
	result.metricInfoStore.Init()
	return result
}

func (c *timeSeriesCollectionType) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closed = true
}

func (c *timeSeriesCollectionType) All() (result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, ts := range c.timeSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) Lookup(m *trimessages.Metric) (
	id *MetricInfo, timeSeries *timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	id = c.metricInfoStore.Register(m)
	timeSeries = c.timeSeries[id]
	return
}

func (c *timeSeriesCollectionType) TsByName(name string) (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	infoList := c.metricInfoStore.ByName[name]
	// We aren't guarnateed that returned array will be the
	// same length as infoList. When adding a new
	// timeSeries, we add its id first, release the lock to
	// get a new page, then reacquire the lock to add the new
	// timeSeries.
	for _, info := range infoList {
		result = append(result, c.timeSeries[info])
	}
	return
}

func (c *timeSeriesCollectionType) TsByPrefix(prefix string) (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, info := range c.metricInfoStore.ByInfo {
		if strings.HasPrefix(info.Path(), prefix) {
			result = append(result, c.timeSeries[info])
		}
	}
	return
}

func (c *timeSeriesCollectionType) AddTimeSeries(
	id *MetricInfo, series *timeSeriesType) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed {
		return false
	}
	if c.timeSeries[id] != nil {
		panic("Multiple goroutines trying to add to this time series collection.")
	}
	c.timeSeries[id] = series
	return true
}

func (c *timeSeriesCollectionType) Add(
	timestamp float64,
	m *trimessages.Metric,
	supplier *pageSupplierType) bool {
	// We must take care not to call supplier.Get() while we are holding
	// a lock. Because supplier.Get() may have to obtain a lock to reap
	// a page, calling it while holding a lock will make deadlock
	// possible.
	id, timeSeries := c.Lookup(m)
	if timeSeries == nil {
		newPage := supplier.Get()
		timeSeries := newTimeSeriesType(
			id, newPage, timestamp, m.Value)
		if !c.AddTimeSeries(id, timeSeries) {
			supplier.AddFront(newPage)
			return false
		}
		return true
	}
	needToAdd, needPage := timeSeries.NeedToAdd(timestamp, m.Value)
	if needToAdd {
		var page *pageType
		if needPage {
			page = supplier.Get()
		}
		isAddSucceeded, isEligibleForReaping := timeSeries.Add(
			timestamp, m.Value, page)
		if !isAddSucceeded {
			if page != nil {
				supplier.AddFront(page)
			}
			return false
		}
		if isEligibleForReaping {
			supplier.Add(timeSeries)
		}
		return true
	}
	return false
}

func (c *timeSeriesCollectionType) ByName(
	name string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	for _, timeSeries := range c.TsByName(name) {
		timeSeries.Fetch(c.applicationId, start, end, result)
	}
}

func (c *timeSeriesCollectionType) ByPrefix(
	prefix string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	for _, timeSeries := range c.TsByPrefix(prefix) {
		timeSeries.Fetch(c.applicationId, start, end, result)
	}
}

func (c *timeSeriesCollectionType) Latest(result Appender) {
	for _, timeSeries := range c.TsByPrefix("") {
		timeSeries.Fetch(c.applicationId, kPlusInf, kPlusInf, result)
	}
}

type reaperType interface {
	// Reap must succeed and return a non-nil page
	Reap() *pageType
}

type pageSupplierType struct {
	lock  sync.Mutex
	queue list.List
}

func newPageSupplierType(valueCountPerPage, pageCount int) *pageSupplierType {
	var result pageSupplierType
	result.queue.Init()
	for i := 0; i < pageCount; i++ {
		newPage := make(pageType, valueCountPerPage)
		result.queue.PushBack(&newPage)
	}
	return &result
}

func (s *pageSupplierType) Get() (result *pageType) {
	for result == nil {
		reaper := s.pop()
		result = reaper.Reap()
	}
	result.Clear()
	return result
}

func (s *pageSupplierType) Add(r reaperType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.queue.PushBack(r)
}

func (s *pageSupplierType) AddFront(r reaperType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.queue.PushFront(r)
}

func (s *pageSupplierType) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.queue.Len()
}

func (s *pageSupplierType) MoveToFront(belongsInFront map[*timeSeriesType]bool) {
	frontCounts := make(map[reaperType]int, len(belongsInFront))
	for series, ok := range belongsInFront {
		if ok {
			frontCounts[series] = series.PageCount()
		}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	e := s.queue.Front()
	for e != nil {
		current := e
		// Advance e ahead of time as removing an Element resets its
		// pointers.
		e = e.Next()
		if _, ok := frontCounts[current.Value.(reaperType)]; ok {
			s.queue.Remove(current)
		}
	}
	for reaper, count := range frontCounts {
		for i := 0; i < count; i++ {
			s.queue.PushFront(reaper)
		}
	}
}

func (s *pageSupplierType) pop() reaperType {
	s.lock.Lock()
	defer s.lock.Unlock()
	front := s.queue.Front()
	if front == nil {
		panic("No more pages left! Make page size smaller, add memory, or both.")
	}
	return s.queue.Remove(front).(reaperType)
}

type recordListType []*Record

func (l *recordListType) Append(r *Record) {
	recordCopy := *r
	*l = append(*l, &recordCopy)
}

func (b *Builder) registerEndpoint(endpointId *scotty.Endpoint) {

	if (*b.store).byApplication[endpointId] != nil {
		panic("Endpoint already registered")
	}
	var collection *timeSeriesCollectionType
	if b.prevStore != nil {
		collection = b.prevStore.byApplication[endpointId]
	}
	if collection == nil {
		collection = newTimeSeriesCollectionType(endpointId)
	}
	(*b.store).byApplication[endpointId] = collection
}

func (b *Builder) build() *Store {
	if b.prevStore != nil {
		closeUnusedInPreviousStore(*b.store, b.prevStore)
	}
	result := *b.store
	*b.store = nil
	return result
}

func (s *Store) newBuilder() *Builder {
	store := &Store{
		byApplication:    make(map[*scotty.Endpoint]*timeSeriesCollectionType),
		supplier:         s.supplier,
		totalPageCount:   s.totalPageCount,
		maxValuesPerPage: s.maxValuesPerPage,
	}
	return &Builder{store: &store, prevStore: s}
}

func (s *Store) add(
	endpointId *scotty.Endpoint,
	timestamp float64, m *trimessages.Metric) bool {
	return s.byApplication[endpointId].Add(timestamp, m, s.supplier)
}

func (s *Store) addBatch(
	endpointId *scotty.Endpoint,
	timestamp float64,
	metricList trimessages.MetricList,
	filter func(*trimessages.Metric) bool) int {
	result := 0
	for _, metric := range metricList {
		if filter != nil && !filter(metric) {
			continue
		}
		if s.add(endpointId, timestamp, metric) {
			result++
		}
	}
	gMetricValueCount.Add(int64(result))
	return result
}

func (s *Store) byNameAndEndpoint(
	name string,
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByName(name, start, end, result)
}

func (s *Store) byPrefixAndEndpoint(
	prefix string,
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix(prefix, start, end, result)
}

func (s *Store) byEndpoint(
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix("", start, end, result)
}

func (s *Store) latestByEndpoint(
	endpointId *scotty.Endpoint,
	result Appender) {
	s.byApplication[endpointId].Latest(result)
}

func (s *Store) visitAllEndpoints(v Visitor) (err error) {
	for endpointId := range s.byApplication {
		if err = v.Visit(s, endpointId); err != nil {
			return
		}
	}
	return
}

func (s *Store) registerMetrics() (err error) {
	// Let Garbage collector reclaim s.
	supplier := s.supplier
	totalPageCount := s.totalPageCount
	maxValuesPerPage := s.maxValuesPerPage
	if err = tricorder.RegisterMetric(
		"/store/pagesPerMetric",
		gPagesPerMetricDist,
		units.None,
		"Number of pages used per metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/lastTimeStampEvicted",
		&gLastTimeEvicted,
		units.None,
		"Latest timestamp evicted from data store."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/availablePages",
		func() int {
			return supplier.Len()
		},
		units.None,
		"Number of pages available to hold new metrics."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/totalPages",
		&totalPageCount,
		units.None,
		"Total number of pages."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/maxValuesPerPage",
		&maxValuesPerPage,
		units.None,
		"Maximum number ofvalues that can fit in a page."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/pageUtilization",
		func() float64 {
			return float64(gMetricValueCount.Get()) / gPagesPerMetricDist.Sum() / float64(maxValuesPerPage)
		},
		units.None,
		"Page utilization 0.0 - 1.0"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/metricValueCount",
		gMetricValueCount.Get,
		units.None,
		"Number of stored metrics values and timestamps"); err != nil {
		return
	}
	return
}

func reclaimPages(
	collections []*timeSeriesCollectionType, supplier *pageSupplierType) {
	// First close all the collections to ensure that no new time series
	// come in while we close all the existing time series.
	for _, coll := range collections {
		coll.Close()
	}
	// Now close each time series
	closedTimeSeries := make(map[*timeSeriesType]bool)
	for _, coll := range collections {
		timeSeriesList := coll.All()
		for _, timeSeries := range timeSeriesList {
			timeSeries.Close()
			closedTimeSeries[timeSeries] = true
		}
	}
	supplier.MoveToFront(closedTimeSeries)
}

func closeUnusedInPreviousStore(store, prevStore *Store) {
	var unusedTimeSeriesCollections []*timeSeriesCollectionType
	for endpointId := range prevStore.byApplication {
		if store.byApplication[endpointId] == nil {
			unusedTimeSeriesCollections = append(
				unusedTimeSeriesCollections,
				prevStore.byApplication[endpointId])
		}
	}
	reclaimPages(unusedTimeSeriesCollections, prevStore.supplier)
}
