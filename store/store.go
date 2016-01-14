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
	gPagesPerMetricDist = tricorder.NewGeometricBucketer(
		1.0, 1e6).NewDistribution()
	gLatestTimeEvicted *time.Time
	gMetricValueCount  = &countType{}
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
	applicationId *scotty.Machine,
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

func (t *timeSeriesType) Reap() *pageType {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldLen := float64(t.pages.Len())
	result := t.pages.Remove(t.pages.Front()).(*pageType)
	gPagesPerMetricDist.Update(oldLen, oldLen-1.0)
	latestTime, _ := result.LatestValue()
	goLatestTime := trimessages.FloatToTime(latestTime)
	gLatestTimeEvicted = &goLatestTime
	if !result.IsFull() {
		panic("Oops, timeSeries is giving up a partially filled page.")
	}
	gMetricValueCount.Add(int64(-len(*result)))
	return result
}

func (t *timeSeriesType) needToAdd(timestamp float64, value interface{}) (
	needToAdd, needPage bool) {
	latestPage := t.latestPage()
	latestTs, latestValue := latestPage.LatestValue()
	if timestamp <= latestTs || value == latestValue {
		return false, false
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
	isEligibleForReaping bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
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
	return
}

func (t *timeSeriesType) Fetch(
	applicationId *scotty.Machine,
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
	applicationId   *scotty.Machine
	lock            sync.Mutex
	timeSeries      map[*MetricInfo]*timeSeriesType
	metricInfoStore metricInfoStoreType
}

func newTimeSeriesCollectionType(
	app *scotty.Machine) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId: app,
		timeSeries:    make(map[*MetricInfo]*timeSeriesType),
	}
	result.metricInfoStore.Init()
	return result
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
	id *MetricInfo, series *timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.timeSeries[id] != nil {
		panic("Multiple goroutines trying to add to this time series collection.")
	}
	c.timeSeries[id] = series
}

func (c *timeSeriesCollectionType) Add(
	timestamp float64,
	m *trimessages.Metric,
	supplier pageSupplierType) bool {
	// We must take care not to call supplier.Get() while we are holding
	// a lock. Because supplier.Get() may have to obtain a lock to reap
	// a page, calling it while holding a lock will make deadlock
	// possible.
	id, timeSeries := c.Lookup(m)
	if timeSeries == nil {
		timeSeries := newTimeSeriesType(
			id, supplier.Get(), timestamp, m.Value)
		c.AddTimeSeries(id, timeSeries)
		return true
	}
	needToAdd, needPage := timeSeries.NeedToAdd(timestamp, m.Value)
	if needToAdd {
		var page *pageType
		if needPage {
			page = supplier.Get()
		}
		if timeSeries.Add(timestamp, m.Value, page) {
			supplier <- timeSeries
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

type pageSupplierType chan reaperType

func newPageSupplierType(valueCountPerPage, pageCount int) pageSupplierType {
	result := make(pageSupplierType, pageCount)
	for i := 0; i < pageCount; i++ {
		newPage := make(pageType, valueCountPerPage)
		result <- &newPage
	}
	return result
}

func (s pageSupplierType) Get() *pageType {
	select {
	case reaper := <-s:
		result := reaper.Reap()
		result.Clear()
		return result
	default:
		panic("No more pages left! Make page size smaller, add memory, or both.")
	}
}

type recordListType []*Record

func (l *recordListType) Append(r *Record) {
	recordCopy := *r
	*l = append(*l, &recordCopy)
}

func (b *Builder) registerMachine(machineId *scotty.Machine) {
	if b.byApplication[machineId] != nil {
		panic("Machine already registered")
	}
	var collection *timeSeriesCollectionType
	if b.prevStore != nil {
		collection = b.prevStore.byApplication[machineId]
	}
	if collection == nil {
		collection = newTimeSeriesCollectionType(machineId)
	}
	b.byApplication[machineId] = collection
}

func (b *Builder) build() *Store {
	byApplication := b.byApplication
	supplier := b.supplier
	b.byApplication = nil
	b.supplier = nil
	return &Store{
		byApplication: byApplication,
		supplier:      supplier,
	}
}

func (s *Store) newBuilder() *Builder {
	return &Builder{
		byApplication: make(map[*scotty.Machine]*timeSeriesCollectionType),
		supplier:      s.supplier,
		prevStore:     s}
}

func (s *Store) add(
	machineId *scotty.Machine,
	timestamp float64, m *trimessages.Metric) bool {
	return s.byApplication[machineId].Add(timestamp, m, s.supplier)
}

func (s *Store) addBatch(
	machineId *scotty.Machine,
	timestamp float64,
	metricList trimessages.MetricList,
	filter func(*trimessages.Metric) bool) int {
	result := 0
	for _, metric := range metricList {
		if filter != nil && !filter(metric) {
			continue
		}
		if s.add(machineId, timestamp, metric) {
			result++
		}
	}
	gMetricValueCount.Add(int64(result))
	return result
}

func (s *Store) byNameAndMachine(
	name string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byApplication[machineId].ByName(name, start, end, result)
}

func (s *Store) byPrefixAndMachine(
	prefix string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byApplication[machineId].ByPrefix(prefix, start, end, result)
}

func (s *Store) byMachine(
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byApplication[machineId].ByPrefix("", start, end, result)
}

func (s *Store) latestByMachine(
	machineId *scotty.Machine,
	result Appender) {
	s.byApplication[machineId].Latest(result)
}

func (s *Store) visitAllMachines(v Visitor) (err error) {
	for machineId := range s.byApplication {
		if err = v.Visit(s, machineId); err != nil {
			return
		}
	}
	return
}

func (s *Store) registerMetrics() {
	if err := tricorder.RegisterMetric(
		"/store/pagesPerMetric",
		gPagesPerMetricDist,
		units.None,
		"Number of pages used per metric"); err != nil {
		panic(err)
	}
	if err := tricorder.RegisterMetric(
		"/store/latestTimeStampEvicted",
		&gLatestTimeEvicted,
		units.None,
		"Latest timestamp evicted from data store."); err != nil {
		panic(err)
	}
	if err := tricorder.RegisterMetric(
		"/store/availablePages",
		func() int {
			return len(s.supplier)
		},
		units.None,
		"Number of pages available to hold new metrics."); err != nil {
		panic(err)
	}
	if err := tricorder.RegisterMetric(
		"/store/metricValueCount",
		gMetricValueCount.Get,
		units.None,
		"Number of stored metrics values and timestamps"); err != nil {
		panic(err)
	}
}
