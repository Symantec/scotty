package store

import (
	"container/list"
	"github.com/Symantec/scotty/store/btreepq"
	"github.com/Symantec/tricorder/go/tricorder"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/google/btree"
	"math"
	"sort"
	"strings"
	"sync"
)

var (
	kPlusInf = math.Inf(1)
)

var (
	// 1 up to 1,000,000 bucketer
	gBucketer = tricorder.NewGeometricBucketer(1.0, 1e6)
)

type inactiveType int

var gInactive inactiveType

type doneAppenderType struct {
	Done    bool
	Wrapped Appender
}

func (d *doneAppenderType) Append(r *Record) bool {
	if !d.Wrapped.Append(r) {
		d.Done = true
		return false
	}
	return true
}

func (r *Record) setValue(value interface{}) {
	if value == gInactive {
		r.Active = false
		r.Value = r.Info.Kind().ZeroValue()
	} else {
		r.Active = true
		r.Value = value
	}
}

// TODO: Maybe make this part of tricorder?
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

func (p *pageType) Add(val tsValueType) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length] = val
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
	applicationId interface{},
	id *MetricInfo,
	start, end float64,
	result Appender) (keepGoing bool) {
	lastIdx := p.findGreaterOrEqual(end)
	if lastIdx == 0 {
		return true
	}
	firstIdx := p.findGreater(start) - 1
	if firstIdx < 0 {
		keepGoing = true
		firstIdx = 0
	}
	record := Record{
		EndpointId: applicationId,
		Info:       id}
	for i := lastIdx - 1; i >= firstIdx; i-- {
		record.TimeStamp = p[i].TimeStamp
		record.setValue(p[i].Value)
		if !result.Append(&record) {
			return false
		}
	}
	return
}

func (p pageType) findGreaterOrEqual(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp >= ts })
}

func (p pageType) findGreater(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp > ts })
}

type pageMetaDataType struct {
	seqNo uint64
	owner *timeSeriesType
}

func (m *pageMetaDataType) SetSeqNo(i uint64) {
	m.seqNo = i
}

func (m *pageMetaDataType) SeqNo() uint64 {
	return m.seqNo
}

type pageWithMetaDataType struct {
	// page queue lock protects this.
	pageMetaDataType
	// Lock of current page owner protects this.
	pageType
}

func (p *pageWithMetaDataType) Less(than btree.Item) bool {
	pthan := than.(*pageWithMetaDataType)
	return p.seqNo < pthan.seqNo
}

// Keeps track of next metric value to write to LMM
// Mutex of eclosing timeSeriesType protects.
type tsValuePtrType struct {
	// page with next value.
	// (pagePosit->next == nil && offset == len(*pagePosit)) ||
	// (pagePosit == nil && offset <= 0) means next value is not in a
	// page but is stored in the 'lastValue' field of the time series
	// instance.
	// (pagePosit->next == nil && offset > len(*pagePosit)) ||
	// (pagePosit == nil && offset > 0) means no next value.
	pagePosit *list.Element
	// Offset in page. < 0 means pages have to be skipped and first
	// available value is first value in *pagePosit.
	offset int
	// Points to lastValue field of enclosing time series.
	lastValue []tsValueType
}

// Advances this instance n values
func (p *tsValuePtrType) Advance(x int) {
	p.offset += x
}

// Tell this instance that first page is going away. Must be called before
// first is removed
func (p *tsValuePtrType) PopFirstPage(first *list.Element) {
	if first == p.pagePosit {
		page := p.pagePosit.Value.(*pageWithMetaDataType)
		p.offset -= len(page.pageType)
		p.pagePosit = p.pagePosit.Next()
	}
}

// Tell this instance that we are adding a new last page.
func (p *tsValuePtrType) AddLastPage(last *list.Element) {
	if p.pagePosit == nil {
		p.pagePosit = last
	}
}

func copyValues(page pageType) (result pageType) {
	result = make(pageType, len(page))
	copy(result, page)
	return
}

// Fetch some timestamp value pairs starting at where this instance points to.
// In case the value this instance points to was reclaimed, returns timestamp
// value pairs starting with the earliest available timestamp.
// In that case, it returns how many intermediate values must be skipped to
// get to the earliest value. In case this instance points past the
// latest value, returns (nil, 0)
func (p *tsValuePtrType) Values() (values []tsValueType, skipped int) {
	if p.pagePosit == nil {
		if p.offset <= 0 {
			return copyValues(p.lastValue), -p.offset
		}
		return
	}
	p.normalize()
	page := p.pagePosit.Value.(*pageWithMetaDataType)
	if p.offset > len(page.pageType) {
		return
	}
	if p.offset == len(page.pageType) {
		return copyValues(p.lastValue), 0
	}
	if p.offset < 0 {
		return copyValues(page.pageType), -p.offset
	}
	return copyValues(page.pageType[p.offset:]), 0
}

// Pre-conditions: p.pagePosit != nil
// Post-conditions: p.pagePosit != nil. Either p.offset < len(*p.pagePosit)
// or p.pagePosit->Next == nil.
func (p *tsValuePtrType) normalize() {
	page := p.pagePosit.Value.(*pageWithMetaDataType)
	for p.offset >= len(page.pageType) {
		next := p.pagePosit.Next()
		if next == nil {
			break
		}
		p.offset -= len(page.pageType)
		p.pagePosit = next
		page = p.pagePosit.Value.(*pageWithMetaDataType)
	}
}

// pageListType Represents a list of pages owned by the same time series.
// Page owners can change at any time. In particular they can change
// between when we get a time series' page list and when we change
// the priority of those pages. This abstraction makes it possible to
// ensure that we don't change the priority of a page that has been given
// a new owner.
type pageListType struct {
	Owner *timeSeriesType
	Pages []*pageWithMetaDataType
}

type timeSeriesType struct {
	id            *MetricInfo
	metrics       *storeMetricsType
	lock          sync.Mutex
	ptr           tsValuePtrType
	lastValue     [1]tsValueType
	leftToWrite   int
	pages         list.List
	nextPageToUse *pageWithMetaDataType
}

func newTimeSeriesType(
	id *MetricInfo,
	ts float64, value interface{},
	metrics *storeMetricsType) *timeSeriesType {
	result := &timeSeriesType{id: id, metrics: metrics}
	result.ptr.lastValue = result.lastValue[:]
	result.lastValue[0].TimeStamp = ts
	result.lastValue[0].Value = value
	result.leftToWrite = 1
	result.pages.Init()
	result.metrics.LeftToWritePerMetricDist.Add(
		float64(result.leftToWrite))
	result.metrics.PagesPerMetricDist.Add(0.0)
	return result
}

// AdvancePosition advances the position of the pstore iterator n values.
func (t *timeSeriesType) AdvancePosition(n int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.ptr.Advance(n)
	oldLeftToWrite := t.leftToWrite
	t.leftToWrite -= n
	t.metrics.LeftToWritePerMetricDist.Update(float64(
		oldLeftToWrite), float64(t.leftToWrite))
}

// FetchFromPosition fetches values starting at position of the pstore iterator.
// Returns values in order of timestamp. If values were skipped because they
// were too old and reclaimed, returns how many values were skipped.
func (t *timeSeriesType) FetchFromPosition() (values []tsValueType, skipped int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.ptr.Values()
}

// AcceptPage grants given page to this time series. The page queue calls
// this to bestow a page to a time series. This method must not be called
// directly.
func (t *timeSeriesType) AcceptPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.nextPageToUse != nil {
		panic("Oops, look like multiple goroutines are attempting to add pages to a time series.")
	}
	t.nextPageToUse = page
	page.pageType.Clear()
}

// GiveUpPage instructs this time series to give up the given page.
// Only the page queue calls this. This method must not be called directly.
// The page queue will always take pages away from a time series in the same
// order it bestowed them.
func (t *timeSeriesType) GiveUpPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// First see if this instance has the page but just isn't using it yet.
	// Since pages are always taken away in the same order they are
	// granted, this will only happen it this instance was just granted
	// its first page and isn't using it yet. Otherwise, the page being
	// given up will always be at the front of the linked list.
	if t.nextPageToUse == page {
		if t.pages.Len() != 0 {
			panic("Oops, timeSeries being asked to give up page it was just granted, but it already has older pages that it should give up first.")
		}
		t.nextPageToUse = nil
		return
	}
	oldLen := float64(t.pages.Len())
	front := t.pages.Front()
	if front == nil {
		panic("Oops, timeSeries being asked to give up a page when it has none.")
	}
	if front.Value.(*pageWithMetaDataType) != page {
		panic("Oops, timeSeries being asked to give up a page that is not the oldest page.")
	}
	t.ptr.PopFirstPage(front)
	t.pages.Remove(front)
	t.metrics.MetricValueCount.Add(int64(-len(page.pageType)))
	t.metrics.PagesPerMetricDist.Update(oldLen, oldLen-1.0)
}

func (t *timeSeriesType) PageList() pageListType {
	t.lock.Lock()
	defer t.lock.Unlock()
	var pages []*pageWithMetaDataType
	for e := t.pages.Front(); e != nil; e = e.Next() {
		pages = append(pages, e.Value.(*pageWithMetaDataType))
	}
	return pageListType{
		Owner: t,
		Pages: pages,
	}
}

func (t *timeSeriesType) needToAdd(timestamp float64, value interface{}) (
	needToAdd, needPage bool) {
	if value == t.lastValue[0].Value || (timestamp <= t.lastValue[0].TimeStamp && value != gInactive) {
		return
	}
	latestPage := t.latestPage()
	return true, latestPage == nil || latestPage.IsFull()
}

// Add adds a single value to this time series.
//
// If the value and timestamp should not be added because the value matches
// the last value in time series or timestamp does not come after last
// timestamp in the series, returns false, false, false.
//
// If the timestamp and value should be added, but cannot be added because
// there is no free space in a page, returns true, false, false.
// In this case, the caller should call GivePageTo on the page queue to
// bestow a new page for this time series and then try callng Add again.
//
// If the timestamp and value were added successfully, returns true, true, true
// If timeseries was previously inactive or true, true, false if timeseries
// was already active.
//
// A time series will never automatically request new pages internally to
// avoid deadlock. Doing so would require this time series to lock another
// time series while it itself is locked. At the same time, that time series
// may be blocked waiting to lock this time series.
func (t *timeSeriesType) Add(
	timestamp float64, value interface{}) (
	neededToAdd, addSuccessful, justActivated bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	needToAdd, needPage := t.needToAdd(timestamp, value)
	if !needToAdd {
		return
	}
	neededToAdd = true
	if !needPage && t.nextPageToUse != nil {
		panic("Oops, no new page needed but a next page to use is available. Perhaps multiple goroutines are adding to this same time series?")
	}
	// Another timeSeries took our page before we could use it.
	// Report add unsuccessful so that caller will request another page
	// for us.
	if needPage && t.nextPageToUse == nil {
		return
	}
	addSuccessful = true
	justActivated = t.lastValue[0].Value == gInactive

	var latestPage *pageType
	if t.nextPageToUse == nil {
		latestPage = t.latestPage()
	} else {
		oldLen := float64(t.pages.Len())
		t.ptr.AddLastPage(t.pages.PushBack(t.nextPageToUse))
		latestPage = &t.nextPageToUse.pageType
		t.nextPageToUse = nil
		t.metrics.PagesPerMetricDist.Update(oldLen, oldLen+1)
	}
	latestPage.Add(t.lastValue[0])
	// Ensure that timestamps are monotone increasing. We could get an
	// earlier timestamp if we are getting an inactive marker.
	if timestamp <= t.lastValue[0].TimeStamp {
		// If current timestamp is earlier, make it be 1ms more than
		// last timestamp. Adding 1ms makes the timestamp
		// greater for any timestamp < 2^43 seconds past Jan 1 1970
		// which is millinea into the future.
		t.lastValue[0].TimeStamp += 0.001
	} else {
		t.lastValue[0].TimeStamp = timestamp
	}
	t.lastValue[0].Value = value

	t.metrics.LeftToWritePerMetricDist.Update(
		float64(t.leftToWrite), float64(t.leftToWrite+1))
	t.leftToWrite++
	return
}

// Fetch fetches all the values and timestamps such that it can be known
// what the value of the metric is between start inclusive and end exclusive.
// This means that it may return a value with a timestamp just before start.
func (t *timeSeriesType) Fetch(
	applicationId interface{},
	start, end float64,
	result Appender) {
	t.lock.Lock()
	defer t.lock.Unlock()
	// Only include latest value if it comes before end timestamp.
	if t.lastValue[0].TimeStamp < end {
		record := Record{
			EndpointId: applicationId,
			Info:       t.id,
			TimeStamp:  t.lastValue[0].TimeStamp,
		}
		record.setValue(t.lastValue[0].Value)
		if !result.Append(&record) {
			return
		}
	}
	// If latest value has timestamp on or before start we are done.
	if t.lastValue[0].TimeStamp <= start {
		return
	}
	var e *list.Element
	for e = t.pages.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*pageWithMetaDataType)
		if !page.pageType.Fetch(applicationId, t.id, start, end, result) {
			break
		}
	}
}

// May return nil if time series has no pages
func (t *timeSeriesType) latestPage() (result *pageType) {
	back := t.pages.Back()
	if back == nil {
		return
	}
	return &back.Value.(*pageWithMetaDataType).pageType
}

func addToTimeSeries(
	timeSeries *timeSeriesType,
	timestamp float64,
	value interface{},
	supplier *pageQueueType) (
	neededToAdd, justActivated bool) {
	neededToAdd, addSuccessful, justActivated := timeSeries.Add(
		timestamp, value)
	// If add fails because of no free space, keep trying Add
	// in a loop until it succeeds.
	for neededToAdd && !addSuccessful {
		supplier.GivePageTo(timeSeries)
		neededToAdd, addSuccessful, justActivated = timeSeries.Add(
			timestamp, value)
	}
	return
}

func inactivateTimeSeries(
	timeSeries *timeSeriesType,
	timestamp float64,
	supplier *pageQueueType) (justInactivated bool) {
	justInactivated, _ = addToTimeSeries(timeSeries, timestamp, gInactive, supplier)
	return
}

type timeSeriesCollectionType struct {
	applicationId interface{}
	metrics       *storeMetricsType
	// A function must hold this lock when changing the status of
	// any time series in this instance or when adding a new time series
	// to ensure that when it returns, the status of each time series
	// is consistent with the status of its corresponding pages.
	// If this lock is acquired, it must be acquired before the normal
	// lock of this instance.
	statusChangeLock sync.Mutex
	// Normal lock of this instance.
	lock            sync.Mutex
	timeSeries      map[*MetricInfo]*timeSeriesType
	metricInfoStore metricInfoStoreType
	active          bool
}

func newTimeSeriesCollectionType(
	app interface{},
	metrics *storeMetricsType) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId: app,
		metrics:       metrics,
		timeSeries:    make(map[*MetricInfo]*timeSeriesType),
		active:        true,
	}
	result.metricInfoStore.Init()
	return result
}

func (c *timeSeriesCollectionType) Iterators() (result []*Iterator) {
	c.lock.Lock()
	defer c.lock.Unlock()
	result = make([]*Iterator, len(c.timeSeries))
	idx := 0
	for _, ts := range c.timeSeries {
		result[idx] = newIterator(ts)
		idx++
	}
	return
}

func (c *timeSeriesCollectionType) tsAll() (
	result []*timeSeriesType) {
	for _, ts := range c.timeSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) TsAllMarkingInactive() (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.active = false
	return c.tsAll()
}

func (c *timeSeriesCollectionType) TsByName(name string) (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	infoList := c.metricInfoStore.ByName[name]
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

// LookkupBatch looks up all the metrics in one go and returns the
// following:
// fetched: timeSeries already in this collection keyed by Metric.
//  values must be added to these manually.
// newOnes: timeSeries just added as a result of this lookup. Since these
// are new, the first value added automatically.
// notFetched: timeSeries in this collection but not fetched. These
// are the time series that should be marked inactive.
func (c *timeSeriesCollectionType) LookupBatch(
	timestamp float64, metrics trimessages.MetricList) (
	fetched map[*timeSeriesType]interface{},
	newOnes, notFetched []*timeSeriesType, ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.active {
		return
	}
	ok = true
	valueByMetric := make(map[*MetricInfo]interface{})
	fetched = make(map[*timeSeriesType]interface{})
	for i := range metrics {
		// TODO: Allow distribution metrics later.
		if metrics[i].Kind == types.Dist {
			continue
		}
		id := c.metricInfoStore.Register(metrics[i])
		valueByMetric[id] = metrics[i].Value
	}
	// populate notFetched
	for id, series := range c.timeSeries {
		if _, ok := valueByMetric[id]; !ok {
			notFetched = append(notFetched, series)
		}
	}
	for id, value := range valueByMetric {
		if c.timeSeries[id] == nil {
			c.timeSeries[id] = newTimeSeriesType(
				id, timestamp, value, c.metrics)
			newOnes = append(newOnes, c.timeSeries[id])
		} else {
			fetched[c.timeSeries[id]] = value
		}
	}
	return
}

func (c *timeSeriesCollectionType) MarkActive() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.active = true
}

func (c *timeSeriesCollectionType) MarkInactive(
	timestamp float64, supplier *pageQueueType) {
	var reclaimHighList []pageListType
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	tslist := c.TsAllMarkingInactive()
	var inactiveCount int
	for i := range tslist {
		// If it was active
		if inactivateTimeSeries(tslist[i], timestamp, supplier) {
			reclaimHighList = append(
				reclaimHighList,
				tslist[i].PageList())
			inactiveCount++
		}
	}
	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	c.metrics.MetricValueCount.Add(int64(inactiveCount))

	return
}

func (c *timeSeriesCollectionType) AddBatch(
	timestamp float64,
	metrics trimessages.MetricList,
	supplier *pageQueueType) (result int, ok bool) {
	var reclaimLowList, reclaimHighList []pageListType
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	fetched, newOnes, notFetched, ok := c.LookupBatch(timestamp, metrics)
	if !ok {
		return
	}
	addedCount := len(newOnes)
	// For each metric in fetched, manually add its value to its
	// time series
	for timeSeries, value := range fetched {
		needToAdd, justActivated := addToTimeSeries(
			timeSeries, timestamp, value, supplier)
		if needToAdd {
			addedCount++
		}
		// If status went from inactive to active.
		if justActivated {
			reclaimLowList = append(
				reclaimLowList,
				timeSeries.PageList())
		}
	}
	var inactiveCount int
	for i := range notFetched {
		if inactivateTimeSeries(
			notFetched[i], timestamp, supplier) {
			reclaimHighList = append(
				reclaimHighList,
				notFetched[i].PageList())
			inactiveCount++
		}
	}
	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	// recliamLowList has all the page lists that should be reclaimed with
	// low priority
	supplier.ReclaimLow(reclaimLowList)

	result = inactiveCount + addedCount
	c.metrics.MetricValueCount.Add(int64(result))
	return
}

func (c *timeSeriesCollectionType) ByName(
	name string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByName(name) {
		timeSeries.Fetch(c.applicationId, start, end, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

func (c *timeSeriesCollectionType) ByPrefix(
	prefix string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByPrefix(prefix) {
		timeSeries.Fetch(c.applicationId, start, end, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

func (c *timeSeriesCollectionType) Latest(result Appender) {
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByPrefix("") {
		timeSeries.Fetch(
			c.applicationId, kPlusInf, kPlusInf, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

type pageQueueType struct {
	valueCountPerPage  int
	pageCount          int
	inactiveThreshhold float64
	degree             int
	lock               sync.Mutex
	pq                 *btreepq.PageQueue
}

func newPageQueueType(
	valueCountPerPage int,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *pageQueueType {
	pages := btreepq.New(
		pageCount,
		int(float64(pageCount)*inactiveThreshhold),
		degree,
		func() btreepq.Page {
			pp := make(pageType, 0, valueCountPerPage)
			return &pageWithMetaDataType{pageType: pp}
		})
	return &pageQueueType{
		valueCountPerPage:  valueCountPerPage,
		pageCount:          pageCount,
		inactiveThreshhold: inactiveThreshhold,
		degree:             degree,
		pq:                 pages}
}

func (s *pageQueueType) MaxValuesPerPage() int {
	return s.valueCountPerPage
}

func (s *pageQueueType) PageQueueStats(stats *btreepq.PageQueueStats) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pq.Stats(stats)
}

func (s *pageQueueType) RegisterMetrics() (err error) {
	var queueStats btreepq.PageQueueStats
	queueRegion := tricorder.RegisterRegion(func() {
		s.PageQueueStats(&queueStats)
	})
	if err = tricorder.RegisterMetricInRegion(
		"/store/highPriorityCount",
		&queueStats.HighPriorityCount,
		queueRegion,
		units.None,
		"Number of pages in high priority queue"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"/store/lowPriorityCount",
		&queueStats.LowPriorityCount,
		queueRegion,
		units.None,
		"Number of pages in low priority queue"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"/store/nextLowPrioritySeqNo",
		&queueStats.NextLowPrioritySeqNo,
		queueRegion,
		units.None,
		"Next seq no in low priority queue, 0 if empty"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"/store/nextHighPrioritySeqNo",
		&queueStats.NextHighPrioritySeqNo,
		queueRegion,
		units.None,
		"Next seq no in high priority queue, 0 if empty"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"/store/endSeqNo",
		&queueStats.EndSeqNo,
		queueRegion,
		units.None,
		"All seq no smaller than this. Marks end of both queues."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"/store/highPriorityRatio",
		queueStats.HighPriorityRatio,
		queueRegion,
		units.None,
		"High priority page ratio"); err != nil {
		return
	}

	if err = tricorder.RegisterMetric(
		"/store/totalPages",
		&s.pageCount,
		units.None,
		"Total number of pages."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/maxValuesPerPage",
		&s.valueCountPerPage,
		units.None,
		"Maximum number ofvalues that can fit in a page."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/inactiveThreshhold",
		&s.inactiveThreshhold,
		units.None,
		"The ratio of inactive pages needed before they are reclaimed first"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/btreeDegree",
		&s.degree,
		units.None,
		"The degree of the btrees in the queue"); err != nil {
		return
	}
	return
}

// GivePageTo bestows a new page on t.
// This call may lock another timeSeriesType instance. To avoid deadlock,
// caller must not hold a lock on any timeSeriesType instance.
func (s *pageQueueType) GivePageTo(t *timeSeriesType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	result := s.pq.NextPage().(*pageWithMetaDataType)
	if result.owner != nil {
		result.owner.GiveUpPage(result)
	}
	result.owner = t
	result.owner.AcceptPage(result)
}

func (s *pageQueueType) ReclaimHigh(
	reclaimHighList []pageListType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, pageList := range reclaimHighList {
		for i := range pageList.Pages {
			if pageList.Pages[i].owner == pageList.Owner {
				s.pq.ReclaimHigh(pageList.Pages[i])
			}
		}
	}
}

func (s *pageQueueType) ReclaimLow(
	reclaimLowList []pageListType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, pageList := range reclaimLowList {
		for i := range pageList.Pages {
			if pageList.Pages[i].owner == pageList.Owner {
				s.pq.ReclaimLow(pageList.Pages[i])
			}
		}
	}
}

type recordListType []*Record

func (l *recordListType) Append(r *Record) bool {
	recordCopy := *r
	*l = append(*l, &recordCopy)
	return true
}

type storeMetricsType struct {
	PagesPerMetricDist       *tricorder.NonCumulativeDistribution
	LeftToWritePerMetricDist *tricorder.NonCumulativeDistribution
	MetricValueCount         *countType
}

func newStoreMetricsType() *storeMetricsType {
	return &storeMetricsType{
		PagesPerMetricDist:       gBucketer.NewNonCumulativeDistribution(),
		LeftToWritePerMetricDist: gBucketer.NewNonCumulativeDistribution(),
		MetricValueCount:         &countType{},
	}
}

func (s *Store) registerEndpoint(endpointId interface{}) {

	if s.byApplication[endpointId] != nil {
		return
	}
	s.byApplication[endpointId] = newTimeSeriesCollectionType(
		endpointId, s.metrics)
}

func (s *Store) shallowCopy() *Store {
	byApplicationCopy := make(
		map[interface{}]*timeSeriesCollectionType,
		len(s.byApplication))
	for k, v := range s.byApplication {
		byApplicationCopy[k] = v
	}
	return &Store{
		byApplication: byApplicationCopy,
		supplier:      s.supplier,
		metrics:       s.metrics,
	}
}

func (s *Store) addBatch(
	endpointId interface{},
	timestamp float64,
	mlist trimessages.MetricList) (int, bool) {
	return s.byApplication[endpointId].AddBatch(
		timestamp, mlist, s.supplier)
}

func newIterator(ts *timeSeriesType) *Iterator {
	result := &Iterator{timeSeries: ts}
	result.values, result.skipped = result.timeSeries.FetchFromPosition()
	return result
}

func (i *Iterator) next() (timestamp float64, value interface{}, skipped int) {
	if len(i.values) > 0 {
		timestamp = i.values[0].TimeStamp
		value = i.values[0].Value
		skipped = i.skipped
		i.values = i.values[1:]
		i.advances += (i.skipped + 1)
		i.skipped = 0
	}
	if value == gInactive {
		value = i.Info().Kind().ZeroValue()
	}
	return
}

func (i *Iterator) commit() {
	if i.advances > 0 {
		i.timeSeries.AdvancePosition(i.advances)
		i.advances = 0
	}
}

func (s *Store) iterators(endpointId interface{}) []*Iterator {
	return s.byApplication[endpointId].Iterators()
}

func (s *Store) byNameAndEndpoint(
	name string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByName(name, start, end, result)
}

func (s *Store) byPrefixAndEndpoint(
	prefix string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix(prefix, start, end, result)
}

func (s *Store) byEndpoint(
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix("", start, end, result)
}

func (s *Store) markEndpointInactive(
	timestamp float64, endpointId interface{}) {
	s.byApplication[endpointId].MarkInactive(timestamp, s.supplier)
}

func (s *Store) markEndpointActive(endpointId interface{}) {
	s.byApplication[endpointId].MarkActive()
}

func (s *Store) latestByEndpoint(
	endpointId interface{},
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
	if err = s.supplier.RegisterMetrics(); err != nil {
		return
	}
	// Allow this store instance to be GCed
	maxValuesPerPage := s.supplier.MaxValuesPerPage()
	metrics := s.metrics

	if err = tricorder.RegisterMetric(
		"/store/pagesPerMetric",
		metrics.PagesPerMetricDist,
		units.None,
		"Number of pages used per metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/leftToWritePerMetric",
		metrics.LeftToWritePerMetricDist,
		units.None,
		"Number of values to write to persistent store per metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/pageUtilization",
		func() float64 {
			metricValueCount := metrics.MetricValueCount.Get()
			pagesInUseCount := metrics.PagesPerMetricDist.Sum()
			metricCount := metrics.PagesPerMetricDist.Count()
			extraValueCount := uint64(metricValueCount) - metricCount
			return float64(extraValueCount) / pagesInUseCount / float64(maxValuesPerPage)
		},
		units.None,
		"Page utilization 0.0 - 1.0"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"/store/metricValueCount",
		metrics.MetricValueCount.Get,
		units.None,
		"Number of stored metrics values and timestamps"); err != nil {
		return
	}
	return
}
