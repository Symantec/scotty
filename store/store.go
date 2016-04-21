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
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	kPlusInf = math.Inf(1)
)

var (
	// 1 up to 1,000,000 bucketer
	gBucketer       = tricorder.NewGeometricBucketer(1.0, 1e6)
	gTsAndValueSize = tsAndValueSize()
)

type inactiveType int

var gInactive inactiveType

func (r *Record) setValue(value interface{}) {
	if value == gInactive {
		r.Active = false
		r.Value = r.Info.Kind().ZeroValue()
	} else {
		r.Active = true
		r.Value = value
	}
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
		bits:        metric.Bits,
		groupId:     metric.GroupId}
	result, alreadyExists := m.ByInfo[infoStruct]
	if alreadyExists {
		return
	}
	result = &infoStruct
	m.ByInfo[infoStruct] = result
	m.ByName[infoStruct.path] = append(m.ByName[infoStruct.path], result)
	return
}

// a single page of timestamps
type tsPageType []float64

func (p tsPageType) IsFull() bool {
	return len(p) == cap(p)
}

func (p *tsPageType) Clear() {
	*p = (*p)[:0]
}

func (p tsPageType) Len() int {
	return len(p)
}

func (p *tsPageType) Add(ts float64) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length] = ts
}

func (p tsPageType) StoreIndexToRecord(idx int, record *Record) {
	record.TimeStamp = p[idx]
}

func (p tsPageType) FindGreaterOrEqual(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx] >= ts })
}

func (p tsPageType) FindGreater(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx] > ts })
}

type tsValueType struct {
	TimeStamp float64
	Value     interface{}
}

// single page of timestamps with values
type pageType []tsValueType

func (p pageType) Len() int {
	return len(p)
}

func (p *pageType) Add(val tsValueType) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length] = val
}

func (p *pageType) Clear() {
	*p = (*p)[:0]
}

func (p pageType) IsFull() bool {
	return len(p) == cap(p)
}

func (p pageType) StoreIndexToRecord(idx int, record *Record) {
	record.TimeStamp = p[idx].TimeStamp
	record.setValue(p[idx].Value)
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

type pageMetaDataType struct {
	seqNo uint64
	owner pageOwnerType
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
	// Lock of current page owner protects these.
	raw        []byte
	values     pageType
	timestamps tsPageType
}

func newPageWithMetaDataType(bytesPerPage int) *pageWithMetaDataType {
	raw := make([]byte, bytesPerPage)
	values, timestamps := makeUnionSlice(raw)
	return &pageWithMetaDataType{
		raw: raw, values: values, timestamps: timestamps}
}

func (p *pageWithMetaDataType) Values() *pageType {
	return &p.values
}

func (p *pageWithMetaDataType) Times() *tsPageType {
	return &p.timestamps
}

func (p *pageWithMetaDataType) ValuePage() basicPageType {
	return p.Values()
}

func (p *pageWithMetaDataType) TimePage() basicPageType {
	return p.Times()
}

func (p *pageWithMetaDataType) Less(than btree.Item) bool {
	pthan := than.(*pageWithMetaDataType)
	return p.seqNo < pthan.seqNo
}

// pageOwnerType is the interface for any data structure that can own
// pages.
type pageOwnerType interface {
	// GiveUpPage instructs this instance to give up given page
	GiveUpPage(page *pageWithMetaDataType)
	// AcceptPage instructs this instance to accept given page
	AcceptPage(page *pageWithMetaDataType)
}

// pageListType Represents a list of pages owned by the same time series.
// Page owners can change at any time. In particular they can change
// between when we get a time series' page list and when we change
// the priority of those pages. This abstraction makes it possible to
// ensure that we don't change the priority of a page that has been given
// a new owner.
type pageListType struct {
	Owner pageOwnerType
	Pages []*pageWithMetaDataType
}

// basicPageType is the interface that all page data must implement
type basicPageType interface {
	Clear()
	IsFull() bool
	FindGreaterOrEqual(ts float64) int
	FindGreater(ts float64) int
	Len() int
	StoreIndexToRecord(idx int, record *Record)
}

func Fetch(
	p basicPageType,
	start, end float64,
	record *Record,
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
	for i := lastIdx - 1; i >= firstIdx; i-- {
		p.StoreIndexToRecord(i, record)
		if !result.Append(record) {
			return false
		}
	}
	return
}

func FetchForward(
	p basicPageType,
	start, end float64,
	record *Record,
	result Appender) (keepGoing bool) {
	firstIdx := p.FindGreater(start) - 1
	if firstIdx < 0 {
		firstIdx = 0
	}
	lastIdx := p.FindGreaterOrEqual(end)
	if lastIdx == p.Len() {
		keepGoing = true
	}
	for i := firstIdx; i < lastIdx; i++ {
		p.StoreIndexToRecord(i, record)
		if !result.Append(record) {
			return false
		}
	}
	return
}

// pageSeriesType is a collection of pages designed to be included in
// other data structures.
type pageSeriesType struct {
	pages         list.List
	nextPageToUse *pageWithMetaDataType
	toData        func(*pageWithMetaDataType) basicPageType
}

// Init initializes this instance. toData gets the page data out of a page
func (p *pageSeriesType) Init(
	toData func(*pageWithMetaDataType) basicPageType) {
	p.pages.Init()
	p.toData = toData
}

// Len returns the number of pages
func (p *pageSeriesType) Len() int {
	return p.pages.Len()
}

// Back returns the linked list node having the las page.
func (p *pageSeriesType) Back() *list.Element {
	return p.pages.Back()
}

func (p *pageSeriesType) Fetch(
	start, end float64,
	record *Record,
	result Appender) {
	for e := p.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*pageWithMetaDataType)
		if !Fetch(
			p.toData(page),
			start, end,
			record,
			result) {
			break
		}
	}
}

func (p *pageSeriesType) FetchForward(
	start, end float64,
	record *Record,
	result Appender) (keepGoing bool) {
	var startPos *list.Element
	for e := p.Back(); e != nil; e = e.Prev() {
		page := e.Value.(*pageWithMetaDataType)
		startPos = e
		if p.toData(page).FindGreater(start) > 0 {
			break
		}
	}
	doneAppender := &doneAppenderType{Wrapped: result}
	for e := startPos; e != nil; e = e.Next() {
		page := e.Value.(*pageWithMetaDataType)
		if !FetchForward(
			p.toData(page),
			start, end,
			record,
			doneAppender) {
			break
		}
	}
	return !doneAppender.Done
}

// LastPageMustHaveSpace returns the last page with space available or nil
// If a new page must be granted. newPage is true if returned page was just
// added.
func (p *pageSeriesType) LastPageMustHaveSpace(
	newNodeCallback func(*list.Element)) (
	lastPage *pageWithMetaDataType, newPage bool) {
	needNewPage := p.needPage()
	if !needNewPage && p.nextPageToUse != nil {
		panic("Oops, no new page needed but a next page to use is available. Perhaps multiple goroutines are adding to this same series?")
	}
	// Another series took our page before we could use it.
	// Report unsuccessful
	if needNewPage && p.nextPageToUse == nil {
		return
	}
	if p.nextPageToUse == nil {
		return p.lastPage(), false
	}
	newNode := p.pages.PushBack(p.nextPageToUse)
	if newNodeCallback != nil {
		newNodeCallback(newNode)
	}
	result := p.nextPageToUse
	p.nextPageToUse = nil
	return result, true
}

func (p *pageSeriesType) PageList() (pages []*pageWithMetaDataType) {
	for e := p.pages.Front(); e != nil; e = e.Next() {
		pages = append(pages, e.Value.(*pageWithMetaDataType))
	}
	return
}

// AcceptPage accepts given page.
func (p *pageSeriesType) AcceptPage(page *pageWithMetaDataType) {
	if p.nextPageToUse != nil {
		panic("Oops, look like multiple goroutines are attempting to add pages to this series.")
	}
	p.nextPageToUse = page
	p.toData(page).Clear()
}

// GiveUpPage gives up given page. If non-nil, GiveUpPage calls
// nodeGoneCallback with the linked list node containing the page being given
// up. GiveUpPage returns true if a page was given up from list or false
// otherwise.
func (p *pageSeriesType) GiveUpPage(
	page *pageWithMetaDataType,
	nodeGoneCallback func(*list.Element)) bool {

	// First see if this instance has the page but just isn't using it yet.
	// Since pages are always taken away in the same order they are
	// granted, this will only happen it this instance was just granted
	// its first page and isn't using it yet. Otherwise, the page being
	// given up will always be at the front of the linked list.
	if p.nextPageToUse == page {
		if p.pages.Len() != 0 {
			panic("Oops, series being asked to give up page it was just granted, but it already has older pages that it should give up first.")
		}
		p.nextPageToUse = nil
		return false
	}
	front := p.pages.Front()
	if front == nil {
		panic("Oops, series being asked to give up a page when it has none.")
	}
	if front.Value.(*pageWithMetaDataType) != page {
		panic("Oops, series being asked to give up a page that is not the oldest page.")
	}
	if nodeGoneCallback != nil {
		nodeGoneCallback(front)
	}
	p.pages.Remove(front)
	return true
}

func (p *pageSeriesType) lastPage() (result *pageWithMetaDataType) {
	back := p.pages.Back()
	if back == nil {
		return
	}
	return back.Value.(*pageWithMetaDataType)
}

func (p *pageSeriesType) needPage() bool {
	page := p.lastPage()
	return page == nil || p.toData(page).IsFull()
}

func incTs(x float64) float64 {
	return x + 0.001
}

type timestampSeriesType struct {
	groupId int
	metrics *storeMetricsType
	lock    sync.Mutex
	lastTs  float64
	pages   pageSeriesType
	active  bool
}

func newTimeStampSeriesType(
	groupId int,
	ts float64,
	metrics *storeMetricsType) *timestampSeriesType {
	result := &timestampSeriesType{
		groupId: groupId,
		metrics: metrics,
		lastTs:  ts,
		active:  true}
	result.pages.Init((*pageWithMetaDataType).TimePage)
	result.metrics.NewTimeStampSeries()
	return result
}

func (t *timestampSeriesType) GroupId() int {
	return t.groupId
}

func (t *timestampSeriesType) PageList() pageListType {
	t.lock.Lock()
	defer t.lock.Unlock()
	return pageListType{
		Owner: t,
		Pages: t.pages.PageList(),
	}
}

func (t *timestampSeriesType) AddDryRun(ts float64) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return ts > t.lastTs
}

func (t *timestampSeriesType) Add(ts float64) (
	neededToAdd, addSuccessful, justActivated bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if ts <= t.lastTs {
		return
	}
	neededToAdd = true
	lastPage, isNew := t.pages.LastPageMustHaveSpace(nil)
	if lastPage == nil {
		return
	}
	addSuccessful = true
	justActivated = !t.active
	t.active = true
	if isNew {
		t.metrics.AddEmptyTimeStampPage()
	}
	latestPage := lastPage.Times()
	latestPage.Add(t.lastTs)
	t.lastTs = ts
	return
}

func (t *timestampSeriesType) InactivateDryRun() (
	needed bool, actualTs float64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.active {
		return
	}
	return true, incTs(t.lastTs)
}

func (t *timestampSeriesType) Inactivate() (
	neededToAdd, addSuccessful bool, actualTs float64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.active {
		return
	}
	neededToAdd = true
	lastPage, _ := t.pages.LastPageMustHaveSpace(nil)
	if lastPage == nil {
		return
	}
	addSuccessful = true
	t.active = false
	latestPage := lastPage.Times()
	latestPage.Add(t.lastTs)
	t.lastTs = incTs(t.lastTs)
	actualTs = t.lastTs
	return
}

func (t *timestampSeriesType) GiveUpPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.pages.GiveUpPage(page, nil) {
		t.metrics.RemoveTimeStampPage()
	}
}

func (t *timestampSeriesType) AcceptPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.pages.AcceptPage(page)
}

func (t *timestampSeriesType) FindAfter(
	start float64, maxCount int) (result []float64) {
	var record Record
	t.lock.Lock()
	defer t.lock.Unlock()
	appender := AppenderFilterFunc(
		AppenderLimit(
			(*tsAppenderType)(&result),
			maxCount,
		),
		func(r *Record) bool {
			return r.TimeStamp > start
		},
	)

	if !t.pages.FetchForward(start, kPlusInf, &record, appender) {
		return
	}
	record.TimeStamp = t.lastTs
	appender.Append(&record)
	return
}

type timeSeriesType struct {
	id        *MetricInfo
	metrics   *storeMetricsType
	lock      sync.Mutex
	lastValue [1]tsValueType
	pages     pageSeriesType
}

func newTimeSeriesType(
	id *MetricInfo,
	ts float64, value interface{},
	metrics *storeMetricsType) *timeSeriesType {
	result := &timeSeriesType{id: id, metrics: metrics}
	result.lastValue[0].TimeStamp = ts
	result.lastValue[0].Value = value
	result.pages.Init((*pageWithMetaDataType).ValuePage)
	result.metrics.NewValueSeries()
	return result
}

func (t *timeSeriesType) GroupId() int {
	return t.id.GroupId()
}

// AcceptPage grants given page to this time series. The page queue calls
// this to bestow a page to a time series. This method must not be called
// directly.
func (t *timeSeriesType) AcceptPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.pages.AcceptPage(page)
}

// GiveUpPage instructs this time series to give up the given page.
// Only the page queue calls this. This method must not be called directly.
// The page queue will always take pages away from a time series in the same
// order it bestowed them.
func (t *timeSeriesType) GiveUpPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldLen := t.pages.Len()
	if t.pages.GiveUpPage(page, nil) {
		t.metrics.RemoveValuePage(oldLen, page.Values().Len())
	}
}

func (t *timeSeriesType) PageList() pageListType {
	t.lock.Lock()
	defer t.lock.Unlock()
	return pageListType{
		Owner: t,
		Pages: t.pages.PageList(),
	}
}

func (t *timeSeriesType) needToAdd(timestamp float64, value interface{}) (
	needToAdd bool) {
	if value == t.lastValue[0].Value || (timestamp <= t.lastValue[0].TimeStamp && value != gInactive) {
		return false
	}
	return true
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
	needToAdd := t.needToAdd(timestamp, value)
	if !needToAdd {
		return
	}
	neededToAdd = true
	oldLen := t.pages.Len()
	lastPage, newPage := t.pages.LastPageMustHaveSpace(nil)
	if lastPage == nil {
		return
	}
	addSuccessful = true
	justActivated = t.lastValue[0].Value == gInactive
	if newPage {
		t.metrics.AddEmptyValuePage(oldLen)
	}
	latestPage := lastPage.Values()
	latestPage.Add(t.lastValue[0])
	// Ensure that timestamps are monotone increasing. We could get an
	// earlier timestamp if we are getting an inactive marker.
	if timestamp <= t.lastValue[0].TimeStamp {
		// If current timestamp is earlier, make it be 1ms more than
		// last timestamp. Adding 1ms makes the timestamp
		// greater for any timestamp < 2^43 seconds past Jan 1 1970
		// which is millinea into the future.
		t.lastValue[0].TimeStamp = incTs(t.lastValue[0].TimeStamp)
	} else {
		t.lastValue[0].TimeStamp = timestamp
	}
	t.lastValue[0].Value = value
	return
}

// Fetch fetches all the values and timestamps such that it can be known
// what the value of the metric is between start inclusive and end exclusive.
// This means that it may return a value with a timestamp just before start.
func (t *timeSeriesType) Fetch(
	applicationId interface{},
	start, end float64,
	result Appender) {
	record := Record{
		EndpointId: applicationId,
		Info:       t.id}
	t.lock.Lock()
	defer t.lock.Unlock()
	// Only include latest value if it comes before end timestamp.
	if t.lastValue[0].TimeStamp < end {
		record.TimeStamp = t.lastValue[0].TimeStamp
		record.setValue(t.lastValue[0].Value)
		if !result.Append(&record) {
			return
		}
	}
	if t.lastValue[0].TimeStamp > start {
		t.pages.Fetch(start, end, &record, result)
	}
}

func (t *timeSeriesType) FetchForward(
	applicationId interface{},
	start, end float64,
	result Appender) {
	record := Record{
		EndpointId: applicationId,
		Info:       t.id}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.lastValue[0].TimeStamp > start {
		// We have to look at the pages
		if !t.pages.FetchForward(start, end, &record, result) {
			return
		}
	}
	// Only include latest value if it comes before end timestamp.
	if t.lastValue[0].TimeStamp < end {
		record.TimeStamp = t.lastValue[0].TimeStamp
		record.setValue(t.lastValue[0].Value)
		result.Append(&record)
	}
}

// FetchForwardWithTimestamps fetches a value-timestamp pair for each timestamp
// in ascending order by timestamp.
// If a timestamp falls between two unique values in this instance,
// FetchForwardWithTimestamps uses the the value with the timestamp
// immediately before.
// If a timestamp is before the earliest value in this instance,
// FetchForwardWithTimestamps ignores it.
// timestamps are seconds after Jan 1, 1970 GMT and must be in ascending order.
func (t *timeSeriesType) FetchForwardWithTimeStamps(
	applicationId interface{},
	timestamps []float64,
	result Appender) {
	timestampLen := len(timestamps)
	if timestampLen == 0 {
		return
	}
	appender := newMergeWithTimestamps(timestamps, result)
	t.FetchForward(
		applicationId,
		timestamps[0],
		// We have to at least get the last timestamp. If we
		// pull anything extra between lastTimeStamp and
		// lastTimeStamp + 1.0 it won't match up with any timestamp
		// and just get ignored.
		timestamps[timestampLen-1]+1.0,
		appender)
	appender.Finalize()
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

func addToTimeStampSeries(
	timestampSeries *timestampSeriesType,
	timestamp float64,
	supplier *pageQueueType) (
	neededToAdd, justActivated bool) {
	neededToAdd, addSuccessful, justActivated := timestampSeries.Add(timestamp)
	for neededToAdd && !addSuccessful {
		supplier.GivePageTo(timestampSeries)
		neededToAdd, addSuccessful, justActivated = timestampSeries.Add(timestamp)
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

func inactivateTimeStampSeries(
	timestampSeries *timestampSeriesType,
	supplier *pageQueueType) (justInactivated bool, actualTs float64) {
	neededToAdd, addSuccessful, actualTs := timestampSeries.Inactivate()
	for neededToAdd && !addSuccessful {
		supplier.GivePageTo(timestampSeries)
		neededToAdd, addSuccessful, actualTs = timestampSeries.Inactivate()
	}
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
	timestampSeries map[int]*timestampSeriesType
	metricInfoStore metricInfoStoreType
	active          bool
	iterators       map[string]*namedIteratorDataType
}

func newTimeSeriesCollectionType(
	app interface{},
	metrics *storeMetricsType) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId:   app,
		metrics:         metrics,
		timeSeries:      make(map[*MetricInfo]*timeSeriesType),
		timestampSeries: make(map[int]*timestampSeriesType),
		active:          true,
		iterators:       make(map[string]*namedIteratorDataType),
	}
	result.metricInfoStore.Init()
	return result
}

func (c *timeSeriesCollectionType) NewNamedIterator(
	name string, maxFrames int) NamedIterator {
	var startTimes map[int]float64
	var completed map[*timeSeriesType]float64
	c.lock.Lock()
	defer c.lock.Unlock()
	snapshot := c.iterators[name]
	if snapshot != nil {
		startTimes = snapshot.startTimeStamps
		completed = snapshot.completed
	}
	timesByGroup := make(map[int][]float64, len(c.timestampSeries))
	for groupId, series := range c.timestampSeries {
		timesByGroup[groupId] = series.FindAfter(
			startTimes[groupId], maxFrames)
	}
	return &namedIteratorType{
		name:                 name,
		timeSeriesCollection: c,
		startTimeStamps:      startTimes,
		timestamps:           timesByGroup,
		completed:            copyCompleted(completed),
		timeSeries:           c.tsAll(),
	}
}

func (c *timeSeriesCollectionType) saveProgress(
	name string, progress *namedIteratorDataType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.iterators[name] = progress
}

func (c *timeSeriesCollectionType) tsAll() (
	result []*timeSeriesType) {
	for _, ts := range c.timeSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) tsAllTimeStamps() (
	result []*timestampSeriesType) {
	for _, ts := range c.timestampSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) TsAllAndTimeStampsMarkingInactive() (
	[]*timeSeriesType, []*timestampSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.active = false
	return c.tsAll(), c.tsAllTimeStamps()
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

// LookupBatch looks up all the metrics in one go and returns the
// following:
// fetched: timeSeries already in this collection keyed by Metric.
//  values must be added to these manually.
// newOnes: timeSeries just added as a result of this lookup. Since these
// are new, the first value added automatically.
// notFetched: timeSeries in this collection but not fetched. These
// are the time series that should be marked inactive.
// ok is true if metrics can be added to this instance or false if this
// instance is inactive and closed for new metrics.
func (c *timeSeriesCollectionType) LookupBatch(
	timestamp float64, metrics trimessages.MetricList) (
	fetched map[*timeSeriesType]interface{},
	newOnes, notFetched []*timeSeriesType,
	fetchedTimeStamps map[*timestampSeriesType]float64,
	newTimeStampsByGroupId map[int]float64,
	notFetchedTimeStamps []*timestampSeriesType,
	ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.active {
		return
	}
	ok = true
	valueByMetric := make(map[*MetricInfo]interface{})
	timestampByGroupId := make(map[int]float64)
	fetched = make(map[*timeSeriesType]interface{})
	fetchedTimeStamps = make(map[*timestampSeriesType]float64)
	newTimeStampsByGroupId = make(map[int]float64)
	for i := range metrics {
		// TODO: Allow distribution metrics later.
		if metrics[i].Kind == types.Dist {
			continue
		}
		id := c.metricInfoStore.Register(metrics[i])
		valueByMetric[id] = metrics[i].Value
		if metrics[i].TimeStamp == nil {
			timestampByGroupId[id.GroupId()] = timestamp
		} else {
			timestampByGroupId[id.GroupId()] = trimessages.TimeToFloat(metrics[i].TimeStamp.(time.Time))
		}
	}
	// populate notFetched
	for id, series := range c.timeSeries {
		if _, ok := valueByMetric[id]; !ok {
			notFetched = append(notFetched, series)
		}
	}
	// populate fetched and newOnes
	for id, value := range valueByMetric {
		if c.timeSeries[id] == nil {
			thisTs := timestampByGroupId[id.GroupId()]
			c.timeSeries[id] = newTimeSeriesType(
				id, thisTs, value, c.metrics)
			newOnes = append(newOnes, c.timeSeries[id])
		} else {
			fetched[c.timeSeries[id]] = value
		}
	}
	// populate notFetchedTimeStamps
	for groupId, series := range c.timestampSeries {
		if _, ok := timestampByGroupId[groupId]; !ok {
			notFetchedTimeStamps = append(
				notFetchedTimeStamps, series)
		}
	}
	// populate fetchedTimeStamps and newTimeStampsByGroupId
	for groupId, ts := range timestampByGroupId {
		if c.timestampSeries[groupId] == nil {
			c.timestampSeries[groupId] = newTimeStampSeriesType(
				groupId, ts, c.metrics)
			newTimeStampsByGroupId[groupId] = ts
		} else {
			fetchedTimeStamps[c.timestampSeries[groupId]] = ts
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
	timeSeriesList, timestampSeriesList := c.TsAllAndTimeStampsMarkingInactive()
	var inactiveCount int
	for i := range timeSeriesList {
		// If it was active
		if inactivateTimeSeries(
			timeSeriesList[i], timestamp, supplier) {
			reclaimHighList = append(
				reclaimHighList,
				timeSeriesList[i].PageList())
			inactiveCount++
		}
	}
	for i := range timestampSeriesList {
		justInactivated, _ := inactivateTimeStampSeries(
			timestampSeriesList[i], supplier)
		// If it was active
		if justInactivated {
			reclaimHighList = append(
				reclaimHighList,
				timestampSeriesList[i].PageList())
		}
	}

	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	c.metrics.AddUniqueValues(inactiveCount)

	return
}

func (c *timeSeriesCollectionType) AddBatch(
	timestamp float64,
	metrics trimessages.MetricList,
	supplier *pageQueueType) (result int, ok bool) {
	var reclaimLowList, reclaimHighList []pageListType
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	fetched, newOnes, notFetched, tsFetched, timestamps, tsNotFetched, ok := c.LookupBatch(timestamp, metrics)
	if !ok {
		return
	}
	addedCount := len(newOnes)

	// Do a dry run of adding timestamps so that we can finish
	// populating the timestamps map. We don't actually add the
	// timestamps because they must be added AFTER the values for
	// iteration when writing to LMM to work correctly.
	for timestampSeries, tsValue := range tsFetched {
		neededToAdd := timestampSeries.AddDryRun(tsValue)
		if neededToAdd {
			timestamps[timestampSeries.GroupId()] = tsValue
		}
	}

	// Do a dry run of inactivating a timestamp series
	for i := range tsNotFetched {
		justInactivated, adjustedTsValue := tsNotFetched[i].InactivateDryRun()
		if justInactivated {
			timestamps[tsNotFetched[i].GroupId()] = adjustedTsValue
		}
	}

	// For each metric in fetched, manually add its value to its
	// time series
	for timeSeries, value := range fetched {
		thisTimeStamp, ok := timestamps[timeSeries.GroupId()]
		if ok {
			needToAdd, justActivated := addToTimeSeries(
				timeSeries,
				thisTimeStamp,
				value,
				supplier)

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
	}

	var inactiveCount int
	for i := range notFetched {
		thisTimeStamp, ok := timestamps[notFetched[i].GroupId()]
		if ok && inactivateTimeSeries(
			notFetched[i],
			thisTimeStamp,
			supplier) {
			reclaimHighList = append(
				reclaimHighList,
				notFetched[i].PageList())
			inactiveCount++
		}
	}

	// Add each timestamp to its timestamp series. This must be done
	// after adding the values.
	for timestampSeries, tsValue := range tsFetched {
		_, justActivated := addToTimeStampSeries(
			timestampSeries, tsValue, supplier)
		if justActivated {
			reclaimLowList = append(
				reclaimLowList,
				timestampSeries.PageList())
		}
	}

	// Inactivate any missed timestamp series.
	// Must be done after adding the values
	for i := range tsNotFetched {
		justInactivated, _ := inactivateTimeStampSeries(tsNotFetched[i], supplier)
		if justInactivated {
			reclaimHighList = append(
				reclaimHighList,
				notFetched[i].PageList())
		}
	}

	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	// recliamLowList has all the page lists that should be reclaimed with
	// low priority
	supplier.ReclaimLow(reclaimLowList)

	result = inactiveCount + addedCount
	c.metrics.AddUniqueValues(result)
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

func tsAndValueSize() int {
	var p pageType
	return int(reflect.TypeOf(p).Elem().Size())
}

func makeUnionSlice(raw []byte) (p pageType, t tsPageType) {
	sizeInBytes := len(raw)
	rawPtr := (*reflect.SliceHeader)(unsafe.Pointer(&raw)).Data
	pHeader := (*reflect.SliceHeader)(unsafe.Pointer(&p))
	tHeader := (*reflect.SliceHeader)(unsafe.Pointer(&t))
	pHeader.Data = rawPtr
	pHeader.Len = 0
	pHeader.Cap = sizeInBytes / int(reflect.TypeOf(p).Elem().Size())
	tHeader.Data = rawPtr
	tHeader.Len = 0
	tHeader.Cap = sizeInBytes / int(reflect.TypeOf(t).Elem().Size())
	return
}

func newPageQueueType(
	bytesPerPage int,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *pageQueueType {
	pages := btreepq.New(
		pageCount,
		int(float64(pageCount)*inactiveThreshhold),
		degree,
		func() btreepq.Page {
			return newPageWithMetaDataType(bytesPerPage)
		})
	return &pageQueueType{
		valueCountPerPage:  bytesPerPage / gTsAndValueSize,
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
	queueGroup := tricorder.NewGroup()
	queueGroup.RegisterUpdateFunc(func() time.Time {
		s.PageQueueStats(&queueStats)
		return time.Now()
	})
	if err = tricorder.RegisterMetricInGroup(
		"/store/highPriorityCount",
		&queueStats.HighPriorityCount,
		queueGroup,
		units.None,
		"Number of pages in high priority queue"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/lowPriorityCount",
		&queueStats.LowPriorityCount,
		queueGroup,
		units.None,
		"Number of pages in low priority queue"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/nextLowPrioritySeqNo",
		&queueStats.NextLowPrioritySeqNo,
		queueGroup,
		units.None,
		"Next seq no in low priority queue, 0 if empty"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/nextHighPrioritySeqNo",
		&queueStats.NextHighPrioritySeqNo,
		queueGroup,
		units.None,
		"Next seq no in high priority queue, 0 if empty"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/endSeqNo",
		&queueStats.EndSeqNo,
		queueGroup,
		units.None,
		"All seq no smaller than this. Marks end of both queues."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/highPriorityRatio",
		queueStats.HighPriorityRatio,
		queueGroup,
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
// This call may lock another pageOwnerType instance. To avoid deadlock,
// caller must not hold a lock on any pageOwnerType instance.
func (s *pageQueueType) GivePageTo(t pageOwnerType) {
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

type storePrimitiveMetricsType struct {
	UniqueMetricValueCount int64
	TimeStampPageCount     int64
}

type storeMetricsType struct {
	PagesPerMetricDist *tricorder.NonCumulativeDistribution
	lock               sync.Mutex
	values             storePrimitiveMetricsType
}

func (s *storeMetricsType) Metrics(v *storePrimitiveMetricsType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	*v = s.values
}

func (s *storeMetricsType) RemoveValuePage(
	oldLen int, metricCountInPage int) {
	oldLenF := float64(oldLen)
	s.PagesPerMetricDist.Update(oldLenF, oldLenF-1.0)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.UniqueMetricValueCount -= int64(metricCountInPage)
}

func (s *storeMetricsType) RemoveTimeStampPage() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.TimeStampPageCount -= 1
}

func (s *storeMetricsType) NewValueSeries() {
	s.PagesPerMetricDist.Add(0.0)
}

func (s *storeMetricsType) NewTimeStampSeries() {
}

func (s *storeMetricsType) AddEmptyValuePage(oldLen int) {
	oldLenF := float64(oldLen)
	s.PagesPerMetricDist.Update(oldLenF, oldLenF+1.0)
}

func (s *storeMetricsType) AddEmptyTimeStampPage() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.TimeStampPageCount += 1
}

func (s *storeMetricsType) AddUniqueValues(count int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.UniqueMetricValueCount += int64(count)
}

func newStoreMetricsType() *storeMetricsType {
	return &storeMetricsType{
		PagesPerMetricDist: gBucketer.NewNonCumulativeDistribution(),
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

func (s *Store) namedIteratorForEndpoint(
	name string,
	endpointId interface{},
	maxFrames int) NamedIterator {
	return s.byApplication[endpointId].NewNamedIterator(name, maxFrames)
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
	var primitiveMetrics storePrimitiveMetricsType
	storeGroup := tricorder.NewGroup()
	storeGroup.RegisterUpdateFunc(func() time.Time {
		metrics.Metrics(&primitiveMetrics)
		return time.Now()
	})
	if err = tricorder.RegisterMetricInGroup(
		"/store/pageUtilization",
		func() float64 {
			metricValueCount := primitiveMetrics.UniqueMetricValueCount
			pagesInUseCount := metrics.PagesPerMetricDist.Sum()
			metricCount := metrics.PagesPerMetricDist.Count()
			extraValueCount := float64(metricValueCount) - float64(metricCount)
			return extraValueCount / pagesInUseCount / float64(maxValuesPerPage)
		},
		storeGroup,
		units.None,
		"Page utilization 0.0 - 1.0"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/metricValueCount",
		&primitiveMetrics.UniqueMetricValueCount,
		storeGroup,
		units.None,
		"Number of unique metrics values"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/valuePageCount",
		metrics.PagesPerMetricDist.Sum,
		storeGroup,
		units.None,
		"Number of pages used for values."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/timestampPageCount",
		&primitiveMetrics.TimeStampPageCount,
		storeGroup,
		units.None,
		"Number of pages used for timestamps."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/totalPagesInUseCount",
		func() int64 {
			return primitiveMetrics.TimeStampPageCount + int64(metrics.PagesPerMetricDist.Sum())
		},
		storeGroup,
		units.None,
		"Total number of pages used."); err != nil {
		return
	}
	return
}
