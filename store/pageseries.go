package store

import (
	"container/list"
	"math"
	"reflect"
	"sync"
)

// This file contains all the code related to data structures containing
// a linked list of pages including time series of metric values and
// timestamp lists.

type inactiveType int

var gInactive inactiveType

var (
	kPlusInf = math.Inf(1)
)

func (m *MetricInfo) valuesAreEqual(lhs, rhs interface{}) bool {
	if lhs != gInactive && rhs != gInactive && !m.kind.SupportsEquality() {
		return reflect.DeepEqual(lhs, rhs)
	}
	return lhs == rhs
}

// pageOwnerType is the interface for any data structure that can own
// pages.
type pageOwnerType interface {
	// GiveUpPage instructs this instance to give up given page
	GiveUpPage(page *pageWithMetaDataType)
	// AcceptPage instructs this instance to accept given page
	AcceptPage(page *pageWithMetaDataType)
}

// pageListType Represents a list of pages owned by the same page series.
// Page owners can change at any time. In particular they can change
// between when we get a time series' page list and when we change
// the priority of those pages. This abstraction makes it possible to
// ensure that we don't change the priority of a page that has been given
// a new owner.
type pageListType struct {
	Owner pageOwnerType
	Pages []*pageWithMetaDataType
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

// timestampSeriesType represents a sorted list of timestamps for a particular
// metric group.
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

// Latest returns the latest timestamp in this series as seconds since
// Jan 1, 1970 GMT.
func (t *timestampSeriesType) Latest() float64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.lastTs
}

func (t *timestampSeriesType) Earliest() float64 {
	firstTimes := t.FindAfter(0.0, 1)
	if len(firstTimes) > 0 {
		return firstTimes[0]
	}
	return t.Latest()
}

// PageList returns the pages this series owns.
func (t *timestampSeriesType) PageList() pageListType {
	t.lock.Lock()
	defer t.lock.Unlock()
	return pageListType{
		Owner: t,
		Pages: t.pages.PageList(),
	}
}

// AddDryRun checks if timestamp can be added without adding it.
func (t *timestampSeriesType) AddDryRun(ts float64) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return ts > t.lastTs
}

// Add attempts to add a timestamp to the end of this timestamp list.
//
// neededToAdd = false means that the timestamp can't be added because
// it is not greater than the last timestamp in the list.
//
// neededToAdd = true; addSuccessful = false means that timestamp was not
// addded because there is no available space.
//
// neededToAdd = true; addSuccessful = true means that timestamp was
// successfully added.
//
// Everything true means timestamp was successfully added and this
// timestamp series was just activated.
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

// Inactivates this time series getting the new timestamp for inactivation
// without actually inactivating.
func (t *timestampSeriesType) InactivateDryRun() (
	needed bool, actualTs float64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.active {
		return
	}
	return true, incTs(t.lastTs)
}

// Inactivate this time series.
// neededToAdd = false means timestamp already inactivated
//
// neededToAdd = true; addSuccessful = false means no space available for
// new inactivate timestamp
//
// neededtoAdd = true; addSuccessful = true means this series inactivated
// and actualTs is the inactivate timestamp
func (t *timestampSeriesType) Inactivate() (
	neededToAdd, addSuccessful bool, actualTs float64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if !t.active {
		return
	}
	neededToAdd = true
	lastPage, isNew := t.pages.LastPageMustHaveSpace(nil)
	if lastPage == nil {
		return
	}
	addSuccessful = true
	t.active = false
	if isNew {
		t.metrics.AddEmptyTimeStampPage()
	}
	latestPage := lastPage.Times()
	latestPage.Add(t.lastTs)
	t.lastTs = incTs(t.lastTs)
	actualTs = t.lastTs
	return
}

// GiveUpPage instructs this series to give up the given page.
// Only the page queue calls this. This method must not be called directly.
// The page queue will always take pages away in the same
// order it bestowed them.
func (t *timestampSeriesType) GiveUpPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.pages.GiveUpPage(page, nil) {
		t.metrics.RemoveTimeStampPage()
	}
}

// AcceptPage grants given page to this series. The page queue calls
// this to bestow a page to this series. This method must not be called
// directly.
func (t *timestampSeriesType) AcceptPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.pages.AcceptPage(page)
}

// FindAfter returns timestamps after start and returns at most maxCount
// timestamps. If maxCount = 0, returns all timestamps after start.
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

// timeSeriesType represents a list of timestamped values in ascending order
// by timestamp.
type timeSeriesType struct {
	id        *MetricInfo
	metrics   *storeMetricsType
	lock      sync.Mutex
	lastValue tsValueType
	pages     pageSeriesType
}

func newTimeSeriesType(
	id *MetricInfo,
	ts float64, value interface{},
	metrics *storeMetricsType) *timeSeriesType {
	result := &timeSeriesType{id: id, metrics: metrics}
	result.lastValue.TimeStamp = ts
	result.lastValue.Value = value
	result.pages.Init((*pageWithMetaDataType).ValuePage)
	result.metrics.NewValueSeries()
	return result
}

func (t *timeSeriesType) GroupId() int {
	return t.id.GroupId()
}

// AcceptPage grants given page to this series. The page queue calls
// this to bestow a page to this series. This method must not be called
// directly.
func (t *timeSeriesType) AcceptPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.pages.AcceptPage(page)
}

// GiveUpPage instructs this series to give up the given page.
// Only the page queue calls this. This method must not be called directly.
// The page queue will always take pages away in the same
// order it bestowed them.
func (t *timeSeriesType) GiveUpPage(page *pageWithMetaDataType) {
	t.lock.Lock()
	defer t.lock.Unlock()
	oldLen := t.pages.Len()
	if t.pages.GiveUpPage(page, nil) {
		t.metrics.RemoveValuePage(oldLen, page.Values().Len())
	}
}

// PageList returns the pages this series owns.
func (t *timeSeriesType) PageList() pageListType {
	t.lock.Lock()
	defer t.lock.Unlock()
	return pageListType{
		Owner: t,
		Pages: t.pages.PageList(),
	}
}

func (t *timeSeriesType) valuesAreEqual(lhs, rhs interface{}) bool {
	return t.id.valuesAreEqual(lhs, rhs)
}

func (t *timeSeriesType) needToAdd(timestamp float64, value interface{}) (
	needToAdd bool) {
	// Let a new gInactive value come in even if its timestamp
	// is earlier than the last.
	if (timestamp <= t.lastValue.TimeStamp && value != gInactive) || t.valuesAreEqual(value, t.lastValue.Value) {
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
	justActivated = t.lastValue.Value == gInactive
	if newPage {
		t.metrics.AddEmptyValuePage(oldLen)
	}
	latestPage := lastPage.Values()
	latestPage.Add(t.lastValue)
	// Ensure that timestamps are monotone increasing. We could get an
	// earlier timestamp if we are getting an inactive marker.
	if timestamp <= t.lastValue.TimeStamp {
		// If current timestamp is earlier, make it be 1ms more than
		// last timestamp. Adding 1ms makes the timestamp
		// greater for any timestamp < 2^43 seconds past Jan 1 1970
		// which is millinea into the future.
		t.lastValue.TimeStamp = incTs(t.lastValue.TimeStamp)
	} else {
		t.lastValue.TimeStamp = timestamp
	}
	t.lastValue.Value = value
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
	if t.lastValue.TimeStamp < end {
		record.TimeStamp = t.lastValue.TimeStamp
		record.setValue(t.lastValue.Value)
		if !result.Append(&record) {
			return
		}
	}
	if t.lastValue.TimeStamp > start {
		t.pages.Fetch(start, end, &record, result)
	}
}

// FetchForward works like fetch except that it adds values to result in
// ascending order.
func (t *timeSeriesType) FetchForward(
	applicationId interface{},
	start, end float64,
	result Appender) {
	record := Record{
		EndpointId: applicationId,
		Info:       t.id}
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.lastValue.TimeStamp > start {
		// We have to look at the pages
		if !t.pages.FetchForward(start, end, &record, result) {
			return
		}
	}
	// Only include latest value if it comes before end timestamp.
	if t.lastValue.TimeStamp < end {
		record.TimeStamp = t.lastValue.TimeStamp
		record.setValue(t.lastValue.Value)
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
	appender.Flush()
}

// A convenience method for adding a value to a series.
// It handles when an add fails because there is no space by instructing
// the page queue to grant a new page.
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

// A convenience method for adding a timestamp to a timestamp series.
// It handles when an add fails because there is no space by instructing
// the page queue to grant a new page.
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

// A convenience method for inactivating a series.
// It handles when an add fails because there is no space by instructing
// the page queue to grant a new page.
func inactivateTimeSeries(
	timeSeries *timeSeriesType,
	timestamp float64,
	supplier *pageQueueType) (justInactivated bool) {
	justInactivated, _ = addToTimeSeries(timeSeries, timestamp, gInactive, supplier)
	return
}

// A convenience method for inactivating a timestamp series.
// It handles when an add fails because there is no space by instructing
// the page queue to grant a new page.
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

type pathAndNumeric struct {
	Path      string
	IsNumeric bool
}

func (m MetricGroupingStrategy) partition(
	ts []*timeSeriesType) partitionType {
	return &timeSeriesPartitionType{timeSeries: ts, strategy: m}
}

func (m MetricGroupingStrategy) partitionByReference(
	tsRef *[]*timeSeriesType) partitionType {
	return &timeSeriesByReferencePartitionType{tsRef: tsRef, strategy: m}
}

func (m MetricGroupingStrategy) orderedPartition(
	ts []*timeSeriesType) partitionType {
	result := &timeSeriesPartitionType{timeSeries: ts, strategy: m}
	formPartition(result)
	return result
}

type timeSeriesPartitionType struct {
	timeSeries []*timeSeriesType
	strategy   MetricGroupingStrategy
}

func (t *timeSeriesPartitionType) Len() int {
	return len(t.timeSeries)
}

func (t *timeSeriesPartitionType) SubsetId(idx int) interface{} {
	return t.strategy(t.timeSeries[idx].id)
}

func (t *timeSeriesPartitionType) Swap(i, j int) {
	t.timeSeries[i], t.timeSeries[j] = t.timeSeries[j], t.timeSeries[i]
}

type timeSeriesByReferencePartitionType struct {
	tsRef    *[]*timeSeriesType
	strategy MetricGroupingStrategy
}

func (t *timeSeriesByReferencePartitionType) Len() int {
	return len(*t.tsRef)
}

func (t *timeSeriesByReferencePartitionType) SubsetId(idx int) interface{} {
	return t.strategy((*t.tsRef)[idx].id)
}

// mergerType merges records from several time series together.
// Using the same mergerType instance to do multiple merges amortizes heap
// allocations. The zero value of mergerType is a ready to use instance.
type mergerType struct {
	records      []Record
	recordSlices [][]Record
}

func newMerger() *mergerType {
	return &mergerType{}
}

// MergeOldestFirst merges from oldest to newest.
// series is all the time series
// producer produces the records for one time series. producer must produce
// records oldest to newest. dest is where the merged records are appended.
func (m *mergerType) MergeOldestFirst(
	series []*timeSeriesType,
	producer func(ts *timeSeriesType, a Appender),
	dest Appender) {
	m.mergeSomeWay(
		series,
		producer,
		mergeOldestFirst,
		dest)
}

// MergeOldestFirst merges from newest to oldest.
// series is all the time series
// producer produces the records for one time series. producer must produce
// records newest to oldest. dest is where the merged records are appended.
func (m *mergerType) MergeNewestFirst(
	series []*timeSeriesType,
	producer func(ts *timeSeriesType, a Appender),
	dest Appender) {
	m.mergeSomeWay(
		series,
		producer,
		mergeNewestFirst,
		dest)
}

func (m *mergerType) mergeSomeWay(
	series []*timeSeriesType,
	producer func(ts *timeSeriesType, a Appender),
	finalMerger func(recordSeries [][]Record, a Appender),
	dest Appender) {
	if len(series) == 1 {
		producer(series[0], dest)
	} else {
		m.records = m.records[:0]
		m.recordSlices = m.recordSlices[:0]
		sliceAppender := AppendTo(&m.records)
		startIdx := 0
		for _, ts := range series {
			producer(ts, sliceAppender)
			m.recordSlices = append(
				m.recordSlices, m.records[startIdx:])
			startIdx = len(m.records)
		}
		finalMerger(m.recordSlices, dest)
		m.cleanUpForGC()
	}
}

func (m *mergerType) cleanUpForGC() {
	for i := range m.records {
		m.records[i] = Record{}
	}
}
