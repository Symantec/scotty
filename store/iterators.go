package store

import (
	"math"
	"sort"
)

type optimisedNamedIteratorType interface {
	next(r *Record, typeFilter func(*MetricInfo) bool) bool
	commit(typeFilter func(*MetricInfo) bool)
	Name() string
}

// This file contains all the code for implementing iterators.

type filterIteratorType struct {
	filter  Filterer
	wrapped Iterator
}

func (f *filterIteratorType) Next(r *Record) bool {
	for f.wrapped.Next(r) {
		if f.filter.Filter(r) {
			return true
		}
	}
	return false
}

type filterOptimisedNamedIteratorType struct {
	filter  TypeFilterer
	wrapped optimisedNamedIteratorType
}

func (f *filterOptimisedNamedIteratorType) Next(r *Record) bool {
	for f.wrapped.next(r, f.filter.FilterByType) {
		if f.filter.Filter(r) {
			return true
		}
	}
	return false
}

func (f *filterOptimisedNamedIteratorType) Commit() {
	f.wrapped.commit(f.filter.FilterByType)
}

func (f *filterOptimisedNamedIteratorType) Name() string {
	return f.wrapped.Name()
}

func namedIteratorFilter(
	ni NamedIterator, filter Filterer) NamedIterator {
	aFilter, fok := filter.(TypeFilterer)
	namedIterator, nok := ni.(optimisedNamedIteratorType)
	if fok && nok {
		return &filterOptimisedNamedIteratorType{
			filter:  aFilter,
			wrapped: namedIterator}
	}
	return &changedNamedIteratorType{
		NamedIterator: ni,
		change:        IteratorFilter(ni, filter)}
}

type coordinatorIteratorType struct {
	wrapped     Iterator
	coordinator Coordinator
	skipped     *uint64
	leaseSpan   float64
	startTime   float64
	endTime     float64
}

func (c *coordinatorIteratorType) Next(r *Record) bool {
	for c.wrapped.Next(r) {
		if r.TimeStamp >= c.endTime {
			c.startTime, c.endTime = c.coordinator.Lease(
				c.leaseSpan, r.TimeStamp)
		}
		// r.TimeStamp < c.endTime
		if r.TimeStamp >= c.startTime {
			return true
		}
		if c.skipped != nil {
			(*c.skipped)++
		}
	}
	return false
}

type coordinatorNamedIteratorType struct {
	wrapped       NamedIterator
	change        Iterator
	updateSkipped func(uint64)
	skippedSoFar  uint64
}

func (c *coordinatorNamedIteratorType) Next(r *Record) bool {
	return c.change.Next(r)
}

func (c *coordinatorNamedIteratorType) Name() string {
	return c.wrapped.Name()
}

func (c *coordinatorNamedIteratorType) Commit() {
	c.wrapped.Commit()
	if c.updateSkipped != nil {
		c.updateSkipped(c.skippedSoFar)
	}
	c.skippedSoFar = 0
}

type changedNamedIteratorType struct {
	NamedIterator
	change Iterator
}

func (c *changedNamedIteratorType) Next(r *Record) bool {
	return c.change.Next(r)
}

// Designed to be immutable
type namedIteratorDataType struct {
	startTimeStamps map[int]float64
	completed       map[*MetricInfo]float64
}

type namedIteratorType struct {
	name                 string
	timeSeriesCollection *timeSeriesCollectionType
	// startTimeStamps does not change during this instance's lifetime
	startTimeStamps map[int]float64
	timestamps      map[int][]float64
	completed       map[*MetricInfo]float64
	timeSeries      []*timeSeriesType
	strategy        MetricGroupingStrategy
	partition       partitionType
	merger          mergerType
	records         []Record
	recordIdx       int
}

func (n *namedIteratorType) recordsFromSingleTimeSeries(
	ts *timeSeriesType, appender Appender) {
	ourTimestamps := n.timestamps[ts.GroupId()]
	lastWrittenTimestamp := n.completed[ts.id]
	index := sort.Search(
		len(ourTimestamps),
		func(x int) bool {
			return ourTimestamps[x] > lastWrittenTimestamp
		})
	ts.FetchForwardWithTimeStamps(
		n.timeSeriesCollection.applicationId,
		ourTimestamps[index:],
		appender)
}

func filterTimeSeries(
	timeSeries []*timeSeriesType,
	typeFilter func(*MetricInfo) bool) (filtered []*timeSeriesType) {
	for _, ts := range timeSeries {
		if typeFilter(ts.id) {
			filtered = append(filtered, ts)
		}
	}
	return filtered
}

func (n *namedIteratorType) moreRecords(
	typeFilter func(*MetricInfo) bool) bool {
	if len(n.timeSeries) == 0 {
		return false
	}
	if n.partition == nil {
		n.partition = n.strategy.partitionByReference(&n.timeSeries)
	}
	subsetLen := nextSubset(n.partition, 0)
	currentSubset := n.timeSeries[:subsetLen]
	if typeFilter != nil {
		currentSubset = filterTimeSeries(currentSubset, typeFilter)
	}
	n.timeSeries = n.timeSeries[subsetLen:]
	n.records = n.records[:0]
	if len(currentSubset) > 0 {
		n.merger.MergeOldestFirst(
			currentSubset,
			n.recordsFromSingleTimeSeries,
			AppendTo(&n.records))
	}
	return true
}

func copyCompleted(
	original map[*MetricInfo]float64) map[*MetricInfo]float64 {
	result := make(map[*MetricInfo]float64, len(original))
	for k, v := range original {
		result[k] = v
	}
	return result
}

// nextStartTimeStamps must not change the state of this iterator.
func (n *namedIteratorType) nextStartTimeStamps() map[int]float64 {
	result := make(map[int]float64, len(n.startTimeStamps))
	for k, v := range n.startTimeStamps {
		result[k] = v
	}
	for groupId, tss := range n.timestamps {
		length := len(tss)
		if length > 0 {
			result[groupId] = tss[length-1]
		}
	}
	return result
}

func (n *namedIteratorType) snapshot(
	typeFilter func(*MetricInfo) bool) *namedIteratorDataType {
	if !n.hasNext(typeFilter) {
		return &namedIteratorDataType{
			startTimeStamps: n.nextStartTimeStamps(),
		}
	}
	return &namedIteratorDataType{
		startTimeStamps: n.startTimeStamps,
		completed:       copyCompleted(n.completed),
	}
}

func (n *namedIteratorType) Name() string {
	return n.name
}

func (n *namedIteratorType) Commit() {
	n.commit(nil)
}

func (n *namedIteratorType) commit(typeFilter func(*MetricInfo) bool) {
	n.timeSeriesCollection.SaveProgress(n.name, n.snapshot(typeFilter))
}

func (n *namedIteratorType) hasNext(
	typeFilter func(*MetricInfo) bool) bool {
	for n.recordIdx == len(n.records) {
		if !n.moreRecords(typeFilter) {
			return false
		}
		n.recordIdx = 0
	}
	return true
}

// Like Next, but doesn't advance iterator.
func (n *namedIteratorType) peek(
	r *Record, typeFilter func(*MetricInfo) bool) bool {
	if !n.hasNext(typeFilter) {
		return false
	}
	*r = n.records[n.recordIdx]
	return true
}

func (n *namedIteratorType) next(
	r *Record, typeFilter func(*MetricInfo) bool) bool {
	if !n.hasNext(typeFilter) {
		return false
	}
	*r = n.records[n.recordIdx]
	n.recordIdx++
	// Mark current progress
	n.completed[r.Info] = r.TimeStamp
	return true
}

func (n *namedIteratorType) Next(r *Record) bool {
	return n.next(r, nil)
}

type aggregatorType struct {
	record   Record
	timeSum  float64
	valueSum float64
	count    int
}

func (a *aggregatorType) FirstTs() float64 {
	if a.count == 0 {
		panic("Aggregator empty")
	}
	return a.record.TimeStamp
}

func (a *aggregatorType) FirstInfo() *MetricInfo {
	if a.count == 0 {
		panic("Aggregator empty")
	}
	return a.record.Info
}

func (a *aggregatorType) Result(result *Record) {
	if a.count == 0 {
		panic("Aggregator empty")
	}
	*result = a.record
	if result.Info.Kind().CanToFromFloat() {
		result.TimeStamp = a.timeSum / float64(a.count)
		result.Value = result.Info.Kind().FromFloat(
			a.valueSum / float64(a.count))
	}
}

func (a *aggregatorType) Clear() {
	a.count = 0
	a.timeSum = 0.0
	a.valueSum = 0.0
}

func (a *aggregatorType) IsEmpty() bool {
	return a.count == 0
}

func (a *aggregatorType) Add(r *Record) {
	if !r.Active {
		panic("This aggregator can aggregate only active records.")
	}
	if a.count == 0 {
		a.record = *r
	}
	if r.Info.Kind().CanToFromFloat() {
		a.timeSum += r.TimeStamp
		a.valueSum += r.Info.Kind().ToFloat(r.Value)
	}
	a.count++
}

type rollUpNamedIteratorType struct {
	*namedIteratorType
	interval   float64
	strategy   MetricGroupingStrategy
	aggregator aggregatorType
}

func (n *rollUpNamedIteratorType) Next(result *Record) bool {
	return n.next(result, nil)
}

func (n *rollUpNamedIteratorType) next(
	result *Record, typeFilter func(*MetricInfo) bool) bool {
	// When rolling up, we use the exclusive end timestamp of the range
	// instead of the inclusive start time stamp of the range.
	// By doing this we ensure that if the roll up span changes during run
	// time, we won't overwrite data for the same timestamp or earlier
	// timestamp for a given time series.
	//
	// We know that the timestamp for the next range we write out will be
	// strictly after the timestamp we just visited because we use the
	// exclusive end timestamp. We also know that the last timestamp we wrote
	// out is on or before the timestamp we just visited since we have to be
	// past a range to write it out.
	//
	// Both of these are always true regardless of how the roll up span changes.
	// So we can be confident that we will never overwrite data for the
	// same timestamp or earlier one.
	var record Record
	for n.namedIteratorType.peek(&record, typeFilter) {
		// Can't roll up inactive values with active ones so we
		// skip the inactive ones.
		if !record.Active {
			n.namedIteratorType.Next(&record)
			continue
		}
		frameId := getFrameId(record.TimeStamp, n.interval)
		if !n.aggregator.IsEmpty() {
			if !n.strategy.Equal(record.Info, n.aggregator.FirstInfo()) || frameId != getFrameId(n.aggregator.FirstTs(), n.interval) {
				n.aggregator.Result(result)
				n.aggregator.Clear()
				return true
			}
		}
		n.namedIteratorType.Next(&record)
		// Normalise timestamps
		record.TimeStamp = frameId
		n.aggregator.Add(&record)
	}
	if n.aggregator.IsEmpty() {
		return false
	}
	n.aggregator.Result(result)
	n.aggregator.Clear()
	return true
}

func getFrameId(t, dur float64) float64 {
	return math.Floor(t/dur+0.5) * dur
}

// chopForRollUp truncates times so that it contains no partial interval at
// the end.
//
// dur is the length of the interval. dur should be something that
// divides 1 second, 1 minute, or 1 hour.
//
// maxFrames > 0 means no more than maxFrames intervals;
// maxFrames <= 0 means no limit on intervals.
func chopForRollUp(dur float64, maxFrames int, times *[]float64) {
	length := len(*times)
	if length == 0 {
		return
	}
	lastFrameId := getFrameId((*times)[0], dur)
	endIdx := 0
	for i := 1; i < length; i++ {
		frameId := getFrameId((*times)[i], dur)
		if frameId != lastFrameId {
			lastFrameId = frameId
			endIdx = i
			maxFrames--
			if maxFrames == 0 {
				break
			}
		}
	}
	*times = (*times)[:endIdx]
}
