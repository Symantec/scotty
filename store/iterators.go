package store

import (
	"sort"
)

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
	completed       map[*timeSeriesType]float64
}

type namedIteratorType struct {
	name                 string
	timeSeriesCollection *timeSeriesCollectionType
	// startTimeStamps does not change during this instance's lifetime
	startTimeStamps   map[int]float64
	timestamps        map[int][]float64
	completed         map[*timeSeriesType]float64
	timeSeries        []*timeSeriesType
	records           []Record
	recordIdx         int
	currentTimeSeries *timeSeriesType
}

func (n *namedIteratorType) moreRecords() bool {
	if len(n.timeSeries) == 0 {
		return false
	}
	n.currentTimeSeries = n.timeSeries[0]
	n.timeSeries = n.timeSeries[1:]
	ourTimestamps := n.timestamps[n.currentTimeSeries.GroupId()]
	lastWrittenTimestamp := n.completed[n.currentTimeSeries]
	index := sort.Search(
		len(ourTimestamps),
		func(x int) bool {
			return ourTimestamps[x] > lastWrittenTimestamp
		})
	n.records = n.records[:0]
	n.currentTimeSeries.FetchForwardWithTimeStamps(
		n.timeSeriesCollection.applicationId,
		ourTimestamps[index:],
		AppendTo(&n.records))
	return true
}

func copyCompleted(
	original map[*timeSeriesType]float64) map[*timeSeriesType]float64 {
	result := make(map[*timeSeriesType]float64, len(original))
	for k, v := range original {
		result[k] = v
	}
	return result
}

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

func (n *namedIteratorType) snapshot() *namedIteratorDataType {
	if !n.hasNext() {
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
	n.timeSeriesCollection.saveProgress(n.name, n.snapshot())
}

func (n *namedIteratorType) hasNext() bool {
	for n.recordIdx == len(n.records) {
		if !n.moreRecords() {
			return false
		}
		n.recordIdx = 0
	}
	return true
}

// Like Next, but doesn't advance iterator.
func (n *namedIteratorType) peek(r *Record) bool {
	if !n.hasNext() {
		return false
	}
	*r = n.records[n.recordIdx]
	return true
}

func (n *namedIteratorType) Next(r *Record) bool {
	if !n.hasNext() {
		return false
	}
	*r = n.records[n.recordIdx]
	n.recordIdx++
	// Mark current progress
	n.completed[n.currentTimeSeries] = r.TimeStamp
	return true
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
	if a.count > 0 && r.Info != a.record.Info {
		panic("Trying to aggregate heterogeneous records")
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
	Interval   float64
	aggregator aggregatorType
}

func (n *rollUpNamedIteratorType) Next(result *Record) bool {
	var record Record
	for n.namedIteratorType.peek(&record) {
		// Can't roll up inactive values with active ones so we
		// skip the inactive ones.
		if !record.Active {
			n.namedIteratorType.Next(&record)
			continue
		}
		if !n.aggregator.IsEmpty() {
			frameId := getFrameId(record.TimeStamp, n.Interval)
			if record.Info != n.aggregator.FirstInfo() || frameId != getFrameId(n.aggregator.FirstTs(), n.Interval) {
				n.aggregator.Result(result)
				n.aggregator.Clear()
				return true
			}
		}
		n.namedIteratorType.Next(&record)
		n.aggregator.Add(&record)
	}
	if n.aggregator.IsEmpty() {
		return false
	}
	n.aggregator.Result(result)
	n.aggregator.Clear()
	return true
}

func getFrameId(t, dur float64) int64 {
	return int64(t / dur)
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
