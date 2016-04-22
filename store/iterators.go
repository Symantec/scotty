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
