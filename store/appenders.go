package store

import (
	"container/heap"
)

// This file contains the code for implementing Appenders.

type limitAppenderType struct {
	limit   int
	wrapped Appender
}

func (l *limitAppenderType) Append(r *Record) bool {
	result := l.wrapped.Append(r)
	l.limit--
	if l.limit == 0 {
		return false
	}
	return result
}

type filterAppenderType struct {
	filter  Filterer
	wrapped Appender
}

func (f *filterAppenderType) Append(r *Record) bool {
	if f.filter.Filter(r) {
		return f.wrapped.Append(r)
	}
	return true
}

type recordListType []Record

func (l *recordListType) Append(r *Record) bool {
	*l = append(*l, *r)
	return true
}

type tsAppenderType []float64

func (t *tsAppenderType) Append(r *Record) bool {
	*t = append(*t, r.TimeStamp)
	return true
}

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

type mergeWithTimestampsType struct {
	timestamps    []float64
	timestampIdx  int
	wrapped       Appender
	lastRecord    Record
	lastRecordSet bool
}

// newMergeWithTimestamps creates an appender that accepts unique timestamped
// values, merges them with the timestamps in timestamps, and emits them to
// result.
//
// This appender will emit each value it accepts 0 or more times to result
// depending on how many timestamps in the timestamps slice fall on it or
// immediately after it. For example, if this appender accepts value A
// with timestamp 1000 and value B with timestamp 1004 and value C with
// timestamp 1007 and the timestamp slice contains:
// ..., 1000, 1002, 1003, 1008,... then this appender will emit:
// ...; A, 1000; A, 1002; A, 1003; C, 1008; ... to result
// Note that no B is emitted to result as there is no timestamp that falls
// on 1004 or immediately after 1004 but not before C's timestamp of 1007.
//
// The timestamps slice must be sorted in ascending order and the returned
// appender must accept value-timestamp records in ascending order by
// timestamp.
func newMergeWithTimestamps(
	timestamps []float64, result Appender) *mergeWithTimestampsType {
	return &mergeWithTimestampsType{
		timestamps: timestamps,
		wrapped:    result}
}

func (m *mergeWithTimestampsType) Append(r *Record) bool {
	tsLen := len(m.timestamps)

	// We hit this block when we accept the very first timestamp-value
	// record. We remember that we will emit for this first value
	// but we don't emit anything just yet.
	if !m.lastRecordSet {
		m.lastRecord = *r
		m.lastRecordSet = true
		// Advance through timestamps until we get a timestamp that
		// falls on or after the value we just accepted.
		for m.timestampIdx < tsLen {
			if m.timestamps[m.timestampIdx] >= r.TimeStamp {
				return true
			}
			m.timestampIdx++
		}
		// If we reached end of timestamp slice, let caller know we
		// are done accepting values.
		return false
	}
	// We always start here if we are accepting our second, third, fourth
	// etc. value timestamp pair. We emit to wrapped appender for each
	// timestamp using the previous value until the timestamp falls on
	// or after the timestamp of the value we just accepted.
	for m.timestampIdx < tsLen {
		timestamp := m.timestamps[m.timestampIdx]

		// We are done emitting for the previous value.
		if timestamp >= r.TimeStamp {
			m.lastRecord = *r
			return true
		}
		m.lastRecord.TimeStamp = timestamp
		// Emit
		if !m.wrapped.Append(&m.lastRecord) {
			// Since our appender is bailing early, we don't
			// want Finalize to do anything
			m.lastRecordSet = false
			return false
		}
		m.timestampIdx++
	}
	// If we reached end of timestamps, tell caller we are done accepting
	// values.
	return false
}

// Caller must always call Finalize when done with this instance.
// Finalize may emit additional items to the underlying appender.
func (m *mergeWithTimestampsType) Finalize() {
	tsLen := len(m.timestamps)
	if !m.lastRecordSet {
		return
	}
	for m.timestampIdx < tsLen {
		m.lastRecord.TimeStamp = m.timestamps[m.timestampIdx]
		if !m.wrapped.Append(&m.lastRecord) {
			return
		}
		m.timestampIdx++
	}
}

func biggerFloatsFirst(first, second float64) bool {
	return first > second
}

func smallerFloatsFirst(first, second float64) bool {
	return first < second
}

// mergeNewestFirst Appends records from multiple record slices to given
// appender latest to earliest.
// Each slice in recordSeries must be sorted by time from latest to earliest.
// meregeNewestFirst is intended to merge records of grouped metrics so if
// two records have the same timestamp, it emits ony one of them to a, an
// active record if possible.
func mergeNewestFirst(recordSeries [][]Record, a Appender) {
	mergeSomeWay(
		recordSeries,
		biggerFloatsFirst,
		a)
}

// mergeOldestFirst Appends records from multiple record slices to given
// appender earliest to latest.
// Each slice in recordSeries must be sorted by time from earliest to latest.
// meregeOldestFirst is intended to merge records of grouped metrics so if
// two records have the same timestamp, it emits ony one of them to a, an
// active record if possible.
func mergeOldestFirst(recordSeries [][]Record, a Appender) {
	mergeSomeWay(
		recordSeries,
		smallerFloatsFirst,
		a)
}

func trimEmptyRecordLists(series [][]Record) [][]Record {
	result := make([][]Record, len(series))
	idx := 0
	for i := range series {
		if len(series[i]) > 0 {
			result[idx] = series[i]
			idx++
		}
	}
	return result[:idx]
}

func mergeSomeWay(
	recordSeries [][]Record,
	comesBefore func(self, other float64) bool,
	appender Appender) {
	preferActiveAppender := newPreferActiveAppender(appender)
	aHeapWithLess := &recordHeapTypeWithLess{
		recordHeapType: trimEmptyRecordLists(recordSeries),
		before:         comesBefore,
	}
	heap.Init(aHeapWithLess)
	for !aHeapWithLess.isEmpty() {
		alist := heap.Pop(aHeapWithLess).([]Record)
		if !preferActiveAppender.Append(&alist[0]) {
			return
		}
		alist = alist[1:]
		if len(alist) > 0 {
			heap.Push(aHeapWithLess, alist)
		}
	}
	preferActiveAppender.Flush()
}

type recordHeapType [][]Record

func (h recordHeapType) Len() int {
	return len(h)
}

func (h recordHeapType) Swap(i, j int) {
	h[j], h[i] = h[i], h[j]
}

func (h *recordHeapType) Push(x interface{}) {
	*h = append(*h, x.([]Record))
}

func (h *recordHeapType) Pop() interface{} {
	old := *h
	n := len(old)
	*h = old[0 : n-1]
	return old[n-1]
}

func (h recordHeapType) isEmpty() bool {
	return len(h) == 0
}

type recordHeapTypeWithLess struct {
	recordHeapType
	before func(lhs, rhs float64) bool
}

func (h *recordHeapTypeWithLess) Less(i, j int) bool {
	return h.before(
		h.recordHeapType[i][0].TimeStamp,
		h.recordHeapType[j][0].TimeStamp)
}

type preferActiveAppenderType struct {
	wrapped           Appender
	lastTs            float64
	inactive          Record
	inactivePopulated bool
	active            Record
	activePopulated   bool
}

func newPreferActiveAppender(wrapped Appender) *preferActiveAppenderType {
	return &preferActiveAppenderType{wrapped: wrapped}
}

func (a *preferActiveAppenderType) Flush() (result bool) {
	switch {
	case a.activePopulated:
		result = a.wrapped.Append(&a.active)
	case a.inactivePopulated:
		result = a.wrapped.Append(&a.inactive)
	default:
		result = true
	}
	a.activePopulated = false
	a.inactivePopulated = false
	return
}

func (a *preferActiveAppenderType) Append(r *Record) (result bool) {
	if r.TimeStamp != a.lastTs {
		result = a.Flush()
	} else {
		result = true
	}
	if r.Active {
		a.active = *r
		a.activePopulated = true
	} else {
		a.inactive = *r
		a.inactivePopulated = true
	}
	a.lastTs = r.TimeStamp
	return
}
