package store

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
// depending on how many timestamps in the timestamps array fall on it or
// immediately after it. For example, if this appender accepts value A
// with timestamp 1000 and value B with timestamp 1004 and value C with
// timestamp 1007 and the timestamp array contains:
// ..., 1000, 1002, 1003, 1008,... then this appender will emit:
// ...; A, 1000; A, 1002; A, 1003; C, 1008; ... to result
// Note that no B is emitted to result as there is no timestamp that falls
// on 1004 or immediately after 1004 but not before C's timestamp of 1007.
//
// The timestamps array must be sorted in ascending order and the returned
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
		// If we reached end of timestamp array, let caller know we
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
	// If we reached end ot timestamps, tell caller we are done accepting
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
