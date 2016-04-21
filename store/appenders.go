package store

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

func newMergeWithTimestamps(
	timestamps []float64, result Appender) *mergeWithTimestampsType {
	return &mergeWithTimestampsType{
		timestamps: timestamps,
		wrapped:    result}
}

func (m *mergeWithTimestampsType) Append(r *Record) bool {
	tsLen := len(m.timestamps)
	if !m.lastRecordSet {
		m.lastRecord = *r
		m.lastRecordSet = true
		return m.timestampIdx < tsLen
	}
	for m.timestampIdx < tsLen {
		timestamp := m.timestamps[m.timestampIdx]
		if timestamp >= r.TimeStamp {
			m.lastRecord = *r
			return true
		}
		m.lastRecord.TimeStamp = timestamp
		if !m.wrapped.Append(&m.lastRecord) {
			// Since are appender is bailing early, we don't
			// want Finalize to do anything
			m.lastRecordSet = false
			return false
		}
		m.timestampIdx++
	}
	return false
}

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
