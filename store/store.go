package store

import (
	"github.com/Symantec/scotty"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"strings"
	"sync"
)

func (r *Record) isZero() bool {
	return r.machineId == nil
}

func (s *Session) _close() error {
	for lock, ok := range s.readerLocks {
		if ok {
			lock.Unlock()
		}
	}
	return nil
}

func (s *Session) isNew() bool {
	return len(s.readerLocks) == 0
}

// metricInfoStoreType keeps a pool of pointers to unique MetricInfo instances
type metricInfoStoreType struct {
	ByInfo map[MetricInfo]*MetricInfo
	ByName map[string][]*MetricInfo
}

func newMetricInfoStore() *metricInfoStoreType {
	return &metricInfoStoreType{
		ByInfo: make(map[MetricInfo]*MetricInfo),
		ByName: make(map[string][]*MetricInfo)}
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

type linkedRecordType struct {
	Record

	// Points to the record for the same metric that comes just
	// befor this one
	Ptr int64
}

// earliestRecordByMetricType maps a MetricInfo pointer to the earliest
// known record for that metric.
// The pointer in this earliest record points to the latest record.
type earliestRecordByMetricType map[*MetricInfo]*linkedRecordType

// readerLockType represents a reader lock.
// Multiple readers can hold a readerLockType lock.
// A reader lock uses the mutex of its enclosing circularBufferType
// instance. Caller must hold this mutex from enclosing instance
// to call lower case methods; caller must not hold mutex from enclosing
// instance to call capitalized methods.
type readerLockType struct {
	readers     int
	zeroReached sync.Cond
}

func (r *readerLockType) Unlock() {
	r.zeroReached.L.Lock()
	defer r.zeroReached.L.Unlock()
	if r.readers == 0 {
		panic("Already at 0")
	}
	r.readers--
	if r.readers == 0 {
		r.zeroReached.Broadcast()
	}
}

// Initializes this lock with mutex from enclosing circularBufferType.
func (r *readerLockType) init(l sync.Locker) {
	r.zeroReached.L = l
}

// wait blocks caller until nobody else has the lock.
func (r *readerLockType) wait() {
	for r.readers > 0 {
		r.zeroReached.Wait()
	}
}

func (r *readerLockType) lock() {
	r.readers++
}

type recordListType []*Record

func (l *recordListType) Append(r *Record) bool {
	*l = append(*l, r)
	return true
}

// circularBufferType represents a circular buffer of metrics.
// Caller must hold this instance's mutex to call lowercase methods;
// Caller must not hold this instance's mutex to call capitalized methods.
type circularBufferType struct {
	buffer    []linkedRecordType
	bufferLen int64

	// 2 * lockChunkSize -1 is the minimum spacing between the head and
	// tail of the circular queue.
	lockChunkSize int64

	// This lock protects everything below
	lock sync.Mutex

	// head pointer. always increases from 0.
	head int64

	// tail pointer. always increases from 0
	tail int64

	// locks for readers. readers reading from tail where
	// 2k * lockChunkSize <= tail < (2k+1) * lockChunkSize lock [0];
	// readers reading from tail where
	// (2k+1) * lockChunkSize <= tail < 2k * lockChunkSize lock [1].
	// The lock with tail is the active lock. This is the lock
	// issued to all new readers. The other lock is the inactive lock.
	// The inactive lock is always being drained. It is never issued
	// to new readers while readers that acquired it previously
	// eventually unlock it.
	readerLocks [2]readerLockType

	earliestRecords earliestRecordByMetricType
	metricInfoStore *metricInfoStoreType
}

// newCircularBufferType creates a new circular buffer
// size is the fixed size of the buffer; spacing is the minimum
// spacing between the head and tail of the buffer. spacing must
// be at least 1. Make spacing large enough that goroutines reading
// from the tail of the buffer finish before head overwrites what
// was at the tail.
func newCircularBufferType(size, spacing int) *circularBufferType {
	var result circularBufferType
	result.buffer = make([]linkedRecordType, size)
	result.bufferLen = int64(size)
	result.lockChunkSize = int64(spacing/2) + 1
	result.head = 0
	result.tail = 0
	result.readerLocks[0].init(&result.lock)
	result.readerLocks[1].init(&result.lock)
	result.earliestRecords = make(earliestRecordByMetricType)
	result.metricInfoStore = newMetricInfoStore()
	return &result
}

// Add adds a record to this circular buffer.
// Returns true if Add added a record or false if Add did not add a
// record because the value remained unchanged.
func (c *circularBufferType) Add(
	machineId *scotty.Machine,
	timestamp float64,
	m *trimessages.Metric) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	info := c.metricInfoStore.Register(m)
	lastValue, lastTimestamp := c.getLatestValueAndTimestamp(info)
	if timestamp <= lastTimestamp {
		panic("Timestamps must be ever increasing.")
	}
	if lastValue != m.Value {
		c.advanceHead()

		// Now we have to write where head was.
		newRec := c.get(c.head - 1)
		newRec.machineId = machineId
		newRec.info = info
		newRec.timestamp = timestamp
		newRec.value = m.Value
		newRec.Ptr = c.updateLatestRecordPtr(info, c.head-1)
		return true
	}
	return false
}

// ByName returns the records for metric(s) with given path.
// ByName appends the records to result.
func (c *circularBufferType) ByName(
	sess *Session,
	path string,
	start, end float64,
	result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	var recordsAdded bool
	infoList := c.metricInfoStore.ByName[path]
	for _, info := range infoList {
		if c.fetch(info, start, end, result) {
			recordsAdded = true
		}
	}
	if recordsAdded {
		c.obtainReadLock(sess)
	}
}

// ByPrefix returns the records for metric(s) with given path.
// ByPrefix appends the records to result.
func (c *circularBufferType) ByPrefix(
	sess *Session,
	prefix string,
	start, end float64,
	result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	var recordsAdded bool
	for _, info := range c.metricInfoStore.ByInfo {
		if strings.HasPrefix(info.Path(), prefix) {
			if c.fetch(info, start, end, result) {
				recordsAdded = true
			}
		}
	}
	if recordsAdded {
		c.obtainReadLock(sess)
	}
}

// Latest returns the latest records for all metrics.
// Latest appends the records to result.
func (c *circularBufferType) Latest(sess *Session, result Appender) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var recordsAdded bool
	for _, info := range c.metricInfoStore.ByInfo {
		earliest := c.earliestRecords[info]
		if earliest == nil {
			continue
		}
		arecord := c.get(earliest.Ptr)
		if arecord != nil {
			if result.Append(&arecord.Record) {
				recordsAdded = true
			}
		} else {
			// We have to copy because the read locks only protect
			// the records in the circular buffer not the ones in
			// the EarliestRecords map.
			copyOfEarliest := earliest.Record
			if result.Append(&copyOfEarliest) {
				recordsAdded = true
			}
		}
	}
	if recordsAdded {
		c.obtainReadLock(sess)
	}
}

// obtainReadLock obtains read lock from current tail for s.
// If s already has the lock, obatinReadLock is a no-op.
func (c *circularBufferType) obtainReadLock(s *Session) {
	rLock := c.readerLock(c.tail)
	if !s.readerLocks[rLock] {
		rLock.lock()
		s.readerLocks[rLock] = true
	}
}

// get returns the record in this instance corresponding to ptr.
// If ptr is negative or ptr < tail or ptr >= head returns nil.
func (c *circularBufferType) get(ptr int64) *linkedRecordType {
	if ptr < 0 || ptr < c.tail || ptr >= c.head {
		return nil
	}
	return &c.buffer[ptr%c.bufferLen]
}

// getLatestValueAndTimestamp returns the last recorded value for given metric.
// along with its timestamp. If there is no latest value, returns nil and 0.0.
func (c *circularBufferType) getLatestValueAndTimestamp(
	info *MetricInfo) (value interface{}, timestamp float64) {
	earliest := c.earliestRecords[info]
	if earliest == nil {
		return
	}
	arecord := c.get(earliest.Ptr)
	if arecord == nil {
		return earliest.Value(), earliest.Timestamp()
	}
	return arecord.Value(), arecord.Timestamp()
}

// As adding to this circular buffer overwrites old values with new ones,
// updatEarlist updates the earliest known record for a given metric
// to be the contents of rec.
func (c *circularBufferType) updateEarliest(rec *Record) {
	c.earliestRecords[rec.Info()].Record = *rec
}

// fetch fetches all the records for a given metric in descending order
// and appends them to output. fetch() goes just before start when
// possible so that the value of the metric is known at time start.
func (c *circularBufferType) fetch(
	metric *MetricInfo,
	start, end float64,
	output Appender) (fetched bool) {
	if start >= end {
		return
	}
	earliest := c.earliestRecords[metric]
	if earliest == nil {
		return
	}
	// Earliest record points to latest record
	arecord := c.get(earliest.Ptr)
	for arecord != nil && arecord.Timestamp() > start {
		if arecord.Timestamp() < end {
			if output.Append(&arecord.Record) {
				fetched = true
			}
		}
		arecord = c.get(arecord.Ptr)
	}
	if arecord != nil {
		// We know arecord.Timestamp() < end
		if output.Append(&arecord.Record) {
			fetched = true
		}
	} else if !earliest.isZero() {
		if earliest.Timestamp() < end {
			// We have to copy because the read locks only protect
			// the records in the circular buffer not the ones in
			// the EarliestRecords map.
			copyOfEarliest := earliest.Record
			if output.Append(&copyOfEarliest) {
				fetched = true
			}
		}
	}
	return
}

// advanceTail advances the tail of the buffer by 1. Each time it advances
// the tail, it updates earliestRecordIndex with the record
// that just fell off the buffer.
func (c *circularBufferType) advanceTail() {
	c.updateEarliest(&c.get(c.tail).Record)
	c.tail++
}

// advanceHead advances the head pointer of this circular buffer.
// advanceHead will block if it can't advance the head pointer
// because there are readers still using data.
func (c *circularBufferType) advanceHead() {

	// overwritePtr is where the head pointer will overwrite
	// It will be negative the first time around the circular buffer.
	overwritePtr := c.head - c.bufferLen

	// When overwritePtr % c.lockChunkSize == 0, we are crossing a
	// reader lock boundary. We need to wait on any outstanding
	// readers to finish before advancing head.
	if overwritePtr >= 0 && overwritePtr%c.lockChunkSize == 0 {
		c.readerLock(overwritePtr).wait()
	}
	// Advance tail so that it is 2*lockChunkSize away from where
	// we are writing. This way if we are crossing a read
	// lock boundary, we will start to drain the next read lock
	// boundary of readers so that it will likely be available
	// for writing when we get to it.
	for c.tail < overwritePtr+2*c.lockChunkSize {
		c.advanceTail()
	}
	// Finally advance head
	c.head++
}

// updateLatestRecordPtr updates the pointer to the latest record for
// a particular metric and returns the previous pointer or -1 if there
// was no previous pointer.
func (c *circularBufferType) updateLatestRecordPtr(
	info *MetricInfo, ptr int64) int64 {
	earliestRecord := c.earliestRecords[info]
	if earliestRecord == nil {
		earliestRecord = &linkedRecordType{Ptr: -1}
		c.earliestRecords[info] = earliestRecord
	}
	result := earliestRecord.Ptr
	earliestRecord.Ptr = ptr
	return result
}

// readerLock returns the readerLock corresponding to a particular
// tail.
func (c *circularBufferType) readerLock(tail int64) *readerLockType {
	return &c.readerLocks[(tail/c.lockChunkSize)%2]
}

func (b *Builder) registerMachine(machineId *scotty.Machine) {
	if b.buffers[machineId] != nil {
		panic("Machine already registered")
	}
	var buffer *circularBufferType
	if b.prevStore != nil {
		buffer = b.prevStore.buffers[machineId]
	}
	if buffer == nil {
		buffer = newCircularBufferType(b.bufferSizePerMachine, b.spacingPerMachine)
	}
	b.buffers[machineId] = buffer
}

func (b *Builder) build() *Store {
	buffers := b.buffers
	b.buffers = nil
	return &Store{
		buffers:              buffers,
		bufferSizePerMachine: b.bufferSizePerMachine,
		spacingPerMachine:    b.spacingPerMachine}
}

func (s *Store) newBuilder() *Builder {
	return &Builder{
		buffers:              make(map[*scotty.Machine]*circularBufferType),
		bufferSizePerMachine: s.bufferSizePerMachine,
		spacingPerMachine:    s.spacingPerMachine,
		prevStore:            s}
}

func (s *Store) add(
	machineId *scotty.Machine,
	timestamp float64,
	m *trimessages.Metric) bool {
	return s.buffers[machineId].Add(machineId, timestamp, m)
}

func (s *Store) byNameAndMachine(
	sess *Session,
	name string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.buffers[machineId].ByName(sess, name, start, end, result)
}

func (s *Store) byPrefixAndMachine(
	sess *Session,
	prefix string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.buffers[machineId].ByPrefix(sess, prefix, start, end, result)
}

func (s *Store) byMachine(
	sess *Session,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.buffers[machineId].ByPrefix(sess, "", start, end, result)
}

func (s *Store) latestByMachine(
	sess *Session,
	machineId *scotty.Machine,
	result Appender) {
	s.buffers[machineId].Latest(sess, result)
}

func (s *Store) visitAllMachines(v Visitor) (err error) {
	for machineId := range s.buffers {
		if err = v.Visit(s, machineId); err != nil {
			return
		}
	}
	return
}
