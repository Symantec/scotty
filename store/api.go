package store

import (
	"github.com/Symantec/scotty"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
)

// A Session instance allows a single goroutine to read records from a
// particular Store instance. To read from a Store instnace, a goroutine
// must first ask the Store instance for a session instance.
// When the goroutine presents its session instance to the
// Store instance, the Store instance provides read only pointers into its
// data. These pointers remain valid as long as the Session instance is
// opened. When done with the data, the goroutine must close its Session
// instance.
// Using a closed Session instance or using a Session instance created from
// one Store instance on a different Store instance results in undefined
// behavior.
type Session struct {
	readerLocks map[*readerLockType]bool
}

// IsNew returns true if this session is brand new.
// IsNew returns false if this session must be closed to release data.
func (s *Session) IsNew() bool {
	return s.isNew()
}

// Close closes this Session instance. Any data fetched using this
// session become invalid. Goroutines should close their session instances
// as soon as possible. Holding onto sessions indefinitly will eventually
// block attempts to write new data into the store.
func (s *Session) Close() error {
	return s._close()
}

// MetricInfo represents the meta data for a metric
type MetricInfo struct {
	path        string
	description string
	unit        units.Unit
	kind        types.Type
	bits        int
}

// Path returns the path of the metric
func (m *MetricInfo) Path() string {
	return m.path
}

// Description returns the description of the metric
func (m *MetricInfo) Description() string {
	return m.description
}

// Unit returns the unit of the metric
func (m *MetricInfo) Unit() units.Unit {
	return m.unit
}

// Kind returns the kind of the metric
func (m *MetricInfo) Kind() types.Type {
	return m.kind
}

// The number of bits. Only set for Int and Uint, and Float metrics.
func (m *MetricInfo) Bits() int {
	return m.bits
}

// Record represents one value for one particular metric at a particular
// time.
type Record struct {
	machineId *scotty.Machine
	info      *MetricInfo
	timestamp float64
	value     interface{}
}

// MachineId returns the ID of the machine with the metirc
func (r *Record) MachineId() *scotty.Machine {
	return r.machineId
}

// Info returns the data identifying the metric
func (r *Record) Info() *MetricInfo {
	return r.info
}

// Timestamp returns the timestamp of the value
func (r *Record) Timestamp() float64 {
	return r.timestamp
}

// Value returns the value.
func (r *Record) Value() interface{} {
	return r.value
}

// Appender appends records fetched from a Store to this instance.
type Appender interface {
	// Append appends record r to this instance.
	// Append must return true if it holds onto the pointer r.
	// Append should return false if does not.
	Append(r *Record) bool
}

// AppendTo creates an Appender that appends Record instance pointers
// to result. Note that the Append method of returned Appender always
// returns true
func AppendTo(result *[]*Record) Appender {
	return (*recordListType)(result)
}

// Visitor visits machines registered with a Store instance.
type Visitor interface {

	// Called once for each machine registered with the Store instance
	Visit(store *Store, machine *scotty.Machine) error
}

type Builder struct {
	buffers              map[*scotty.Machine]*circularBufferType
	bufferSizePerMachine int
	spacingPerMachine    int
	prevStore            *Store
}

// NewBuilder creates a new store builder.
// bufferSizePerMachine is the fixed size of each circular buffer as
// number of records.
// spacingPerMachine is the minimum amount of space allowed between the
// head and tail of each circular buffer as number of records.
// Therefore each circular buffer can store at most
// bufferSizePerMachine - spacingPerMachine records.
// NewBuilder panics if spacingPerMachine < 1 or
// spacingPerMachine >= bufferSizePerMachine.
func NewBuilder(bufferSizePerMachine, spacingPerMachine int) *Builder {
	if spacingPerMachine < 1 || spacingPerMachine >= bufferSizePerMachine-2 {
		panic("spacingPerMachine must be at least 1 and less than bufferSizePerMachine-2.")
	}
	return &Builder{
		buffers:              make(map[*scotty.Machine]*circularBufferType),
		bufferSizePerMachine: bufferSizePerMachine,
		spacingPerMachine:    spacingPerMachine,
	}
}

// RegisterMachine registers a machine with the store being built.
// RegisterMachine panics if machine is already registered.
func (b *Builder) RegisterMachine(machineId *scotty.Machine) {
	b.registerMachine(machineId)
}

// Build returns the built store and destroys this builder.
func (b *Builder) Build() *Store {
	return b.build()
}

// Store is an in memory store of metrics.
// For maximum write concurrency, store is a collection of circular buffers,
// one per machine. Client must register all the machines with the Store
// instance before storing any metrics.
type Store struct {
	buffers              map[*scotty.Machine]*circularBufferType
	bufferSizePerMachine int
	spacingPerMachine    int
}

// NewSession creates an opened session for reading data from this instance.
func (s *Store) NewSession() *Session {
	return &Session{readerLocks: make(map[*readerLockType]bool)}
}

// NewBuilder returns a Builder for creating a new store when the
// active machines change.
func (s *Store) NewBuilder() *Builder {
	return s.newBuilder()
}

// Add adds a metric value to this store. Add only stores a metric value
// if it has changed. Add returns true if the value for the metric changed
// or false otherwise.
// timestamp is seconds after Jan 1, 1970 GMT
func (s *Store) Add(
	machineId *scotty.Machine,
	timestamp float64, m *trimessages.Metric) bool {
	return s.add(machineId, timestamp, m)
}

// ByNameAndMachine returns records for a metric by path and machine and
// start and end times.
// ByNameAndMachine will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByNameAndMachine appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
// caller may pass nil for sess if the Append method of result never returns
// true.
func (s *Store) ByNameAndMachine(
	sess *Session,
	path string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byNameAndMachine(
		sess, path, machineId, start, end, result)
}

// ByPrefixAndMachine returns records for metrics by machine and
// start and end times with paths that start with prefix.
// ByPrefixAndMachine will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByPrefixAndMachine appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
// caller may pass nil for sess if the Append method of result never returns
// true.
func (s *Store) ByPrefixAndMachine(
	sess *Session,
	prefix string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byPrefixAndMachine(
		sess, prefix, machineId, start, end, result)
}

// ByMachine returns records for a metrics by machine and
// start and end times.
// ByMachine will go back just before start when possible so that
// the value of the metrics is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByMachine appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// The machines and metrics are in no particular order.
// caller may pas nil for sess if the Append method of result never returns
// true.
func (s *Store) ByMachine(
	sess *Session,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byMachine(sess, machineId, start, end, result)
}

// LatestByMachine returns the latest records for each metric for a
// given machine.
// LatestByMachine appends the records to result in no particular order.
// caller may pas nil for sess if the Append method of result never returns
// true.
func (s *Store) LatestByMachine(
	sess *Session,
	machineId *scotty.Machine,
	result Appender) {
	s.latestByMachine(sess, machineId, result)
}

// VisitAllMachines visits each machine registered with this instance.
// If v.Visit() returns a non-nil error, VisitAllMachines returns that
// same error immediately.
func (s *Store) VisitAllMachines(v Visitor) error {
	return s.visitAllMachines(v)
}
