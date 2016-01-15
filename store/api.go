package store

import (
	"github.com/Symantec/scotty"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
)

// MetricInfo represents the meta data for a metric
// MetricInfo instances are immutable by contract
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
	ApplicationId *scotty.Machine
	Info          *MetricInfo
	TimeStamp     float64
	Value         interface{}
}

// Appender appends records fetched from a Store to this instance.
type Appender interface {
	// Append appends the contents record r to this instance.
	// Implementations must not hold onto r as the contents of r
	// may change between calls to Append.
	Append(r *Record)
}

// AppendTo creates an Appender that appends Record instance pointers
// to result. The Append method of returned Appender stores a copy of
// passed in Record.
func AppendTo(result *[]*Record) Appender {
	return (*recordListType)(result)
}

// Visitor visits machines registered with a Store instance.
type Visitor interface {

	// Called once for each machine registered with the Store instance
	Visit(store *Store, machine *scotty.Machine) error
}

type Builder struct {
	byApplication    map[*scotty.Machine]*timeSeriesCollectionType
	supplier         pageSupplierType
	totalPageCount   int
	maxValuesPerPage int
	prevStore        *Store
}

// NewBuilder creates a new store builder.
func NewBuilder(
	valueCountPerPage, pageCount int) *Builder {
	return &Builder{
		byApplication:    make(map[*scotty.Machine]*timeSeriesCollectionType),
		supplier:         newPageSupplierType(valueCountPerPage, pageCount),
		totalPageCount:   pageCount,
		maxValuesPerPage: valueCountPerPage,
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
// Client must register all the machines with the Store
// instance before storing any metrics.
type Store struct {
	byApplication    map[*scotty.Machine]*timeSeriesCollectionType
	supplier         pageSupplierType
	totalPageCount   int
	maxValuesPerPage int
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
// No two goroutines may call Add() on a Store instance concurrently with the
// same machineId. However multiple goroutines may call Add() as long as
// long as each passes a different machineId.
func (s *Store) Add(
	machineId *scotty.Machine,
	timestamp float64, m *trimessages.Metric) bool {
	return s.add(machineId, timestamp, m)
}

// AddBatch works like Add but adds several metric values at once.
// If filter is non-nill, AddBatch ignores any metrics in metricList for
// which filter returns false.
// AddBatch returns the total number of metric values added.
// No two goroutines may call AddBatch() on a Store instance concurrently
// with the same machineId. However multiple goroutines may call
// AddBatch() as long as long as each passes a different machineId.
func (s *Store) AddBatch(
	machineId *scotty.Machine,
	timestamp float64,
	metricList trimessages.MetricList,
	filter func(*trimessages.Metric) bool) int {
	return s.addBatch(machineId, timestamp, metricList, filter)
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
func (s *Store) ByNameAndMachine(
	path string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byNameAndMachine(
		path, machineId, start, end, result)
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
func (s *Store) ByPrefixAndMachine(
	prefix string,
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byPrefixAndMachine(
		prefix, machineId, start, end, result)
}

// ByMachine returns records for a metrics by machine and
// start and end times.
// ByMachine will go back just before start when possible so that
// the value of the metrics is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByMachine appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// The machines and metrics are in no particular order.
func (s *Store) ByMachine(
	machineId *scotty.Machine,
	start, end float64,
	result Appender) {
	s.byMachine(machineId, start, end, result)
}

// LatestByMachine returns the latest records for each metric for a
// given machine.
// LatestByMachine appends the records to result in no particular order.
func (s *Store) LatestByMachine(
	machineId *scotty.Machine,
	result Appender) {
	s.latestByMachine(machineId, result)
}

// VisitAllMachines visits each machine registered with this instance.
// If v.Visit() returns a non-nil error, VisitAllMachines returns that
// same error immediately.
func (s *Store) VisitAllMachines(v Visitor) error {
	return s.visitAllMachines(v)
}

// RegisterMetrics registers metrics associated with this Store instance
// Calling this covers any new store created by calling NewBuilder() on
// this instance.
func (s *Store) RegisterMetrics() {
	s.registerMetrics()
}
