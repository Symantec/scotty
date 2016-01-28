// Package store handles storing metric values in memory.
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
	ApplicationId *scotty.Endpoint
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

// Visitor visits endpoints registered with a Store instance.
type Visitor interface {

	// Called once for each endpoint registered with the Store instance
	Visit(store *Store, endpoint *scotty.Endpoint) error
}

type Builder struct {
	store     **Store
	prevStore *Store
}

// NewBuilder creates a new store builder.
func NewBuilder(
	valueCountPerPage, pageCount int) *Builder {
	store := &Store{
		byApplication:    make(map[*scotty.Endpoint]*timeSeriesCollectionType),
		supplier:         newPageSupplierType(valueCountPerPage, pageCount),
		totalPageCount:   pageCount,
		maxValuesPerPage: valueCountPerPage,
	}

	return &Builder{store: &store}
}

// RegisterEndpoint registers a endpoint with the store being built.
// RegisterEndpoint panics if endpoint is already registered.
func (b *Builder) RegisterEndpoint(endpointId *scotty.Endpoint) {
	b.registerEndpoint(endpointId)
}

// Build returns the built store and destroys this builder.
func (b *Builder) Build() *Store {
	return b.build()
}

// Store is an in memory store of metrics.
// Client must register all the endpoints with the Store
// instance before storing any metrics.
type Store struct {
	byApplication    map[*scotty.Endpoint]*timeSeriesCollectionType
	supplier         *pageSupplierType
	totalPageCount   int
	maxValuesPerPage int
}

// NewBuilder returns a Builder for creating a new store when the
// active endpoints change.
func (s *Store) NewBuilder() *Builder {
	return s.newBuilder()
}

// Add adds a metric value to this store. Add only stores a metric value
// if it has changed. Add returns true if the value for the metric changed
// or false otherwise.
// timestamp is seconds after Jan 1, 1970 GMT
// No two goroutines may call Add() on a Store instance concurrently with the
// same endpointId. However multiple goroutines may call Add() as long as
// long as each passes a different endpointId.
func (s *Store) Add(
	endpointId *scotty.Endpoint,
	timestamp float64, m *trimessages.Metric) bool {
	return s.add(endpointId, timestamp, m, nil)
}

// AddBatch works like Add but adds several metric values at once.
// If filter is non-nil, AddBatch ignores any metrics in metricList for
// which filter returns false.
// If caller supplies a non-nil callback function, AddBatch calls it for
// each metric successfully added.
// AddBatch returns the total number of metric values added.
// No two goroutines may call AddBatch() on a Store instance concurrently
// with the same endpointId. However multiple goroutines may call
// AddBatch() as long as long as each passes a different endpointId.
func (s *Store) AddBatch(
	endpointId *scotty.Endpoint,
	timestamp float64,
	metricList trimessages.MetricList,
	filter func(*trimessages.Metric) bool,
	callback func(r *Record)) int {
	return s.addBatch(endpointId, timestamp, metricList, filter, callback)
}

// ByNameAndEndpoint returns records for a metric by path and endpoint and
// start and end times.
// ByNameAndEndpoint will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByNameAndEndpoint appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
func (s *Store) ByNameAndEndpoint(
	path string,
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byNameAndEndpoint(
		path, endpointId, start, end, result)
}

// ByPrefixAndEndpoint returns records for metrics by endpoint and
// start and end times with paths that start with prefix.
// ByPrefixAndEndpoint will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByPrefixAndEndpoint appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
func (s *Store) ByPrefixAndEndpoint(
	prefix string,
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byPrefixAndEndpoint(
		prefix, endpointId, start, end, result)
}

// ByEndpoint returns records for a metrics by endpoint and
// start and end times.
// ByEndpoint will go back just before start when possible so that
// the value of the metrics is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByEndpoint appends the records to result first grouped by metric
// then sorted by time in descending order within each metric.
// The endpoints and metrics are in no particular order.
func (s *Store) ByEndpoint(
	endpointId *scotty.Endpoint,
	start, end float64,
	result Appender) {
	s.byEndpoint(endpointId, start, end, result)
}

// LatestByEndpoint returns the latest records for each metric for a
// given endpoint.
// LatestByEndpoint appends the records to result in no particular order.
func (s *Store) LatestByEndpoint(
	endpointId *scotty.Endpoint,
	result Appender) {
	s.latestByEndpoint(endpointId, result)
}

// VisitAllEndpoints visits each endpoint registered with this instance.
// If v.Visit() returns a non-nil error, VisitAllEndpoints returns that
// same error immediately.
func (s *Store) VisitAllEndpoints(v Visitor) error {
	return s.visitAllEndpoints(v)
}

// RegisterMetrics registers metrics associated with this Store instance
// Calling this covers any new store created by calling NewBuilder() on
// this instance. It is an error to call RegisterMetrics more than once.
func (s *Store) RegisterMetrics() error {
	return s.registerMetrics()
}

// AvailablePages returns the number of pages available for collecting
// new metrics. This count incudes pages that are currently in use but
// are eligible to be recycled.
func (s *Store) AvailablePages() int {
	return s.supplier.Len()
}
