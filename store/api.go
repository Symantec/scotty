// Package store handles storing metric values in memory.
package store

import (
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
	ApplicationId interface{}
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
	Visit(store *Store, endpoint interface{}) error
}

// Iterator iterates over values in one time series.
type Iterator struct {
	timeSeries *timeSeriesType
	values     []tsValueType
	advances   int
	skipped    int
}

// Info returns the metric info of the time series.
func (i *Iterator) Info() *MetricInfo {
	return i.timeSeries.id
}

// Next returns the next timestamp and value in the series.
// skipped indicates how many intermediate values had to be skipped because
// of memory being reclaimed. Next returns (0, nil, 0) to indicates it has
// no more values to emit. Next returning (0, nil, 0) does not necessarily
// mean that it has reached the latest value in the store.
func (i *Iterator) Next() (timestamp float64, value interface{}, skipped int) {
	return i.next()
}

// Commit indicates that we are done using this iterator and that the next
// Iterator for this time series should start where this one left off.
func (i *Iterator) Commit() {
	i.commit()
}

// Store is an in memory store of metrics.
// Client must register all the endpoints with the Store
// instance before storing any metrics.
type Store struct {
	byApplication map[interface{}]*timeSeriesCollectionType
	supplier      *pageQueueType
	metrics       *storeMetricsType
}

func NewStore(valueCountPerPage, pageCount int) *Store {
	return &Store{
		byApplication: make(map[interface{}]*timeSeriesCollectionType),
		supplier:      newPageQueueType(valueCountPerPage, pageCount, 0.1),
		metrics:       newStoreMetricsType(),
	}
}

func (s *Store) ShallowCopy() *Store {
	return s.shallowCopy()
}

func (s *Store) RegisterEndpoint(endpointId interface{}) {
	s.registerEndpoint(endpointId)
}

// AddBatch adds metric values.
// AddBatch returns the total number of metric values added.
// No two goroutines may call AddBatch() on a Store instance concurrently
// with the same endpointId. However multiple goroutines may call
// AddBatch() as long as long as each passes a different endpointId.
func (s *Store) AddBatch(
	endpointId interface{},
	timestamp float64,
	metricList trimessages.MetricList) int {
	return s.addBatch(endpointId, timestamp, metricList)
}

// TODO: Make so that Add does not change the priority of the pages of
// the other time series.
func (s *Store) Add(
	endpointId interface{},
	timestamp float64,
	metric *trimessages.Metric) bool {
	return s.addBatch(endpointId, timestamp, trimessages.MetricList{metric}) == 1
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
	endpointId interface{},
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
	endpointId interface{},
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
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byEndpoint(endpointId, start, end, result)
}

// Iterators returns all the Iterators for all the time series for the
// given endpoint.
func (s *Store) Iterators(endpointId interface{}) []*Iterator {
	return s.iterators(endpointId)
}

// LatestByEndpoint returns the latest records for each metric for a
// given endpoint.
// LatestByEndpoint appends the records to result in no particular order.
func (s *Store) LatestByEndpoint(
	endpointId interface{},
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
// this instance.
func (s *Store) RegisterMetrics() error {
	return s.registerMetrics()
}
