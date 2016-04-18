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
	EndpointId interface{}
	Info       *MetricInfo
	TimeStamp  float64
	// The equivalent of 0 for inactive flags.
	Value interface{}
	// true if active, false if an inactive flag
	Active bool
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
// Inactive flags appear as a value equivalent to 0 or "".
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
// Store instances may be safely used with multiple goroutines as long
// they don't call RegisterEndpoint.
type Store struct {
	byApplication map[interface{}]*timeSeriesCollectionType
	supplier      *pageQueueType
	metrics       *storeMetricsType
}

// NewStore returns a new Store instance.
// valueCountPerPage is how many values may be stored in a single page.
// pageCount is the number of pages in this store and remains constant.
// inactiveThreshhold is the minimum ratio (0-1) of pages that need to be
// in the high priority queue before they are reclaimed before the other
// pages.
// degree is the degree of the btrees (see github.com/Symantec/btree)
func NewStore(
	valueCountPerPage,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *Store {
	return &Store{
		byApplication: make(map[interface{}]*timeSeriesCollectionType),
		supplier: newPageQueueType(
			valueCountPerPage,
			pageCount,
			inactiveThreshhold,
			degree),
		metrics: newStoreMetricsType(),
	}
}

// ShallowCopy returns a shallow copy of this store. In an environment with
// multiple goroutines, a client can create a shallow copy to safely register
// more endpoints without creating data races.
func (s *Store) ShallowCopy() *Store {
	return s.shallowCopy()
}

// RegisterEndpoint registers a new endpoint.
func (s *Store) RegisterEndpoint(endpointId interface{}) {
	s.registerEndpoint(endpointId)
}

// AddBatch adds metric values.
// AddBatch returns the total number of metric values added including any
// inactive flags.
// AddBatch uses timestamp for all new values in metricList.
// timestamp is seconds since Jan 1, 1970 GMT.
// If a time series already in the given endpoint does not have a new value
// in metricList, then AddBatch marks that time series as inactive by adding
// an inactive flag to it. That time series remains inactive until a
// subsequent call to AddBatch gives it a new value.
// The store may reuse pages used by an inactive time series more quickly
// than other pages.
func (s *Store) AddBatch(
	endpointId interface{},
	timestamp float64,
	metricList trimessages.MetricList) int {
	return s.addBatch(endpointId, timestamp, metricList)
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
// Calling this covers any new store created by calling ShallowCopy on
// this instance.
func (s *Store) RegisterMetrics() error {
	return s.registerMetrics()
}

// MarkEndpointInactive marks all time series for given endpint as inactive by
// adding an inactive flag with given timestamp to each time series.
// timestamp is seconds since Jan 1, 1970 GMT.
// The store may reuse pages used by an inactive time series more quickly
// than other pages.
func (s *Store) MarkEndpointInactive(
	timestamp float64, endpointId interface{}) {
	s.markEndpointInactive(timestamp, endpointId)
}
