// Package store handles storing metric values in memory.
package store

import (
	"errors"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

var (
	// AddBatch returns ErrInactive if endpoint is marked inactive.
	ErrInactive = errors.New("store: endpoint inactive.")
)

// MetricInfo represents the meta data for a metric
// MetricInfo instances are immutable by contract
type MetricInfo struct {
	path        string
	description string
	unit        units.Unit
	kind        types.Type
	subType     types.Type
	groupId     int
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

func (m *MetricInfo) SubType() types.Type {
	return m.subType
}

// The number of bits. Only set for Int and Uint, and Float metrics.
func (m *MetricInfo) Bits() int {
	if m.kind.UsesSubType() {
		return m.subType.Bits()
	}
	return m.kind.Bits()
}

// The group ID of this metric
func (m *MetricInfo) GroupId() int {
	return m.groupId
}

// ValuesAreEqual returns true if lhs is equivalent to rhs.
// lhs and rhs must be values from the metric having this information.
func (m *MetricInfo) ValuesAreEqual(lhs, rhs interface{}) bool {
	return m.valuesAreEqual(lhs, rhs)
}

// MetricInfoBuilder creates a brand new MetricInfo instance from scratch.
// Used for testing.
type MetricInfoBuilder struct {
	Path        string
	Description string
	Unit        units.Unit
	Kind        types.Type
	SubType     types.Type
	GroupId     int
}

func (m *MetricInfoBuilder) Build() *MetricInfo {
	return &MetricInfo{
		path:        m.Path,
		description: m.Description,
		unit:        m.Unit,
		kind:        m.Kind,
		subType:     m.SubType,
		groupId:     m.GroupId}
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

// Filterer filters metric values.
type Filterer interface {
	// Filter returns true to include passed metric value false otherwise.
	Filter(*Record) bool
}

// FiltererFunc is an adapter allowing an ordinary function to be used as a
// Filterer.
type FiltererFunc func(*Record) bool

func (f FiltererFunc) Filter(r *Record) bool {
	return f(r)
}

// Appender appends records fetched from a Store to this instance.
type Appender interface {
	// Append appends the contents record r to this instance.
	// Implementations must not hold onto the r pointer as the contents of
	// r may change between calls to Append.
	// Append should return false when it is done accepting records.
	Append(r *Record) bool
}

// AppendTo returns an Appender that appends Record instances to result.
func AppendTo(result *[]Record) Appender {
	return (*recordListType)(result)
}

// AppenderFilterFunc returns an Appender that appends only when the filter
// function returns true
func AppenderFilterFunc(wrapped Appender, filter func(*Record) bool) Appender {
	return &filterAppenderType{
		filter: FiltererFunc(filter), wrapped: wrapped}
}

// AppenderLimit returns an Appender that appends at most limit items if
// limit is positve. If limit is 0 or negative, returns wrapped.
func AppenderLimit(wrapped Appender, limit int) Appender {
	if limit > 0 {
		return &limitAppenderType{limit: limit, wrapped: wrapped}
	}
	return wrapped
}

// Visitor visits endpoints registered with a Store instance.
type Visitor interface {

	// Called once for each endpoint registered with the Store instance
	Visit(store *Store, endpoint interface{}) error
}

// Iterator iterates over metric values stored in scotty.
type Iterator interface {
	// Next stores the next metric value at r and advances this instance.
	// Next returns false if there are no more values.
	Next(r *Record) bool
}

// IteratorFilterFunc returns an Iterator like the given one except
// that it yields only metric values for which filter returns true.
func IteratorFilterFunc(
	iterator Iterator, filter func(*Record) bool) Iterator {
	return &filterIteratorType{
		filter:  FiltererFunc(filter),
		wrapped: iterator}
}

// IteratorFilter returns an Iterator like the given one except
// that it yields only metric values for which filter returns true.
func IteratorFilter(
	iterator Iterator, filter Filterer) Iterator {
	return &filterIteratorType{
		filter:  filter,
		wrapped: iterator}
}

// NamedIteratorFilterFunc returns an Iterator like the given one except
// that it yields only metric values for which filter returns true.
func NamedIteratorFilterFunc(
	ni NamedIterator, filter func(*Record) bool) NamedIterator {
	return &changedNamedIteratorType{
		NamedIterator: ni,
		change:        IteratorFilterFunc(ni, filter)}
}

// NamedIteratorFilter returns an Iterator like the given one except
// that it yields only metric values for which filter returns true.
func NamedIteratorFilter(
	ni NamedIterator, filter Filterer) NamedIterator {
	return &changedNamedIteratorType{
		NamedIterator: ni,
		change:        IteratorFilter(ni, filter)}
}

// NamedIterator iterates over metric values stored in scotty.
// It is safe to use NamedIterator while new values are being added to
// scotty.
//
// NamedIterator may report no more values before finishing.
// However, if the caller commits the progress, creating
// a new iterator with the same name to iterate over the same items will
// continue where the previous iterator left off. Put another way, the new
// iterator will not visit any of the values that the previous iterator
// visited.
//
// As the name is used to track progress, a caller should create at most one
// NamedIterator with a particular name at a time to iterate over the same
// group of items. If the caller creates multiple NamedIterator instances with
// the same name to iterate over the same group of items and calls Commit on
// all of them, the last iterator to save its progress wins overwriting the
// progress of the others with its own.
type NamedIterator interface {
	// Name returns the name of the iterator.
	Name() string
	// Next stores the next metric value at r and advances this instance.
	// Next returns false if either there are no more values or it is
	// time to commit.
	Next(r *Record) bool
	// Commit permanently stores the progress of this instance so that
	// creating an iterator with the same name to iterate over the same
	// items will continue where this instance left off.
	// If caller does not call commit and then creates another iterator
	// with the same name to iterate over the same items, that new
	// iterator will start in the same place as this iterator and
	// attempt to visit the same values.
	Commit()
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
// valueCount is how many timestamp value pairs can fit on a page.
// pageCount is the number of pages in this store and remains constant.
// inactiveThreshhold is the minimum ratio (0-1) of pages that need to be
// in the high priority queue before they are reclaimed before the other
// pages.
// degree is the degree of the btrees (see github.com/Symantec/btree)
func NewStore(
	valueCount,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *Store {
	return NewStoreBytesPerPage(
		valueCount*kTsAndValueSize,
		pageCount,
		inactiveThreshhold,
		degree)

}

// NewStoreBytesPerPage returns a new Store instance.
// bytesPerPage is the size of a single page in bytes.
// pageCount is the number of pages in this store and remains constant.
// inactiveThreshhold is the minimum ratio (0-1) of pages that need to be
// in the high priority queue before they are reclaimed before the other
// pages.
// degree is the degree of the btrees (see github.com/Symantec/btree)
func NewStoreBytesPerPage(
	bytesPerPage,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *Store {
	return &Store{
		byApplication: make(map[interface{}]*timeSeriesCollectionType),
		supplier: newPageQueueType(
			bytesPerPage,
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
// inactive flags. If endpoint is inactive, AddBatch returns err = ErrInactive.
// AddBatch uses timestamp for all new values in metricList.
// timestamp is seconds since Jan 1, 1970 GMT.
// If a time series already in the given endpoint does not have a new value
// in metricList, then AddBatch marks that time series as inactive by adding
// an inactive flag to it. That time series remains inactive until a
// subsequent call to AddBatch gives it a new value.
// The store may reuse pages used by an inactive time series more quickly
// than other pages.
//
// timestamp is the timestamp from scotty and is provided in case metricList
// is missing timestamps.
func (s *Store) AddBatch(
	endpointId interface{},
	timestamp float64,
	metricList metrics.List) (numAdded int, err error) {
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

// TimeLeft returns the the approximate maximum timespan measured in seconds
// of values left to be iterated from iterator with given name across all
// end points.
func (s *Store) TimeLeft(name string) (seconds float64) {
	return s.timeLeft(name)
}

// NamedIteratorForEndpoint returns an iterator for the given name that
// iterates over metric values for all known timestamps for the given endpoint.
// Although returned iterator iterates over values and timestamps in no
// particular order, it will iterate over values of the same metric by
// increasing timestamp.
//
// The second value returned is the approximate maximum timespan measured
// in seconds of the remaining values left to be iterated once returned
// iterator is exhausted. This second value is approximate because of
// concurrent access to the store with no exlusive lock. In particular, even
// if maxFrames = 0, this returned second value may be > 0.
//
// If maxFrames = 0, the returned iterator will make best effort to iterate
// over all the metric values in the endpoint. A positive maxFrames hints to
// the returned iterator that it should iterate over at most maxFrames values
// per metric. Returned iterator is free to iterate over fewer than maxFrames
// values for a metric even if that metric has additional values.
// Caller must commit progress and create a new iterator with the
// same name for the same endpoint to see the rest of the values.
func (s *Store) NamedIteratorForEndpoint(
	name string,
	endpointId interface{},
	maxFrames int) (
	iterator NamedIterator, remainingValuesInSeconds float64) {
	return s.namedIteratorForEndpoint(name, endpointId, maxFrames)
}

// NamedIteratorForEndpointRollUp works like NamedItgeratorForEndpoint except
// that instead of reporting every individual value for each metric, it
// reports summarised values by averaging / rolling up values for each time
// period.
//
// NamedIteratorForEndpointRollUp summarises values by averaging both the
// values and the timestamps of values for each time period.
// When averaging integer values, it rounds the average to the nearest integer.
// For non-numeric values such as strings, it summarises by reporting the
// first encountered value and timestamp in each time period.
// It ignores any inactive flags encountered when summarising values.
//
// For each metric, NamedIteratorForEndpointRollUp reports at most one
// value for each time period. If a metric has no values or has only
// inactive flag(s) for a given time period,
// NamedIteratorForEndpointRollUp reports no value for that metric during
// that time period.
//
// As described in the NamedIterator documentation, the name is used to
// track progress for iterating over the given endpoint. Therefore, caller
// should avoid having two iterators at once with the same name iterating
// over the same endpoint even if one iterator comes from this method while
// the other comes from NamedIteratorForEndpoint.
//
// dur is the length of the time periods. For example, dur = 5 * time.Minute
// means use time periods 12:00-12:05; 12:05-12:10; 12:15-12:20. Each time
// period includes the lower bound and excludes the upper bound. For example,
// 12:15-12:20 includes 12:15 but not 12:20. To avoid confusion, dur should
// evenly divide 1 minute or 1 hour. dur = 2*time.Minute, dur = 3*time.Minute,
// dur = 4*time.Minute, and dur = 6*time.Minute are all good choices.
// dur = 7*time.Minute is not.
//
// If maxFrames = 0, NamedIteratorForEndpointRollUp makes best effort to
// report the rest of the unreported summarised values. A positive maxFrames
// hints to the returned iterator that it should iterate over at most maxFrames
// time periods, not values, per metric.
//
// To avoid reporting summarised values for a time period before all the values
// are in, NamedIteratorForEndpointRollup will not report a summarised value
// for a given time period for a metric unless values for a later time period
// for that same metric are present.
func (s *Store) NamedIteratorForEndpointRollUp(
	name string,
	endpointId interface{},
	dur time.Duration,
	maxFrames int) (
	iterator NamedIterator, remainingValuesInSeconds float64) {
	return s.namedIteratorForEndpointRollUp(
		name, endpointId, dur, maxFrames)
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

// RegisterMetrics registers metrics under d associated with this Store
// instance Calling this covers any new store created by calling ShallowCopy on
// this instance.
func (s *Store) RegisterMetrics(d *tricorder.DirectorySpec) error {
	return s.registerMetrics(d)
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

// MarkEndpointActive marks given endpoint as active.
func (s *Store) MarkEndpointActive(endpointId interface{}) {
	s.markEndpointActive(endpointId)
}
