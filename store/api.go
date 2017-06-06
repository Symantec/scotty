// Package store handles storing metric values in memory.
package store

import (
	"errors"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

var (
	// AddBatch returns ErrInactive if endpoint is marked inactive.
	ErrInactive = errors.New("store: endpoint inactive.")
)

// DistributionTotals represents the distribution count at a given timestamp
// last recorded DistributionDelta. Instances of this type must be treated
// as immutable.
type DistributionTotals struct {
	Counts        []uint64
	Sum           float64
	RollOverCount uint64
}

// Count returns the total number of values this instance represents.
func (d *DistributionTotals) Count() (result uint64) {
	for _, count := range d.Counts {
		result += count
	}
	return
}

// Ranges depicts the ranges for a distributions.
// Instances of this type must be treated as immutable
type Ranges struct {
	// The upper limits of the buckets.
	UpperLimits []float64
}

// MetricInfo represents the meta data for a metric
// MetricInfo instances are immutable by contract
// MetricInfo instances must always support equality
type MetricInfo struct {
	path        string
	description string
	unit        units.Unit
	kind        types.Type
	subType     types.Type
	// A pointer so that MetricInfo can be a map key. If the path field
	// differs between two structs, this field will differ also even
	// if the ranges are logically equal. However, if the path field
	// is equal between two structs, this field will also be equal if
	// and only if the ranges are logically equivalent.
	ranges          *Ranges
	isNotCumulative bool
	groupId         int
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

// Ranges returns the ranges of the distribution when Kind() == types.Dist,
// otherwise returns nil.
func (m *MetricInfo) Ranges() *Ranges {
	return m.ranges
}

// IsNotCumulative returns true when Kind() == types.Dist and distribution
// is not cumulative. Returns false in all other cases.
func (m *MetricInfo) IsNotCumulative() bool {
	return m.isNotCumulative
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

// Key returns the key for this instance. Two MetricInfo instances return
// equal keys if and only if they are the same except for their group id.
func (m *MetricInfo) Key() interface{} {
	var result MetricInfoBuilder
	result.Init(m)
	result.GroupId = 0
	return result
}

// MetricInfoBuilder creates a brand new MetricInfo instance from scratch.
// Used for testing.
type MetricInfoBuilder struct {
	Path            string
	Description     string
	Unit            units.Unit
	Kind            types.Type
	SubType         types.Type
	Ranges          *Ranges
	IsNotCumulative bool
	GroupId         int
}

// Init initializes this builder from an existing MetricInfo instance.
func (m *MetricInfoBuilder) Init(info *MetricInfo) {
	m.Path = info.Path()
	m.Description = info.Description()
	m.Unit = info.Unit()
	m.Kind = info.Kind()
	m.SubType = info.SubType()
	m.Ranges = info.ranges
	m.IsNotCumulative = info.isNotCumulative
	m.GroupId = info.GroupId()
}

func (m *MetricInfoBuilder) Build() *MetricInfo {
	return &MetricInfo{
		path:            m.Path,
		description:     m.Description,
		unit:            m.Unit,
		kind:            m.Kind,
		subType:         m.SubType,
		ranges:          m.Ranges,
		isNotCumulative: m.IsNotCumulative,
		groupId:         m.GroupId}
}

// MetricGroupingStrategy type describes the strategy for grouping like metrics
// together. Two metrics are grouped together if and only if this instance
// returns equal values for each metric's MetricInfo field.
type MetricGroupingStrategy func(*MetricInfo) interface{}

var (
	// Groups metrics together if and only if their *MetricInfo field are
	// exactly the same.
	GroupMetricExactly MetricGroupingStrategy = func(m *MetricInfo) interface{} {
		return m
	}

	// Groups metrics together if and only if calling Key() on
	// their *MetricInfo field yields the same result.
	GroupMetricByKey MetricGroupingStrategy = func(m *MetricInfo) interface{} {
		return m.Key()
	}

	// Groups metrics together by their path and whether or not they
	// are numeric
	GroupMetricByPathAndNumeric MetricGroupingStrategy = func(
		m *MetricInfo) interface{} {
		return pathAndNumeric{
			Path:      m.Path(),
			IsNumeric: m.Kind().CanToFromFloat(),
		}
	}
)

// Equal returns true if and only if this strategy would group two metrics
// with the given MetricInfo fields together.
func (m MetricGroupingStrategy) Equal(
	maybeNilLeft, maybeNilRight *MetricInfo) bool {
	if maybeNilLeft == maybeNilRight {
		return true
	}
	if maybeNilLeft == nil || maybeNilRight == nil {
		return false
	}
	return m(maybeNilLeft) == m(maybeNilRight)
}

// Record represents one value for one particular metric at a particular
// time.
type Record struct {
	EndpointId interface{}
	Info       *MetricInfo
	TimeStamp  float64
	// The equivalent of 0 for inactive flags.
	// If Value contains a reference type, as is the case for lists or
	// distributions, client code must treat what it references as
	// immutable.
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

type AppendFlusher interface {
	Appender
	// Caller must call this after appending all values even if Append
	// returns false
	Flush()
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

// IteratorCoordinate returns an Iterator like given one except that it
// waits to emit each record until it can obtain a lease for a time range
// including that record. minLeaseSpanInSeconds is the minimum size of a
// lease and must be positive. skipped is a pointer to a uint64 that gets
// incremented each time a record is skipped.
func IteratorCoordinate(
	iterator Iterator,
	coord Coordinator,
	minLeaseSpanInSeconds float64,
	skipped *uint64) Iterator {
	if minLeaseSpanInSeconds <= 0.0 {
		panic(minLeaseSpanInSeconds)
	}
	return &coordinatorIteratorType{
		coordinator: coord,
		wrapped:     iterator,
		leaseSpan:   minLeaseSpanInSeconds,
		skipped:     skipped,
	}
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

// NamedIteratorCoordinate returns an Iterator like given one except that
// it waits to emit each record until it can obtain a lease for a time
// range including that record. minLeaseSpanInSeconds is the minimum size
// of a lease and must be positive. Calling Commit on returned result
// calls updateSkipped with the number of records skipped since the last
// commit.
func NamedIteratorCoordinate(
	ni NamedIterator,
	coord Coordinator,
	minLeaseSpanInSeconds float64,
	updateSkipped func(uint64)) NamedIterator {
	result := &coordinatorNamedIteratorType{
		wrapped:       ni,
		updateSkipped: updateSkipped,
	}
	result.change = IteratorCoordinate(
		ni, coord, minLeaseSpanInSeconds, &result.skippedSoFar)
	return result
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
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *Store {
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
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *Store {
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

// LessenPageCount reduces the number of pages this store uses by ratio, a
// value between 0.0 and 1.0 exclusive. If the store is using 1 million pages
// and ratio is 0.1, then LessenPageCount will remove 100K pages from the
// system leaving only 900K pages once it returns.
//
// For now we don't use this in our current memory management strategy.
// TODO: See if it is safe to remove this code.
func (s *Store) LessenPageCount(ratio float64) {
	s.supplier.LessenPageCount(ratio)
}

// FreeUpBytes attempts to free up bytesToFree bytes by releasing pages.
func (s *Store) FreeUpBytes(bytesToFree uint64) {
	s.supplier.FreeUpBytes(bytesToFree)
}

// SetExpanding controls wither or not the store allocates new pages as
// needed (true) or tries to recycle existing pages (false)
func (s *Store) SetExpanding(expanding bool) {
	s.supplier.SetExpanding(expanding)
}

// IsExpanding returns true if the store allocates new pages as needed or
// false if it recycles existing pages.
func (s *Store) IsExpanding() bool {
	return s.supplier.IsExpanding()
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
	metricList metrics.List) (numAdded uint, err error) {
	return s.addBatch(endpointId, timestamp, metricList)
}

// ByNameAndEndpoint returns records for a metric by path and endpoint and
// start and end times.
// ByNameAndEndpoint will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByNameAndEndpoint appends the records to result first grouped by metric
// exactly then sorted by time in descending order within each group.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
func (s *Store) ByNameAndEndpoint(
	path string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byNameAndEndpoint(
		path,
		endpointId,
		start,
		end,
		GroupMetricExactly,
		result)
}

// ByNameAndEndpointStrategy work like ByNameAndEndpoint except that it appends
// records grouped by given strategy sorted from latest to earliest.
func (s *Store) ByNameAndEndpointStrategy(
	path string,
	endpointId interface{},
	start, end float64,
	strategy MetricGroupingStrategy,
	result Appender) {
	s.byNameAndEndpoint(
		path,
		endpointId,
		start,
		end,
		strategy,
		result)
}

// ByPrefixAndEndpoint returns records for metrics by endpoint and
// start and end times with paths that start with prefix.
// ByPrefixAndEndpoint will go back just before start when possible so that
// the value of the metric is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByPrefixAndEndpoint appends the records to result first grouped by metric
// exactly then sorted by time in descending order within each group.
// It is possible, but unlikely, that two different metrics exist with the
// same path. This could happen if the definition of a metric changes.
func (s *Store) ByPrefixAndEndpoint(
	prefix string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byPrefixAndEndpoint(
		prefix,
		endpointId,
		start,
		end,
		GroupMetricExactly,
		result)
}

// ByPrefixAndEndpointStrategy work like ByPrefixAndEndpoint except that it
// appends records grouped by given strategy sorted from latest to earliest.
func (s *Store) ByPrefixAndEndpointStrategy(
	prefix string,
	endpointId interface{},
	start, end float64,
	strategy MetricGroupingStrategy,
	result Appender) {
	s.byPrefixAndEndpoint(
		prefix,
		endpointId,
		start,
		end,
		strategy,
		result)
}

// ByEndpoint returns records for a metrics by endpoint and
// start and end times.
// ByEndpoint will go back just before start when possible so that
// the value of the metrics is known at time start.
// start and end are seconds after Jan 1, 1970 GMT
// ByEndpoint appends the records to result first grouped by metric exactly
// then sorted by time in descending order within each metric.
func (s *Store) ByEndpoint(
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byEndpoint(
		endpointId,
		start,
		end,
		GroupMetricExactly,
		result)
}

// ByEndpointStrategy work like ByEndpoint except that it
// appends records grouped by given strategy sorted from latest to earliest.
func (s *Store) ByEndpointStrategy(
	endpointId interface{},
	start, end float64,
	strategy MetricGroupingStrategy,
	result Appender) {
	s.byEndpoint(
		endpointId,
		start,
		end,
		strategy,
		result)
}

// TimeLeft returns the the approximate maximum timespan measured in seconds
// of values left to be iterated from iterator with given name across all
// end points.
func (s *Store) TimeLeft(name string) (seconds float64) {
	return s.timeLeft(name)
}

// TsdbTimeSeries returns the time series with given name from given endpoint.
//
// If no such metric exists for given endpoint, TsdbTimeSeries returns false
// instead of true for the second argument.
//
// name is the name of the time series; endpointId identifies the endpoint.
// start and end are seconds since Jan 1, 1970 and denote the start time
// inclusive and the end time exclusive.
//
// TsdbTimeSeries only works on time series with numeric values. For other
// time series, it returns (nil, false) as if the time series were not found.
// If multiple time series with different numeric types exist for the
// same name, TsdbTimeSeries merges them together in the result as floats.
// Any inactive flags found are not reflected in returned value.
func (s *Store) TsdbTimeSeries(
	name string,
	endpointId interface{},
	start, end float64) (tsdb.TimeSeries, bool) {
	return s.tsdbTimeSeries(name, endpointId, start, end)
}

// NamedIteratorForEndpoint returns an iterator for the given name that
// iterates over metric values for all known timestamps for the given endpoint.
// Since the name is used to track progress of iterating over the given
// endpoint, caller should avoid creating two iterators with the same name
// iterating over the same endpoint at the same time.
// Although returned iterator iterates over values and timestamps in no
// particular order, it will iterate over values of the same metric by
// increasing timestamp. For now, this method uses the
// store.GroupMetricByPathAndNumeric strategy to determine whether or not
// two metrics are the same.
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
	maxFrames uint) (
	iterator NamedIterator, remainingValuesInSeconds float64) {
	return s.namedIteratorForEndpoint(name, endpointId, int(maxFrames))
}

// NamedIteratorForEndpointRollUp works like NamedItgeratorForEndpoint except
// that instead of reporting every individual value for each metric, it
// reports summarised values by averaging / rolling up values for each time
// period.
//
// NamedIteratorForEndpointRollUp summarises values by averaging the
// values for each time period and emits each average with the start time
// of the corresponding time period.
// When averaging integer values, it rounds the average to the nearest integer.
// For non-numeric values such as strings, it summarises by reporting the
// first encountered value with the start timestamp of each time period.
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
// strategy describes how NamedIteratorForEndpointRollUp groups metrics for
// aggregation. Generally, caller will pass store.GroupMetricByPathAndNumeric,
// but if a persistent store writer wishes to store additional metric data
// in rolled up metrics besides just its path, it will need to use a
// different strategy.
//
// To avoid reporting summarised values for a time period before all the values
// are in, NamedIteratorForEndpointRollup will not report a summarised value
// for a given time period for a metric unless values for a later time period
// for that same metric are present.
func (s *Store) NamedIteratorForEndpointRollUp(
	name string,
	endpointId interface{},
	dur time.Duration,
	maxFrames uint,
	strategy MetricGroupingStrategy) (
	iterator NamedIterator, remainingValuesInSeconds float64) {
	return s.namedIteratorForEndpointRollUp(
		name, endpointId, dur, int(maxFrames), strategy)
}

// StartAtBeginning removes the stored state for iterators with given names.
func (s *Store) StartAtBeginning(endpointId interface{}, names ...string) {
	s.startAtBeginning(endpointId, names)
}

// SetIteratorTo sets the position of future iterators named destName to be
// the same as future iterators named srcName. Commiting an existing
// iterator named destName after making this call will have no effect.
func (s *Store) SetIteratorTo(
	endpointId interface{}, destName, srcName string) {
	s.setIteratorTo(endpointId, destName, srcName)
}

// LatestByEndpoint returns the latest records for each metric for a
// given endpoint.
// LatestByEndpoint appends the records to result in no particular order.
func (s *Store) LatestByEndpoint(
	endpointId interface{},
	result Appender) {
	s.latestByPrefixAndEndpoint("", endpointId, GroupMetricExactly, result)
}

// LatestByEndpointStraegy returns the latest record for each metric group
// in no particular order.
func (s *Store) LatestByPrefixAndEndpointStrategy(
	prefix string,
	endpointId interface{},
	strategy MetricGroupingStrategy,
	result Appender) {
	s.latestByPrefixAndEndpoint(prefix, endpointId, strategy, result)
}

// VisitAllEndpoints visits each endpoint registered with this instance.
// If v.Visit() returns a non-nil error, VisitAllEndpoints returns that
// same error immediately.
func (s *Store) VisitAllEndpoints(v Visitor) error {
	return s.visitAllEndpoints(v)
}

// Endpoints returns all the endpoints in this store
func (s *Store) Endpoints() []interface{} {
	return s.endpoints()
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

// Coordinator coordinates writes to persistent stores across multiple scotty
// processes. Each scotty process should have only one coordinator shared
// among the goroutines writing to persistent store. This one coordinator
// assigns a lease to the scotty process. The lease gives the scotty process
// permission to write the metric values with timestamps falling within the
// lease's time range to persistent store.
//
// The primary purpose of the lease system is to ensure that only one scotty
// process at any given time is writing to persistent store. We call this
// one scotty process the leader. Only the leader can acquire a new lease
// or extend an existing lease. If a non-leader tries to acquire or extend a
// lease, it blocks until such time that it is elected the leader.
// Eventually, more than one scotty process may think it is the leader for
// a short time; therefore, the second purpose of the lease system is to
// ensure that at most one scotty process writes metric values for each
// time range. Once a coordinator instance assigns a lease to its scotty
// process, the time range in that lease forever becomes the responsibility of
// that process. After that, no other scotty process will ever get a
// lease with a time range overlapping with the already assigned time range.
//
// If a metric value time stamp comes before the start of the assigned
// lease, the scotty process should skip writing that particular value as
// likely the timestamp is the responsibility of another process.
// If a metric value timestamp falls within the assigned lease, the scotty
// process may safely write that value at any time as the lease will never
// be assigned to another process. If a metric value timestamp comes after
// the assigned lease, the scotty process should request an extension of its
// lease. If another scotty process is responsible for times after the
// assigned lease, an extension may not be possible. Whenever an extension
// can't be granted, the coordinator instance will assign a brand new lease
// to its scotty process with the earliest possible start time.
//
// Although leases never expire, for simplicity sake, a coordinator instance
// maintains one and only one active lease for its scotty process. When
// the coordinator must assign a new lease instead of extending the old
// one, record of the old lease is gone even though in theory, it is still
// in effect as no other process will be assigned to it. This forgetting
// of previous leases is ok because once a scotty process gets a lease, it
// should maintain its leadership role as long as it is running. Therefore,
// it will always be able to extend its lease. However, in rare cases, a
// running scotty process may loose its leadership role over network failures.
// It is in these rare cases, that the scotty process would be given a brand
// new lease whenever it regained its leadership role.
//
// Note that while the leasing system that coordinator instances use prevent
// multiple scotty processes from writing metrics for the same time, it
// makes no guarantee that values are being written for all times. For
// instance, a scotty process could die immediately after acquiring or
// extending a lease but before it can write the values falling within
// that lease leaving those values forever lost.
type Coordinator interface {
	// Lease acquires a lease. When possible, Lease will extend the
	// existing lease to include timeToInclude rather than acquiring a
	// brand new lease. If the current lease already includes timeToInclude,
	// it is returned unchanged. If the current process is not the leader,
	// and Lease must acquire a new lease or extend the existing lease to
	// include timeToInclude, Lease blocks the caller until such time that
	// the current process is elected leader. If Lease must extend the
	// existing lease or acquire a new lease, the returned lease will be
	// at least minSpanInSeconds in length and end at least minSpanInSeconds
	// after timeToInclude.
	Lease(minSpanInSeconds, timeToInclude float64) (
		startTimeInSecondsInclusive, endTimeInSecondsExclusive float64)
}
