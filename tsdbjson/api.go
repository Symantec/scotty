// Package tsdbjson handles tsdb JSON requests and responses.
// Package tsdbjson must not depend on any other scotty packages except tsdb
// and its sub packages. That is tsdbjson translates between JSON and
// requests that scotty understands but does not fulfill the requests.
package tsdbjson

import (
	"errors"
	"fmt"
	"github.com/Symantec/scotty/tsdb"
)

const (
	// HostName tag name
	HostName = "HostName"
	// AppName tag name
	AppName = "appname"
)

const (
	// LiteralOr is the name of TSDB "literal_or" filter.
	LiteralOr = "literal_or"
)

var (
	// The filter is unsupported.
	ErrUnsupportedFilter = errors.New("tsdbjson: Unsupported filter")
	// The aggregator is unsupported.
	ErrUnsupportedAggregator = errors.New("tsdbjson: Unsupported aggregator")
	// Bad value in json field.
	ErrBadValue = errors.New("tsdbjson: Bad value")
)

// Filter represents a filter in a query request
type Filter struct {
	// The type such as literal_or, iliteral_or, wildcard, etc.
	Type string `json:"type"`
	// The tag name.
	Tagk string `json:"tagk"`
	// The filter value which depends on the filter type
	Filter string `json:"filter"`
	// True if results should be grouped by this tag
	GroupBy bool `json:"groupBy"`
}

// FilterDescription represents a description of a filter
type FilterDescription struct {
	Examples    string `json:"examples"`
	Description string `json:"description"`
}

// Query represents a single query in an /api/query request
type Query struct {
	// The metric name in TSDB escaped form e.g "A_20Metric"
	Metric string `json:"metric"`
	// The aggregator type such as "avg" or "sum"
	Aggregator string `json:"aggregator"`
	// The down sample specification such as "15m_avg"
	DownSample string `json:"downsample"`
	// The filters
	Filters []*Filter `json:"filters"`
}

// QueryRequest represents an an /api/query request
type QueryRequest struct {
	// Start time in millis since Jan 1, 1970 inclusive
	StartInMillis int64 `json:"start"`
	// The queries
	Queries []*Query `json:"queries"`
	// End time in millis since Jan 1, 1970 exclusive
	EndInMillis int64 `json:"end"`
}

// FilterSpec represents a filter specification in a parsed
// /api/query JSON request
type FilterSpec struct {
	// The filter type such as "literal_or"
	Type string
	// The filter value
	Value string
}

func (f *FilterSpec) String() string {
	if f == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", *f)
}

// DownSampleSpec represents the down sample specification in a parsed
// /api/query request
type DownSampleSpec struct {
	// down sample duration
	DurationInSeconds float64
	// down sample type such as "avg" or "sum"
	Type string
	// down sample fill instruction such as "nan" or "zero" or "null"
	// Empty string means no fill.
	Fill string
}

func (d *DownSampleSpec) String() string {
	if d == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", *d)
}

// AggregatorSpec represents the aggregator specification in a parsed
// /api/query request
type AggregatorSpec struct {
	// the aggregator type such as "avg" or "sum"
	Type string
	// The optional down sample specification
	DownSample *DownSampleSpec
}

// ParsedQueryOptions represents the optional items in a parsed query
type ParsedQueryOptions struct {
	// Optional filter on host name
	HostNameFilter *FilterSpec
	// Optional filter on application name
	AppNameFilter *FilterSpec
	// True if results should be grouped by host name
	GroupByHostName bool
	// True if results should be grouped by application name
	GroupByAppName bool
}

// ParsedQuery represents a single query in a parsed /api/query request
type ParsedQuery struct {
	// The metric name
	Metric string
	// The aggregator specification
	Aggregator AggregatorSpec
	// Start time inclusive in seconds since Jan 1, 1970
	Start float64
	// End time exclusive in seconds since Jan 1, 1970
	End float64
	// Options
	Options ParsedQueryOptions
}

// ParseQueryRequest takes a JSON /api/query request as input and returns
// zero or more parsed queries.
func ParseQueryRequest(
	request *QueryRequest) ([]ParsedQuery, error) {
	return parseQueryRequest(request)
}

// TimeSeries represents a single time series in JSON.
// The response of an /api/query request is zero or more of these values
type TimeSeries struct {
	// The metric name
	Metric string `json:"metric"`
	// Tag names and values this time series represents
	Tags map[string]string `json:"tags"`
	// List of tag names that are aggregated into this time series
	AggregateTags []string `json:"aggregateTags"`
	// The time series
	Dps tsdb.TimeSeries `json:"dps"`
}

// NewTimeSeriesSlice creates a tsdb query json response.
func NewTimeSeriesSlice(
	timeSeriesSet *tsdb.TaggedTimeSeriesSet) []TimeSeries {
	return newTimeSeriesSlice(timeSeriesSet)
}

// NewAggregatorGenerator creates a new aggregator generator.
// aggregator is the aggregator type such as "avg" or "sum"
// downSample is optional and includes the down sample specification.
func NewAggregatorGenerator(
	aggregator string, downSample *DownSampleSpec) (
	tsdb.AggregatorGenerator, error) {
	return newAggregatorGenerator(aggregator, downSample)
}

// NewTagFilter creates a new tag filter.
// filterType is the filter type such as "literal_or" or "wildcard"
// filterValue is the filter value.
func NewTagFilter(filterType, filterValue string) (tsdb.TagFilter, error) {
	return newTagFilter(filterType, filterValue)
}
