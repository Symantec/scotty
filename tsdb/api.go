// package tsdb contains the data structures for emulating the TSDB API.
// The tsdb package must not depend on any other scotty packages.
package tsdb

// TagFilter represents an arbitrary filter on a tag value. Filter returns
// true if a given tag value should be included or false otherwise.
type TagFilter interface {
	Filter(s string) bool
}

// TsValue represents a single timestamped value
type TsValue struct {
	// In seconds since Jan 1, 1970
	Ts float64
	// The value
	Value float64
}

// TimeSeries represents a time series. TimeSeries are time stamped values
// sorted by time stamp in ascending order. TimeSeries contain values for
// all known time stamps even if the value hasn't changed. TimeSeries
// instances must be treated as immutable.
type TimeSeries []TsValue

// MarshalJSON satisfies the encoding/json.Marshaler interface and allows
// correct JSON encoding of TimeSeries values. In particular, time series
// must be encoded as
// {"secondsSinceEpoch1":value1, "secondsSinceEpoch2":value2, ...}
// in ascending order by time.
func (t TimeSeries) MarshalJSON() ([]byte, error) {
	return t.marshalJSON()
}

// TagSet represents a set of tsdb tags for time series in scotty.
// Since scotty organizes time series by host name and app name, those are the
// only tags
type TagSet struct {
	HostName string
	AppName  string
}

// TaggedTimeSeries represents a single tagged timeSeries.
type TaggedTimeSeries struct {
	Tags   TagSet
	Values TimeSeries
}

// TaggedTimeSeriesSet represets a set of tagged time series
type TaggedTimeSeriesSet struct {
	// The metric name
	MetricName string
	// the data
	Data []TaggedTimeSeries
	// If true, Data is grouped by the "HostName" tag
	GroupedByHostName bool
	// If true, Data is grouped by the "appname" tag
	GroupedByAppName bool
}

// Aggregator aggregates time series together.
type Aggregator interface {
	// Add adds a time series
	Add(values TimeSeries)
	// Aggregate returns the aggregated time series.
	Aggregate() TimeSeries
}

// AggregatorGenerator produces Aggregator values.
// start is the start time for the aggregator inclusive;
// end is the end time for the aggregator exclusive.
// start and end are seconds since Jan 1, 1970.
type AggregatorGenerator func(start, end float64) (Aggregator, error)
