// package tsdbimpl contains routines for internally fulfilling tsdb APi
// requests.
// tsdbimpl may depend on any scotty package except for tsdbjson and tsdbexec.
// That is tsdbimpl should do the heavy lifting of fulfilling tsdb requests
// but not be concerned with parsing or generating json.
package tsdbimpl

import (
	"errors"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/tsdb"
)

var (
	// Means the metric does not exist
	ErrNoSuchMetric = errors.New("tsdbimpl: No such metric.")
)

// QueryOptions contain optional configurations for the query function.
type QueryOptions struct {
	// Filter for "HostName" tag values. Optional
	HostNameFilter tsdb.TagFilter
	// Filter for "appname" tag values. Optional
	AppNameFilter tsdb.TagFilter
	// Filter for "region" tag value. Optional
	RegionFilter tsdb.TagFilter
	// True if results should be grouped by "HostName" tag
	GroupByHostName bool
	// True if results should be groupd by "appname" tag.
	GroupByAppName bool
	// True if results should be grouped by region
	GroupByRegion bool
}

// Query queries scotty for given tsdb query.
func Query(
	endpoints *machine.EndpointStore,
	metricName string,
	aggregator tsdb.AggregatorGenerator,
	start, end float64,
	options *QueryOptions) (*tsdb.TaggedTimeSeriesSet, error) {
	return query(
		endpoints,
		metricName,
		aggregator,
		start,
		end,
		options)
}
