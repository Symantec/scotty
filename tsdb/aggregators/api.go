// Package aggregators contains aggregator factory methods.
// The aggregators package must not depend on any other scotty packages
// except for tsdb.
package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

// NewAverage returns an aggregator that averages time series.
//
// The returned aggregator is intended to work like the average aggregator in
// tsdb with the following differences. First, down sampling is required
// whereas it is optional in real tsdb. Second, this aggregator treats missing
// values as NaN instead of using linear interpolation to guess missing
// values.
//
// start and end are the start and end times in seconds since Jan 1, 1970 for
// the aggregation. Time series passed to the Add() method of returned
// aggregator must fall within start inclusive and end exclusive. downSample
// is the down sample time in seconds. NewAverage treats downSample
// values less than 1.0 as 1.0.
func NewAverage(start, end, downSample float64) tsdb.Aggregator {
	return newAverage(start, end, downSample)
}
