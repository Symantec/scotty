package responses

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/influxdata/influxdb/client/v2"
)

// Merge merges responses from multiple servers into a single
// response.
// Merge expects the same query to be sent to all servers except
// for different time ranges.
// An error in any respone means an error in the merged response.
//
// The returned response will contain time series with values sorted by time
// in increasing order even if the responses merged had times in
// decreasing order.
//
// If the returned responses containing multiple series, they will be sorted
// first by name and then by the tags. When sorting tags, Merge
// first places the tag keys of the time series to be sorted in ascending
// order. To compare two sets of tags, Merge first compares the
// first pair of tag keys. If they match, Merge uses the values of
// those first keys to break the tie. If those match, Merge uses
// the second pair of tag keys to break the tie. If those match,
// Merge uses the values of the second keys to brak the tie etc.
//
// Merge includes all messages from the responses being merged in
// the merged response.
func Merge(responses ...*client.Response) (*client.Response, error) {
	return mergeResponses(responses)
}

// MergePreferred merges a response from a preferred source, such as scotty,
// with an existing response using the following rules.
//
// 1. MergePreferred combines the rows from preferred with the rows in
//    response.
// 2. If preferred and response both contain a row with both the same name
//    and the same tags then MergePreferred merges that row using the
//    the following rules:
// 2a. Find the earliest time for which the row in preferred has data
// 2b. To merge use the values in original for times before the time in 2a and
//     use the values in preferred for times on or after the time in 2a
func MergePreferred(response, preferred *client.Response) (
	*client.Response, error) {
	return mergePreferred(response, preferred)
}

// FromTaggedTimeSeriesSets converts a group of TaggedTimeSeriesSet
// instances to an influx db client.Response instance
// Elements in the pqs slice correspond to elements in the series and
// colNames slice.
// Elements in the series slice may be nil. FromTaggedTimeSeriesSets panics
// if pqs, series, and colNames have different lengths.
func FromTaggedTimeSeriesSets(
	series []*tsdb.TaggedTimeSeriesSet,
	colNames [][]string,
	pqs []tsdbjson.ParsedQuery,
	epochConversion func(ts int64) int64) *client.Response {
	if len(series) != len(pqs) {
		panic("Slices must be of equal length")
	}
	if len(series) != len(colNames) {
		panic("Slices must be of equal length")
	}
	return fromTaggedTimeSeriesSets(series, colNames, pqs, epochConversion)
}
