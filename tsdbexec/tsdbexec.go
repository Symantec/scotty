package tsdbexec

import (
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbimpl"
	"github.com/Symantec/scotty/tsdbjson"
)

func newTagFilter(spec *tsdbjson.FilterSpec) (
	func(s string) bool, error) {
	if spec == nil {
		return nil, nil
	}
	return tsdbjson.NewTagFilter(spec.Type, spec.Value)
}

func query(
	request *tsdbjson.QueryRequest,
	endpoints *datastructs.ApplicationStatuses) (
	result []tsdbjson.TimeSeries, err error) {
	parsedQueries, err := tsdbjson.ParseQueryRequest(request)
	if err != nil {
		return
	}
	var allSeries []tsdbjson.TimeSeries
	for i := range parsedQueries {
		var options tsdbimpl.QueryOptions
		options.HostNameFilter, err = newTagFilter(
			parsedQueries[i].Options.HostNameFilter)
		if err != nil {
			return
		}
		options.AppNameFilter, err = newTagFilter(
			parsedQueries[i].Options.AppNameFilter)
		if err != nil {
			return
		}
		options.GroupByAppName = parsedQueries[i].Options.GroupByAppName
		options.GroupByHostName = parsedQueries[i].Options.GroupByHostName
		var aggregatorGen tsdb.AggregatorGenerator
		aggregatorGen, err = tsdbjson.NewAggregatorGenerator(
			parsedQueries[i].Aggregator.Type,
			parsedQueries[i].Aggregator.DownSample,
		)
		if err != nil {
			return
		}
		var series *tsdb.TaggedTimeSeriesSet
		series, err = tsdbimpl.Query(
			endpoints,
			parsedQueries[i].Metric,
			aggregatorGen,
			parsedQueries[i].Start,
			parsedQueries[i].End,
			&options)
		if err != nil {
			return
		}
		allSeries = append(allSeries, tsdbjson.NewTimeSeriesSlice(series)...)
	}
	result = allSeries
	return
}
