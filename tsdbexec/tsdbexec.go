package tsdbexec

import (
	"errors"
	"fmt"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbimpl"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var (
	kOptions = &apiutil.Options{
		ErrorGenerator: func(status int, err error) interface{} {
			return tsdbjson.NewError(status, err)
		},
	}
)

func newTagFilter(spec *tsdbjson.FilterSpec) (
	tsdb.TagFilter, error) {
	if spec == nil {
		return nil, nil
	}
	return tsdbjson.NewTagFilter(spec.Type, spec.Value)
}

func _suggest(
	params url.Values,
	suggesterMap map[string]suggest.Suggester) (
	result []string, err error) {
	maxStr := params.Get("max")
	var max int
	if maxStr == "" {
		max = 25
	} else {
		max, err = strconv.Atoi(params.Get("max"))
		if err != nil {
			return
		}
	}
	qtype := params.Get("type")
	suggester := suggesterMap[qtype]
	if suggester != nil {
		result = suggester.Suggest(max, params.Get("q"))
		if result == nil {
			result = []string{}
		}
		return
	} else {
		return nil, errors.New(
			fmt.Sprintf("Invalid 'type' parameter:%s", qtype))
	}
}

func ensureDurationAtLeast(
	spec *tsdbjson.DownSampleSpec,
	minDurationInSeconds float64) *tsdbjson.DownSampleSpec {
	newSpec := *spec
	if newSpec.DurationInSeconds < minDurationInSeconds {
		newSpec.DurationInSeconds = minDurationInSeconds
	}
	return &newSpec
}

func query(
	request *tsdbjson.QueryRequest,
	endpoints *datastructs.ApplicationStatuses,
	minDownSampleTime time.Duration) (
	result []tsdbjson.TimeSeries, err error) {
	parsedQueries, err := tsdbjson.ParseQueryRequest(request)
	if err != nil {
		return
	}
	var allSeries []tsdbjson.TimeSeries
	for i := range parsedQueries {
		var series *tsdb.TaggedTimeSeriesSet
		series, err = runSingleParsedQuery(
			parsedQueries[i], endpoints, minDownSampleTime)
		if err != nil {
			return
		}
		allSeries = append(allSeries, tsdbjson.NewTimeSeriesSlice(series)...)
	}
	if allSeries == nil {
		return make([]tsdbjson.TimeSeries, 0), nil
	}
	return allSeries, nil
}

func runParsedQueries(
	requests []tsdbjson.ParsedQuery,
	endpoints *datastructs.ApplicationStatuses,
	minDownSampleTime time.Duration) (
	[]*tsdb.TaggedTimeSeriesSet, error) {
	results := make([]*tsdb.TaggedTimeSeriesSet, len(requests))
	for i, request := range requests {
		result, err := runSingleParsedQuery(
			request, endpoints, minDownSampleTime)
		if err == tsdbimpl.ErrNoSuchMetric {
			results[i] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	return results, nil
}

func runSingleParsedQuery(
	request tsdbjson.ParsedQuery,
	endpoints *datastructs.ApplicationStatuses,
	minDownSampleTime time.Duration) (
	result *tsdb.TaggedTimeSeriesSet, err error) {
	var options tsdbimpl.QueryOptions
	options.HostNameFilter, err = newTagFilter(
		request.Options.HostNameFilter)
	if err != nil {
		return
	}
	options.AppNameFilter, err = newTagFilter(
		request.Options.AppNameFilter)
	if err != nil {
		return
	}
	options.GroupByAppName = request.Options.GroupByAppName
	options.GroupByHostName = request.Options.GroupByHostName
	if request.Aggregator.DownSample == nil {
		return nil, tsdbjson.ErrUnsupportedAggregator
	}
	request.Aggregator.DownSample = ensureDurationAtLeast(
		request.Aggregator.DownSample,
		duration.ToFloat(minDownSampleTime))
	var aggregatorGen tsdb.AggregatorGenerator
	aggregatorGen, err = tsdbjson.NewAggregatorGenerator(
		request.Aggregator.Type,
		request.Aggregator.DownSample,
		request.Aggregator.RateOptions,
	)
	if err != nil {
		return
	}
	request.EnsureStartTimeRecentEnough()
	return tsdbimpl.Query(
		endpoints,
		request.Metric,
		aggregatorGen,
		request.Start,
		request.End,
		&options)
}

func newHandler(handler interface{}) http.Handler {
	return apiutil.NewHandler(handler, kOptions)
}
