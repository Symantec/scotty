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
			return newHTTPError(status, err)
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
	minDurationInSeconds float64,
	spec **tsdbjson.DownSampleSpec) {
	if (*spec).DurationInSeconds >= minDurationInSeconds {
		return
	}
	newSpec := **spec
	newSpec.DurationInSeconds = minDurationInSeconds
	*spec = &newSpec
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
		if parsedQueries[i].Aggregator.DownSample == nil {
			return nil, tsdbjson.ErrUnsupportedAggregator
		}
		ensureDurationAtLeast(
			duration.ToFloat(minDownSampleTime),
			&parsedQueries[i].Aggregator.DownSample)
		var aggregatorGen tsdb.AggregatorGenerator
		aggregatorGen, err = tsdbjson.NewAggregatorGenerator(
			parsedQueries[i].Aggregator.Type,
			parsedQueries[i].Aggregator.DownSample,
			parsedQueries[i].Aggregator.RateOptions,
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
	if allSeries == nil {
		return make([]tsdbjson.TimeSeries, 0), nil
	}
	return allSeries, nil
}

func newHandler(handler interface{}) http.Handler {
	return apiutil.NewHandler(handler, kOptions)
}

func newHTTPError(status int, err error) apiutil.HTTPError {
	var anError httpErrorType
	anError.E.Code = status
	anError.E.Message = err.Error()
	return &anError
}

type errorCodeType struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type httpErrorType struct {
	E errorCodeType `json:"error"`
}

func (h *httpErrorType) Error() string {
	return h.E.Message
}

func (h *httpErrorType) Status() int {
	return h.E.Code
}
