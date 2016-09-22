package tsdbexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbimpl"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

var (
	kErrorType     = reflect.TypeOf((*error)(nil)).Elem()
	kUrlValuesType = reflect.TypeOf(url.Values(nil))
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

type tsdbHandlerType struct {
	inType       reflect.Type
	handlerValue reflect.Value
}

func newHandler(
	handler interface{}) http.Handler {
	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()
	if handlerType.Kind() != reflect.Func {
		panic("NewHandler argument must be a func.")
	}
	if handlerType.NumIn() != 1 {
		panic("NewHandler argument must be a func of one arg")
	}
	if handlerType.NumOut() != 2 || handlerType.Out(1) != kErrorType {
		panic("NewHandler argument must be a func returning 1 value and 1 error")
	}
	inType := handlerType.In(0)
	return &tsdbHandlerType{inType: inType, handlerValue: handlerValue}
}

func (h *tsdbHandlerType) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var inValue reflect.Value
	if h.inType == kUrlValuesType {
		if err := r.ParseForm(); err != nil {
			showError(w, 400, err.Error())
			return
		}
		inValue = reflect.ValueOf(r.Form)
	} else {
		ptrValue := reflect.New(h.inType)
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(ptrValue.Interface()); err != nil {
			showError(w, 400, err.Error())
			return
		}
		inValue = ptrValue.Elem()
	}
	// Set up response headers
	headers := w.Header()
	headers.Add("Content-Type", "application/json; charset=UTF-8")
	// Call the handler
	results := h.handlerValue.Call([]reflect.Value{inValue})
	if errInterface := results[1].Interface(); errInterface != nil {
		showError(w, 400, errInterface.(error).Error())
		return
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(results[0].Interface())
}

func notFoundHandlerFunc(w http.ResponseWriter, r *http.Request) {
	showError(w, 404, "Endpoint not found")
}

func showError(w http.ResponseWriter, statusCode int, message string) {
	type errorCodeType struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}

	type errorResponseType struct {
		Error errorCodeType `json:"error"`
	}
	var anError errorResponseType
	anError.Error.Code = statusCode
	anError.Error.Message = message
	w.WriteHeader(statusCode)
	encoder := json.NewEncoder(w)
	encoder.Encode(&anError)
}
