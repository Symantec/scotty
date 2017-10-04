package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Symantec/Dominator/lib/log"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// appends together latest metrics in JSON
type latestMetricsAppenderJSON struct {
	result   *[]*messages.LatestMetric
	hostName string
	appName  string
}

func (a *latestMetricsAppenderJSON) Append(r *store.Record) bool {
	// Skip inactive records
	if !r.Active {
		return true
	}
	jsonValue, jsonKind, jsonSubType := asJsonWithSubType(
		r.Value,
		r.Info.Kind(),
		r.Info.SubType(),
		r.Info.Unit())
	var upperLimits []float64
	if r.Info.Kind() == types.Dist {
		upperLimits = r.Info.Ranges().UpperLimits
	}
	aMetric := &messages.LatestMetric{
		HostName:        a.hostName,
		AppName:         a.appName,
		Path:            r.Info.Path(),
		Kind:            jsonKind,
		SubType:         jsonSubType,
		Description:     r.Info.Description(),
		Bits:            r.Info.Bits(),
		Unit:            r.Info.Unit(),
		Value:           jsonValue,
		Timestamp:       duration.SinceEpochFloat(r.TimeStamp).String(),
		IsNotCumulative: r.Info.IsNotCumulative(),
		UpperLimits:     upperLimits,
	}
	*a.result = append(*a.result, aMetric)
	return true
}

func asDistribution(distTotals *store.DistributionTotals) (
	result *messages.Distribution) {
	if distTotals != nil {
		result = &messages.Distribution{
			Sum:           distTotals.Sum,
			Count:         distTotals.Count(),
			Counts:        distTotals.Counts,
			RollOverCount: distTotals.RollOverCount,
		}
	}
	return
}

func fixForGoRPC(value interface{}, kind types.Type) interface{} {
	if kind == types.Dist {
		return asDistribution(value.(*store.DistributionTotals))
	}
	return value
}

// appends together latest metrics for go rpc
type latestMetricsAppender struct {
	result   *[]*messages.LatestMetric
	hostName string
	appName  string
}

func (a *latestMetricsAppender) Append(r *store.Record) bool {
	// Skip inactive records
	if !r.Active {
		return true
	}
	var upperLimits []float64
	if r.Info.Kind() == types.Dist {
		upperLimits = r.Info.Ranges().UpperLimits
	}
	aMetric := &messages.LatestMetric{
		HostName:        a.hostName,
		AppName:         a.appName,
		Path:            r.Info.Path(),
		Kind:            r.Info.Kind(),
		SubType:         r.Info.SubType(),
		Description:     r.Info.Description(),
		Bits:            r.Info.Bits(),
		Unit:            r.Info.Unit(),
		Value:           fixForGoRPC(r.Value, r.Info.Kind()),
		Timestamp:       duration.FloatToTime(r.TimeStamp),
		IsNotCumulative: r.Info.IsNotCumulative(),
		UpperLimits:     upperLimits,
	}
	*a.result = append(*a.result, aMetric)
	return true
}

// endpointMetricsAppender is an implementation of store.Appender that
// appends to a messages.EndpointMetricsList.
// endpointMetricsAppender is NOT threadsafe.
type endpointMetricsAppender struct {
	endpointMetrics *messages.EndpointMetricList
	lastInfo        *store.MetricInfo
	lastMetric      *messages.EndpointMetric
}

// newEndpointMetricsAppender creates a endpointMetricsAppender that appends
// to result.
func newEndpointMetricsAppender(result *messages.EndpointMetricList) *endpointMetricsAppender {
	return &endpointMetricsAppender{endpointMetrics: result}
}

func asJsonWithSubType(
	value interface{}, kind, subType types.Type, unit units.Unit) (
	jsonValue interface{}, jsonKind, jsonSubType types.Type) {
	if kind == types.Dist {
		jsonValue = asDistribution(value.(*store.DistributionTotals))
		jsonKind = kind
		jsonSubType = subType
		return
	}
	return trimessages.AsJsonWithSubType(value, kind, subType, unit)
}

func (a *endpointMetricsAppender) Append(r *store.Record) bool {
	if !store.GroupMetricByKey.Equal(r.Info, a.lastInfo) {
		jsonValue, jsonKind, jsonSubType := asJsonWithSubType(
			r.Value,
			r.Info.Kind(),
			r.Info.SubType(),
			r.Info.Unit())
		var upperLimits []float64
		if r.Info.Kind() == types.Dist {
			upperLimits = r.Info.Ranges().UpperLimits
		}
		a.lastMetric = &messages.EndpointMetric{
			Path:        r.Info.Path(),
			Kind:        jsonKind,
			SubType:     jsonSubType,
			Description: r.Info.Description(),
			Bits:        r.Info.Bits(),
			Unit:        r.Info.Unit(),
			Values: []*messages.TimestampedValue{{
				Timestamp: duration.SinceEpochFloat(
					r.TimeStamp).String(),
				Value:  jsonValue,
				Active: r.Active,
			}},
			IsNotCumulative: r.Info.IsNotCumulative(),
			UpperLimits:     upperLimits,
		}
		*a.endpointMetrics = append(*a.endpointMetrics, a.lastMetric)
	} else {
		jsonValue, _, _ := asJsonWithSubType(
			r.Value,
			r.Info.Kind(),
			r.Info.SubType(),
			r.Info.Unit())
		newTimestampedValue := &messages.TimestampedValue{
			Timestamp: duration.SinceEpochFloat(
				r.TimeStamp).String(),
			Value:  jsonValue,
			Active: r.Active,
		}
		a.lastMetric.Values = append(
			a.lastMetric.Values, newTimestampedValue)
	}
	a.lastInfo = r.Info
	return true
}

func latestMetricsForEndpoint(
	metricStore *store.Store,
	ep *collector.Endpoint,
	canonicalPath string,
	json bool) (result []*messages.LatestMetric) {
	var appender store.Appender
	if json {
		appender = &latestMetricsAppenderJSON{
			result:   &result,
			hostName: ep.HostName(),
			appName:  ep.AppName(),
		}
	} else {
		appender = &latestMetricsAppender{
			result:   &result,
			hostName: ep.HostName(),
			appName:  ep.AppName(),
		}
	}
	metricStore.LatestByPrefixAndEndpointStrategy(
		canonicalPath,
		ep,
		store.GroupMetricByPathAndNumeric,
		store.AppenderFilterFunc(
			appender,
			func(r *store.Record) bool {
				return r.Info.Path() == canonicalPath || strings.HasPrefix(
					r.Info.Path(), canonicalPath+"/")
			},
		),
	)
	sort.Sort(latestByPath(result))
	return
}

// gatherDataForEndpoint serves api/hosts pages.
// metricStore is the metric store.
// endpoint is the endpoint from which we are getting historical metrics.
// canonicalPath is the path of the metrics or the empty string for all
// metrics. canonicalPath is returned from canonicalisePath().
// history is the amount of time to go back in minutes.
// If isSingleton is true, fetched metrics have to match canonicalPath
// exactly.
// Otherwise fetched metrics have to be found underneath canonicalPath.
// On no match, gatherDataForEndpoint returns an empty
// messages.EndpointMetricsList instance
func gatherDataForEndpoint(
	metricStore *store.Store,
	endpoint *collector.Endpoint,
	canonicalPath string,
	history int,
	isSingleton bool) (result messages.EndpointMetricList) {
	result = make(messages.EndpointMetricList, 0)
	now := duration.TimeToFloat(time.Now())
	appender := newEndpointMetricsAppender(&result)
	if canonicalPath == "" {
		metricStore.ByEndpointStrategy(
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			store.GroupMetricByKey,
			appender)
	} else {
		metricStore.ByNameAndEndpointStrategy(
			canonicalPath,
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			store.GroupMetricByKey,
			appender)
		if len(result) == 0 && !isSingleton {
			metricStore.ByPrefixAndEndpointStrategy(
				canonicalPath+"/",
				endpoint,
				now-60.0*float64(history),
				math.Inf(1),
				store.GroupMetricByKey,
				appender)
		}

	}
	sortMetricsByPath(result)
	return
}

// byPath sorts metrics by path lexographically
type latestByPath []*messages.LatestMetric

func (b latestByPath) Len() int {
	return len(b)
}

func (b latestByPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b latestByPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

// byPath sorts metrics by path lexographically
type byPath messages.EndpointMetricList

func (b byPath) Len() int {
	return len(b)
}

func (b byPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b byPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortMetricsByPath(result messages.EndpointMetricList) {
	sort.Sort(byPath(result))
}

func encodeJson(w io.Writer, data interface{}, pretty bool) error {
	if pretty {
		content, err := json.Marshal(data)
		if err != nil {
			return err
		}
		var buffer bytes.Buffer
		json.Indent(&buffer, content, "", "\t")
		buffer.WriteTo(w)
		return nil
	}
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(data); err != nil {
		return err
	}
	return nil
}

// errorHandler provides the api/errors requests.
type errorHandler struct {
	ConnectionErrors *connectionErrorsType
	Logger           log.Logger
}

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	err := encodeJson(
		w, h.ConnectionErrors.GetErrors(), r.Form.Get("format") == "text")
	if err != nil {
		h.Logger.Printf("errorHandler: cannot encode json: %v", err)
		httpError(w, 500)
	}
}

// canonicalisePath removes any trailing slashes from path and ensures it has
// exactly one leading slash. The one exception to this is that if
// path is empty, canonicalisePath returns the empty string.
func canonicalisePath(path string) string {
	if path == "" {
		return ""
	}
	return "/" + strings.Trim(path, "/")
}

func httpError(w http.ResponseWriter, status int) {
	http.Error(
		w,
		fmt.Sprintf(
			"%d %s",
			status,
			http.StatusText(status)),
		status)
}

type latestHandler struct {
	ES     *machine.EndpointStore
	Logger log.Logger
}

func (h latestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	path := canonicalisePath(r.URL.Path)
	apps, metricStore := h.ES.AllActiveWithStore()
	machine.ByHostAndName(apps)
	data := make([]*messages.LatestMetric, 0)
	for _, app := range apps {
		data = append(
			data,
			latestMetricsForEndpoint(metricStore, app.App.EP, path, true)...)
	}
	err := encodeJson(w, data, r.Form.Get("format") == "text")
	if err != nil {
		h.Logger.Printf("latestHandler: cannot encode json: %v", err)
		httpError(w, 500)
	}
}

type earliestHandler struct {
	ES     *machine.EndpointStore
	Logger log.Logger
}

func (h earliestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	hostNameAndPath := strings.SplitN(r.URL.Path, "/", 3)
	var host string
	var name string
	var path string
	if len(hostNameAndPath) < 3 {
		httpError(w, 404)
		return
	} else {
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], canonicalisePath(hostNameAndPath[2])
	}
	endpoint, metricStore := h.ES.ByHostAndName(host, name)
	if endpoint == nil {
		httpError(w, 404)
		return
	}
	earliest := metricStore.Earliest(path, endpoint)
	fmt.Fprintln(w, "Earliest", earliest)
}

// byEndpointHandler handles serving api/hosts requests
type byEndpointHandler struct {
	ES     *machine.EndpointStore
	Logger log.Logger
}

func (h byEndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	hostNameAndPath := strings.SplitN(r.URL.Path, "/", 3)
	var host string
	var name string
	var path string
	if len(hostNameAndPath) == 1 {
		httpError(w, 404)
		return
	} else if len(hostNameAndPath) == 2 {
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], ""
	} else {
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], canonicalisePath(hostNameAndPath[2])
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	endpoint, metricStore := h.ES.ByHostAndName(host, name)
	if endpoint == nil {
		httpError(w, 404)
		return
	}
	data := gatherDataForEndpoint(
		metricStore, endpoint.App.EP, path, history, isSingleton)
	err = encodeJson(w, data, r.Form.Get("format") == "text")
	if err != nil {
		h.Logger.Printf("byEndpointHandler: cannot encode json: %v", err)
		httpError(w, 500)
	}
}

type rpcType struct {
	ES *machine.EndpointStore
}

func (t *rpcType) Latest(
	path string, response *[]*messages.LatestMetric) error {
	apps, metricStore := t.ES.AllActiveWithStore()
	machine.ByHostAndName(apps)
	path = canonicalisePath(path)
	for _, app := range apps {
		*response = append(
			*response,
			latestMetricsForEndpoint(metricStore, app.App.EP, path, false)...)
	}
	return nil
}
