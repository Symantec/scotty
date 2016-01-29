package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/dom/mdbd"
	"github.com/Symantec/Dominator/lib/mdb"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/sysmemory"
	"github.com/Symantec/tricorder/go/tricorder"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	fBytesPerPage = flag.Int(
		"bytes_per_page",
		1024,
		"Space for new metrics for each endpoint in records")
	fPageCount = flag.Int(
		"page_count",
		30*1000*1000,
		"Buffer size per endpoint in records")
	fAppFile = flag.String(
		"app_file",
		"apps.yaml",
		"File containing mapping of ports to apps")
	fMdbFile = flag.String(
		"mdb_file",
		"/var/lib/Dominator/mdb",
		"Name of file from which to read mdb data.")
	fPollCount = flag.Int(
		"poll_count",
		collector.ConcurrentPolls(),
		"Maximum number of concurrent polls")
	fConnectionCount = flag.Int(
		"connection_count",
		collector.ConcurrentConnects(),
		"Maximum number of concurrent connections")
	fCollectionFrequency = flag.Duration(
		"collection_frequency",
		30*time.Second,
		"Amount of time between metric collections")
	fPStoreBatchSize = flag.Int(
		"pstore_batch_size",
		1000,
		"Write at most this many records at once to persistent storage")
	fPStoreChannelCapacity = flag.Int(
		"pstore_channel_capacity",
		100000,
		"The peristent storage buffer size in metric values")
	fKafkaConfigFile = flag.String(
		"kafka_config_file",
		"",
		"kafka configuration file")
)

type byHostName messages.ErrorList

func (b byHostName) Len() int {
	return len(b)
}

func (b byHostName) Less(i, j int) bool {
	return b[i].HostName < b[j].HostName
}

func (b byHostName) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

type connectionErrorsType struct {
	lock     sync.Mutex
	errorMap map[*collector.Endpoint]*messages.Error
}

func newConnectionErrorsType() *connectionErrorsType {
	return &connectionErrorsType{
		errorMap: make(map[*collector.Endpoint]*messages.Error),
	}
}

func (e *connectionErrorsType) Set(
	m *collector.Endpoint, err error, timestamp time.Time) {
	newError := &messages.Error{
		HostName:  m.HostName(),
		Timestamp: trimessages.SinceEpoch(timestamp).String(),
		Error:     err.Error(),
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.errorMap[m] = newError
}

func (e *connectionErrorsType) Clear(m *collector.Endpoint) {
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.errorMap, m)
}

func (e *connectionErrorsType) GetErrors() (result messages.ErrorList) {
	e.lock.Lock()
	result = make(messages.ErrorList, len(e.errorMap))
	idx := 0
	for endpoint := range e.errorMap {
		result[idx] = e.errorMap[endpoint]
		idx++
	}
	e.lock.Unlock()
	sort.Sort(byHostName(result))
	return
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store               *store.Store
	NoBlockPStore       *pstore.NoBlockPStore
	AppList             *datastructs.ApplicationList
	AppStats            *datastructs.ApplicationStatuses
	ConnectionErrors    *connectionErrorsType
	CollectionTimesDist *tricorder.CumulativeDistribution
	ChangedMetricsDist  *tricorder.CumulativeDistribution
}

func (l *loggerType) LogStateChange(
	e *collector.Endpoint, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		l.CollectionTimesDist.Add(
			newS.TimeSpentConnecting() + newS.TimeSpentPolling() + newS.TimeSpentWaitingToConnect() + newS.TimeSpentWaitingToPoll())
	}
	l.AppStats.Update(e, newS)
}

func (l *loggerType) LogError(e *collector.Endpoint, err error, state *collector.State) {
	if err == nil {
		l.ConnectionErrors.Clear(e)
	} else {
		l.ConnectionErrors.Set(e, err, state.Timestamp())
	}
}

func (l *loggerType) LogResponse(
	e *collector.Endpoint, metrics trimessages.MetricList, state *collector.State) {
	ts := trimessages.TimeToFloat(state.Timestamp())
	added := l.Store.AddBatch(
		e,
		ts,
		metrics,
		func(ametric *trimessages.Metric) bool {
			return ametric.Kind != types.Dist
		},
		func(added *store.Record) {
			l.NoBlockPStore.Write(&pstore.Record{
				HostName:  added.ApplicationId.HostName(),
				AppName:   l.AppList.ByPort(added.ApplicationId.Port()).Name(),
				Path:      strings.Replace(added.Info.Path(), "/", "_", -1),
				Kind:      added.Info.Kind(),
				Unit:      added.Info.Unit(),
				Value:     added.Value,
				Timestamp: added.TimeStamp})
		})
	l.AppStats.LogChangedMetricCount(e, added)
	l.ChangedMetricsDist.Add(float64(added))
}

type gzipResponseWriter struct {
	http.ResponseWriter
	W io.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.W.Write(b)
}

type gzipHandler struct {
	H http.Handler
}

func (h gzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		h.H.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	defer gz.Close()
	gzr := &gzipResponseWriter{ResponseWriter: w, W: gz}
	h.H.ServeHTTP(gzr, r)
}

// endpointMetricsAppender is an implementation of store.Appender that
// appends to a messages.EndpointMetricsList.
// endpointMetricsAppender is NOT threadsafe.
type endpointMetricsAppender struct {
	endpointMetrics *messages.EndpointMetricsList
	lastInfo        *store.MetricInfo
	lastMetric      *messages.EndpointMetrics
}

// newEndpointMetricsAppender creates a endpointMetricsAppender that appends
// to result.
func newEndpointMetricsAppender(result *messages.EndpointMetricsList) *endpointMetricsAppender {
	return &endpointMetricsAppender{endpointMetrics: result}
}

func (a *endpointMetricsAppender) Append(r *store.Record) {
	if r.Info != a.lastInfo {
		a.lastInfo = r.Info
		_, jsonKind := trimessages.AsJson(nil, a.lastInfo.Kind(), a.lastInfo.Unit())
		a.lastMetric = &messages.EndpointMetrics{
			Path:        a.lastInfo.Path(),
			Kind:        jsonKind,
			Description: a.lastInfo.Description(),
			Bits:        a.lastInfo.Bits(),
			Unit:        a.lastInfo.Unit()}
		*a.endpointMetrics = append(*a.endpointMetrics, a.lastMetric)
	}
	jsonValue, _ := trimessages.AsJson(r.Value, a.lastInfo.Kind(), a.lastInfo.Unit())
	newTimestampedValue := &messages.TimestampedValue{
		Timestamp: trimessages.SinceEpochFloat(r.TimeStamp).String(),
		Value:     jsonValue,
	}
	a.lastMetric.Values = append(a.lastMetric.Values, newTimestampedValue)
}

// gatherDataForEndpoint serves api/hosts pages.
// metricStore is the metric store.
// endpoint is the endpoint from which we are getting historical metrics.
// path is the path of the metrics or the empty string for all metrics
// history is the amount of time to go back in minutes.
// If isSingleton is true, fetched metrics have to match path exactly.
// Otherwise fetched metrics have to be found underneath path.
// On no match, gatherDataForEndpoint returns an empty
// messages.EndpointMetricsList instance
func gatherDataForEndpoint(
	metricStore *store.Store,
	endpoint *collector.Endpoint,
	path string,
	history int,
	isSingleton bool) (result messages.EndpointMetricsList) {
	result = make(messages.EndpointMetricsList, 0)
	now := trimessages.TimeToFloat(time.Now())
	appender := newEndpointMetricsAppender(&result)
	if path == "" {
		metricStore.ByEndpoint(endpoint, now-60.0*float64(history), math.Inf(1), appender)
	} else {
		metricStore.ByNameAndEndpoint(
			path,
			endpoint,
			now-60.0*float64(history),
			math.Inf(1),
			appender)
		if !isSingleton {
			metricStore.ByPrefixAndEndpoint(
				path+"/",
				endpoint,
				now-60.0*float64(history),
				math.Inf(1),
				appender)
		}

	}
	sortMetricsByPath(result)

	return
}

// byPath sorts metrics by path lexographically
type byPath messages.EndpointMetricsList

func (b byPath) Len() int {
	return len(b)
}

func (b byPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b byPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortMetricsByPath(result messages.EndpointMetricsList) {
	sort.Sort(byPath(result))
}

func encodeJson(w io.Writer, data interface{}, pretty bool) {
	if pretty {
		content, _ := json.Marshal(data)
		var buffer bytes.Buffer
		json.Indent(&buffer, content, "", "\t")
		buffer.WriteTo(w)
	} else {
		encoder := json.NewEncoder(w)
		encoder.Encode(data)
	}
}

// errorHandler provides the api/errors requests.
type errorHandler struct {
	ConnectionErrors *connectionErrorsType
}

func (h *errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	encodeJson(w, h.ConnectionErrors.GetErrors(), r.Form.Get("format") == "text")
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

// byEndpointHandler handles serving api/hosts requests
type byEndpointHandler struct {
	HostsPortsAndStore *datastructs.HostsPortsAndStore
	AppList            *datastructs.ApplicationList
}

func (h *byEndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	metricStore, endpoints := h.HostsPortsAndStore.Get()
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
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], "/"+hostNameAndPath[2]
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	app := h.AppList.ByName(name)
	if app == nil {
		httpError(w, 404)
		return
	}
	hostAndPort := fmt.Sprintf("%s:%d", host, app.Port())
	var data messages.EndpointMetricsList
	endpoint := endpoints[hostAndPort]
	if endpoint != nil {
		data = gatherDataForEndpoint(metricStore, endpoint, path, history, isSingleton)
	} else {
		data = make(messages.EndpointMetricsList, 0)
	}
	encodeJson(w, data, r.Form.Get("format") == "text")
}

var (
	aWriteError = errors.New("A bad write error.")
)

// A fake pstore.Writer implementation. This fake simply stalls 50ms with each
// write.
type stallWriter struct {
}

func (s stallWriter) IsTypeSupported(kind types.Type) bool {
	return true
}

func (s stallWriter) Write(records []pstore.Record) error {
	time.Sleep(50 * time.Millisecond)
	if rand.Float64() < 0.01 {
		return aWriteError
	}
	return nil
}

func newWriter() (result pstore.Writer, err error) {
	if *fKafkaConfigFile == "" {
		return stallWriter{}, nil
	}
	f, err := os.Open(*fKafkaConfigFile)
	if err != nil {
		return
	}
	defer f.Close()
	var config kafka.Config
	if err = config.Read(f); err != nil {
		return
	}
	return kafka.NewWriter(&config)
}

func createNoBlockPStore() *pstore.NoBlockPStore {
	writer, err := newWriter()
	if err != nil {
		log.Fatal(err)
	}
	result := pstore.NewNoBlockPStore(
		writer,
		*fPStoreBatchSize,
		*fPStoreChannelCapacity,
		time.Second)
	result.RegisterMetrics()
	return result
}

func createApplicationList() *datastructs.ApplicationList {
	f, err := os.Open(*fAppFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	builder := datastructs.NewApplicationListBuilder()
	if err := builder.ReadConfig(f); err != nil {
		log.Fatal(err)
	}
	return builder.Build()
}

func computePageCount() int {
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	if totalMemoryToUse > 0 {
		return int(totalMemoryToUse / uint64(*fBytesPerPage))
	}
	return *fPageCount
}

func updateEndpoints(
	machines *mdb.Mdb,
	apps []*datastructs.Application,
	hostsPortsAndStore *datastructs.HostsPortsAndStore) {
	_, endpoints := hostsPortsAndStore.Get()
	newEndpoints := make(datastructs.HostsAndPorts)
	addEndpoints(machines, apps, endpoints, newEndpoints)
	hostsPortsAndStore.Update(newEndpoints)
}

func addEndpoints(
	machines *mdb.Mdb,
	apps []*datastructs.Application,
	origHostsAndPorts,
	hostsAndPorts datastructs.HostsAndPorts) {
	for _, machine := range machines.Machines {
		for _, app := range apps {
			hostsAndPorts.AddIfAbsent(origHostsAndPorts, machine.Hostname, app.Port())
		}
	}
}

func initHostsPortsAndStore(
	appList *datastructs.ApplicationList,
	result *datastructs.HostsPortsAndStore) {
	mdbChannel := mdbd.StartMdbDaemon(
		*fMdbFile, log.New(os.Stderr, "", log.LstdFlags))
	firstEndpoints := make(datastructs.HostsAndPorts)
	addEndpoints(<-mdbChannel, appList.All(), nil, firstEndpoints)
	fmt.Println("Initialization started.")
	// Value interface + float64 = 24 bytes
	result.Init(
		(*fBytesPerPage)/24, computePageCount(), firstEndpoints)
	fmt.Println("Initialization complete.")
	firstStore, _ := result.Get()
	firstStore.RegisterMetrics()
	// Endpoint refresher goroutine
	go func() {
		for {
			updateEndpoints(<-mdbChannel, appList.All(), result)
		}
	}()
}

func startCollector(
	hostsPortsAndStore *datastructs.HostsPortsAndStore,
	noBlockPStore *pstore.NoBlockPStore,
	appList *datastructs.ApplicationList,
	appStats *datastructs.ApplicationStatuses,
	connectionErrors *connectionErrorsType) {
	collector.SetConcurrentPolls(*fPollCount)
	collector.SetConcurrentConnects(*fConnectionCount)

	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	collectionTimesDist := tricorder.NewGeometricBucketer(1e-4, 100.0).NewCumulativeDistribution()
	changedMetricsPerEndpointDist := tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()

	tricorder.RegisterMetric(
		"collector/collectionTimes",
		collectionTimesDist,
		units.Second,
		"Collection Times")
	tricorder.RegisterMetric(
		"collector/changedMetricsPerEndpoint",
		changedMetricsPerEndpointDist,
		units.None,
		"Changed metrics per sweep")
	tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration")
	programStartTime := time.Now()
	tricorder.RegisterMetric(
		"collector/elapsedTime",
		func() time.Duration {
			return time.Now().Sub(programStartTime)
		},
		units.Second,
		"elapsed time")

	// Metric collection goroutine. Collect metrics periodically.
	go func() {
		for {
			metricStore, endpoints := hostsPortsAndStore.Get()
			logger := &loggerType{
				Store:               metricStore,
				NoBlockPStore:       noBlockPStore,
				AppList:             appList,
				AppStats:            appStats,
				ConnectionErrors:    connectionErrors,
				CollectionTimesDist: collectionTimesDist,
				ChangedMetricsDist:  changedMetricsPerEndpointDist}
			sweepTime := time.Now()
			for _, endpoint := range endpoints {
				endpoint.Poll(sweepTime, logger)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			if sweepDuration < *fCollectionFrequency {
				time.Sleep((*fCollectionFrequency) - sweepDuration)
			}
		}
	}()

}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	noBlockPStore := createNoBlockPStore()
	applicationList := createApplicationList()
	applicationStats := datastructs.NewApplicationStatuses()
	connectionErrors := newConnectionErrorsType()
	var hostsPortsAndStore datastructs.HostsPortsAndStore
	initHostsPortsAndStore(applicationList, &hostsPortsAndStore)
	startCollector(
		&hostsPortsAndStore,
		noBlockPStore,
		applicationList,
		applicationStats,
		connectionErrors)

	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			AS:  applicationStats,
			HPS: &hostsPortsAndStore,
			AL:  applicationList,
		}})

	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{&byEndpointHandler{
				HostsPortsAndStore: &hostsPortsAndStore,
				AppList:            applicationList,
			}}))
	http.Handle(
		"/api/errors/",
		gzipHandler{&errorHandler{
			ConnectionErrors: connectionErrors,
		}},
	)
	if err := http.ListenAndServe(":8187", nil); err != nil {
		log.Fatal(err)
	}
}
