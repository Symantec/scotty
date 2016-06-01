package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/Dominator/lib/mdb/mdbd"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/apps/scotty/splash"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/influx"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/sysmemory"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	kPStoreIteratorName       = "pstore"
	kCollectorIteratorName    = "collector"
	kLookAheadWritingToPStore = 5
)

var (
	gAlloc []byte
)

var (
	fPort = flag.Int(
		"portNum",
		6980,
		"Port number for scotty.")
	fBytesPerPage = flag.Int(
		"bytes_per_page",
		1024,
		"Space for new metrics for each endpoint in records")
	fPageCount = flag.Int(
		"page_count",
		30*1000*1000,
		"Total page count")
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
	fPStoreUpdateFrequency = flag.Duration(
		"pstore_update_frequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
	fKafkaConfigFile = flag.String(
		"kafka_config_file",
		"",
		"kafka configuration file")
	fInfluxConfigFile = flag.String(
		"influx_config_file",
		"",
		"influx configuration file")
	fLogBufLines = flag.Uint(
		"logbufLines", 1024, "Number of lines to store in the log buffer")
	fPidFile = flag.String(
		"pidfile", "", "Name of file to write my PID to")
	fThreshhold = flag.Float64(
		"inactiveThreshhold", 0.1, "Ratio of inactive pages needed to begin purging inactive pages")
	fDegree = flag.Int(
		"degree", 10, "Degree of btree")
	fPagePercentage = flag.Float64(
		"page_percentage", 50.0, "Percentage of allocated memory used for pages")
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
		Timestamp: duration.SinceEpoch(timestamp).String(),
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

// pstoreHandlerType implements store.Visitor.
// Its Visit method writes the latest values for each endpoint to
// persistent storage. It uses a pstore.Consumer to buffer writes to
// persistnt storage and to avoid writing the same value twice.
// pstoreHandlerType is NOT threadsafe.
type pstoreHandlerType struct {
	name                string
	consumer            *pstore.ConsumerWithMetrics
	appList             *datastructs.ApplicationList
	startTime           time.Time
	totalTimeSpentDist  *tricorder.CumulativeDistribution
	perMetricWriteTimes *tricorder.CumulativeDistribution
}

func newPStoreHandler(
	appList *datastructs.ApplicationList,
	consumer *pstore.ConsumerWithMetricsBuilder) *pstoreHandlerType {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 1000.0)
	perMetricWriteTimes := bucketer.NewCumulativeDistribution()
	consumer.SetPerMetricWriteTimeDist(perMetricWriteTimes)
	return &pstoreHandlerType{
		consumer:            consumer.Build(),
		appList:             appList,
		totalTimeSpentDist:  bucketer.NewCumulativeDistribution(),
		perMetricWriteTimes: perMetricWriteTimes,
	}
}

func (p *pstoreHandlerType) Name() string {
	return p.consumer.Name()
}

func (p *pstoreHandlerType) StartVisit() {
	p.startTime = time.Now()
}

func (p *pstoreHandlerType) EndVisit() {
	p.consumer.Flush()
	totalTime := time.Now().Sub(p.startTime)
	p.totalTimeSpentDist.Add(totalTime)
}

func (p *pstoreHandlerType) Visit(
	theStore *store.Store, endpointId interface{}) error {

	hostName := endpointId.(*collector.Endpoint).HostName()
	port := endpointId.(*collector.Endpoint).Port()
	appName := p.appList.ByPort(port).Name()
	iterator := theStore.NamedIteratorForEndpoint(
		fmt.Sprintf("%s/%s", kPStoreIteratorName, p.Name()),
		endpointId,
		kLookAheadWritingToPStore,
	)
	p.consumer.Write(iterator, hostName, appName)
	return nil
}

func (p *pstoreHandlerType) Metrics(m *pstore.ConsumerMetrics) {
	p.consumer.MetricsStore().Metrics(m)
}

func (p *pstoreHandlerType) ConsumerMetricsStore() *pstore.ConsumerMetricsStore {
	return p.consumer.MetricsStore()
}

func (p *pstoreHandlerType) RegisterMetrics() (err error) {
	var attributes pstore.ConsumerAttributes
	p.consumer.Attributes(&attributes)
	var data pstore.ConsumerMetrics
	group := tricorder.NewGroup()
	group.RegisterUpdateFunc(
		func() time.Time {
			p.Metrics(&data)
			return time.Now()
		})
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/totalTimeSpent", p.Name()),
		p.totalTimeSpentDist,
		units.Second,
		"total time spent per sweep"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/writeTimePerMetric", p.Name()),
		p.perMetricWriteTimes,
		units.Millisecond,
		"Time spent writing each metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/maxRecordsPerSecond", p.Name()),
		attributes.TotalRecordsPerSecond,
		units.None,
		"Max records per second to write. 0 means unlimited"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/concurrency", p.Name()),
		&attributes.Concurrency,
		units.None,
		"Number of writing goroutines"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		fmt.Sprintf("writer/%s/batchSize", p.Name()),
		&attributes.BatchSize,
		units.None,
		"This many records written each time"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/valuesWritten", p.Name()),
		&data.ValuesWritten,
		group,
		units.None,
		"Number of values written to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/valuesNotWritten", p.Name()),
		&data.ValuesNotWritten,
		group,
		units.None,
		"Number of values not written to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/writeAttempts", p.Name()),
		&data.WriteAttempts,
		group,
		units.None,
		"Number of attempts to write to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/successfulWrites", p.Name()),
		&data.SuccessfulWrites,
		group,
		units.None,
		"Number of successful writes to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/successfulWriteRatio", p.Name()),
		data.SuccessfulWriteRatio,
		group,
		units.None,
		"Ratio of successful writes to write attempts"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		fmt.Sprintf("writer/%s/lastWriteError", p.Name()),
		&data.LastWriteError,
		group,
		units.None,
		"Last write error"); err != nil {
		return
	}
	return
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store               *store.Store
	AppList             *datastructs.ApplicationList
	AppStats            *datastructs.ApplicationStatuses
	ConnectionErrors    *connectionErrorsType
	CollectionTimesDist *tricorder.CumulativeDistribution
	ByProtocolDist      map[string]*tricorder.CumulativeDistribution
	ChangedMetricsDist  *tricorder.CumulativeDistribution
	newValuesConsumer   pstore.ConsumerMetricsStoreList
}

func (l *loggerType) LogStateChange(
	e *collector.Endpoint, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		timeTaken := newS.TimeSpentConnecting()
		timeTaken += newS.TimeSpentPolling()
		timeTaken += newS.TimeSpentWaitingToConnect()
		timeTaken += newS.TimeSpentWaitingToPoll()
		l.CollectionTimesDist.Add(timeTaken)
		dist := l.ByProtocolDist[e.Connector().Name()]
		if dist != nil {
			dist.Add(timeTaken)
		}
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
	e *collector.Endpoint, list metrics.List, state *collector.State) {
	ts := duration.TimeToFloat(state.Timestamp())
	added, ok := l.Store.AddBatch(
		e,
		ts,
		list)
	if ok {
		l.AppStats.LogChangedMetricCount(e, added)
		l.ChangedMetricsDist.Add(float64(added))
		if l.newValuesConsumer != nil {
			namedIterator := l.Store.NamedIteratorForEndpoint(
				kCollectorIteratorName, e, 0)
			l.newValuesConsumer.UpdateCounts(namedIterator)
		}
	}
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
	endpointMetrics *messages.EndpointMetricList
	lastInfo        *store.MetricInfo
	lastMetric      *messages.EndpointMetric
}

// newEndpointMetricsAppender creates a endpointMetricsAppender that appends
// to result.
func newEndpointMetricsAppender(result *messages.EndpointMetricList) *endpointMetricsAppender {
	return &endpointMetricsAppender{endpointMetrics: result}
}

func (a *endpointMetricsAppender) Append(r *store.Record) bool {
	if r.Info != a.lastInfo {
		a.lastInfo = r.Info
		_, jsonKind := trimessages.AsJson(nil, a.lastInfo.Kind(), a.lastInfo.Unit())
		a.lastMetric = &messages.EndpointMetric{
			Path:        a.lastInfo.Path(),
			Kind:        jsonKind,
			Description: a.lastInfo.Description(),
			Bits:        a.lastInfo.Bits(),
			Unit:        a.lastInfo.Unit()}
		*a.endpointMetrics = append(*a.endpointMetrics, a.lastMetric)
	}
	jsonValue, _ := trimessages.AsJson(r.Value, a.lastInfo.Kind(), a.lastInfo.Unit())
	newTimestampedValue := &messages.TimestampedValue{
		Timestamp: duration.SinceEpochFloat(r.TimeStamp).String(),
		Value:     jsonValue,
		Active:    r.Active,
	}
	a.lastMetric.Values = append(a.lastMetric.Values, newTimestampedValue)
	return true
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
	isSingleton bool) (result messages.EndpointMetricList) {
	result = make(messages.EndpointMetricList, 0)
	now := duration.TimeToFloat(time.Now())
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

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	AS *datastructs.ApplicationStatuses
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
		host, name, path = hostNameAndPath[0], hostNameAndPath[1], "/"+hostNameAndPath[2]
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	endpoint, metricStore := h.AS.EndpointIdByHostAndName(host, name)
	if endpoint == nil {
		httpError(w, 404)
		return
	}
	data := gatherDataForEndpoint(
		metricStore, endpoint, path, history, isSingleton)
	encodeJson(w, data, r.Form.Get("format") == "text")
}

func newPStoreConsumers() (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	if *fKafkaConfigFile != "" {
		return kafka.ConsumerBuildersFromFile(*fKafkaConfigFile)
	}
	if *fInfluxConfigFile != "" {
		return influx.ConsumerBuildersFromFile(
			*fInfluxConfigFile)
	}
	return
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

func hostNames(machines []mdb.Machine) (result []string) {
	result = make([]string, len(machines))
	for i := range machines {
		result[i] = machines[i].Hostname
	}
	return
}

func totalMemoryUsed() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}

func createApplicationStats(
	appList *datastructs.ApplicationList,
	logger *log.Logger) *datastructs.ApplicationStatuses {
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	var astore *store.Store
	fmt.Println("Initialization started.")
	if totalMemoryToUse > 0 {
		gAlloc = make([]byte, totalMemoryToUse)
		// Do something with our slice so that compiler doesn't
		// complain or optimise the slice allocation away
		fmt.Printf(
			"Total memory: %d\n",
			totalMemoryToUse+uint64(gAlloc[totalMemoryToUse-1]+gAlloc[0]))
		logger.Printf("totalMemoryInUse: %d\n", totalMemoryUsed())
		gAlloc = nil
		now := time.Now()
		runtime.GC()
		logger.Printf("GCTime: %v; totalMemoryInUse: %d\n", time.Since(now), totalMemoryUsed())
		pagesToUse := int(float64(totalMemoryToUse) / float64(*fBytesPerPage) * (*fPagePercentage) / 100.0)
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage, pagesToUse, *fThreshhold, *fDegree)
		tricorder.RegisterMetric(
			"proc/memory/target-alloc",
			&totalMemoryToUse,
			units.Byte,
			"Target memory usage")
		go func() {
			for {
				time.Sleep(time.Minute)
				totalMemoryInUse := totalMemoryUsed()
				threshhold := uint64(float64(totalMemoryToUse) * 0.9)
				if totalMemoryInUse > threshhold {
					logger.Printf("threshhold: %d totalMemoryInUse: %d\n", threshhold, totalMemoryInUse)
					now := time.Now()
					runtime.GC()
					logger.Printf("GCTime: %v; totalMemoryInUse: %d\n", time.Since(now), totalMemoryUsed())
				}
			}
		}()
	} else {
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage, *fPageCount, *fThreshhold, *fDegree)
	}
	if err := astore.RegisterMetrics(); err != nil {
		log.Fatal(err)
	}
	stats := datastructs.NewApplicationStatuses(appList, astore)
	mdbChannel := mdbd.StartMdbDaemon(*fMdbFile, logger)
	machines := <-mdbChannel
	stats.MarkHostsActiveExclusively(
		duration.TimeToFloat(time.Now()),
		hostNames(machines.Machines))
	fmt.Println("Initialization complete.")
	// Endpoint refresher goroutine
	go func() {
		for {
			machines := <-mdbChannel
			stats.MarkHostsActiveExclusively(
				duration.TimeToFloat(time.Now()),
				hostNames(machines.Machines))
		}
	}()
	return stats
}

func startCollector(
	appStats *datastructs.ApplicationStatuses,
	connectionErrors *connectionErrorsType,
	newValuesConsumer pstore.ConsumerMetricsStoreList) {
	collector.SetConcurrentPolls(*fPollCount)
	collector.SetConcurrentConnects(*fConnectionCount)

	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	collectionBucketer := tricorder.NewGeometricBucketer(1e-4, 100.0)
	collectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	tricorderCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	snmpCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	changedMetricsPerEndpointDist := tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()

	tricorder.RegisterMetric(
		"collector/collectionTimes",
		collectionTimesDist,
		units.Second,
		"Collection Times")
	tricorder.RegisterMetric(
		"collector/collectionTimes_tricorder",
		tricorderCollectionTimesDist,
		units.Second,
		"Tricorder Collection Times")
	tricorder.RegisterMetric(
		"collector/collectionTimes_snmp",
		snmpCollectionTimesDist,
		units.Second,
		"SNMP Collection Times")
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

	byProtocolDist := map[string]*tricorder.CumulativeDistribution{
		"tricorder": tricorderCollectionTimesDist,
		"snmp":      snmpCollectionTimesDist,
	}

	// Metric collection goroutine. Collect metrics periodically.
	go func() {
		for {
			endpoints, metricStore := appStats.ActiveEndpointIds()
			logger := &loggerType{
				Store:               metricStore,
				AppStats:            appStats,
				ConnectionErrors:    connectionErrors,
				CollectionTimesDist: collectionTimesDist,
				ByProtocolDist:      byProtocolDist,
				ChangedMetricsDist:  changedMetricsPerEndpointDist,
				newValuesConsumer:   newValuesConsumer,
			}
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

func startPStoreLoops(
	stats *datastructs.ApplicationStatuses,
	consumerBuilders []*pstore.ConsumerWithMetricsBuilder,
	logger *log.Logger) pstore.ConsumerMetricsStoreList {
	result := make(pstore.ConsumerMetricsStoreList, len(consumerBuilders))
	for i := range result {
		pstoreHandler := newPStoreHandler(
			stats.ApplicationList(),
			consumerBuilders[i])
		result[i] = pstoreHandler.ConsumerMetricsStore()
		if err := pstoreHandler.RegisterMetrics(); err != nil {
			log.Fatal(err)
		}
		go func(handler *pstoreHandlerType) {
			// persistent storage writing goroutine. Write every 30s by default.
			// Notice that this single goroutine handles all the persistent
			// storage writing as multiple goroutines must not access the
			// pstoreHandler instance. accessing pstoreHandler metrics is the
			// one exception to this rule.
			for {
				metricStore := stats.Store()
				writeTime := time.Now()
				handler.StartVisit()
				metricStore.VisitAllEndpoints(handler)
				handler.EndVisit()
				writeDuration := time.Now().Sub(writeTime)
				if writeDuration < *fPStoreUpdateFrequency {
					time.Sleep((*fPStoreUpdateFrequency) - writeDuration)
				}
			}
		}(pstoreHandler)
	}
	return result
}

func gracefulCleanup() {
	os.Remove(*fPidFile)
	os.Exit(1)
}

func writePidfile() {
	file, err := os.Create(*fPidFile)
	if err != nil {
		return
	}
	defer file.Close()
	fmt.Fprintln(file, os.Getpid())
}

func handleSignals(logger *log.Logger) {
	if *fPidFile == "" {
		return
	}
	sigtermChannel := make(chan os.Signal)
	signal.Notify(sigtermChannel, syscall.SIGTERM, syscall.SIGINT)
	writePidfile()
	go func() {
		for {
			select {
			case <-sigtermChannel:
				gracefulCleanup()
			}
		}
	}()
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	circularBuffer := logbuf.New(*fLogBufLines)
	logger := log.New(circularBuffer, "", log.LstdFlags)
	handleSignals(logger)
	// Read configs early so that we will fail fast.
	consumerBuilders, err := newPStoreConsumers()
	if err != nil {
		log.Println(err)
		logger.Println(err)
	}
	appList := createApplicationList()
	applicationStats := createApplicationStats(appList, logger)
	connectionErrors := newConnectionErrorsType()
	if consumerBuilders == nil {
		startCollector(
			applicationStats, connectionErrors, nil)
	} else {
		consumerMetricStoreList := startPStoreLoops(
			applicationStats,
			consumerBuilders,
			logger)
		startCollector(
			applicationStats,
			connectionErrors,
			consumerMetricStoreList)
	}

	http.Handle(
		"/",
		gzipHandler{&splash.Handler{
			AS:  applicationStats,
			Log: circularBuffer,
		}})
	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			AS: applicationStats,
		}})
	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{&byEndpointHandler{
				AS: applicationStats,
			}}))
	http.Handle(
		"/api/errors/",
		gzipHandler{&errorHandler{
			ConnectionErrors: connectionErrors,
		}},
	)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}
