package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
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
	fPStoreUpdateFrequency = flag.Duration(
		"pstore_update_frequency",
		30*time.Second,
		"Amount of time between writing newest metrics to persistent storage")
	fPStoreBatchSize = flag.Int(
		"pstore_batch_size",
		1000,
		"Batch to write at least this many records to persistent storage")
	fKafkaConfigFile = flag.String(
		"kafka_config_file",
		"",
		"kafka configuration file")
	fLogBufLines = flag.Uint(
		"logbufLines", 1024, "Number of lines to store in the log buffer")
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

// metrics for pstoreHandlerType instances
type pstoreHandlerData struct {
	MetricsWritten   int64
	MetricsSkipped   int64
	WriteAttempts    int64
	SuccessfulWrites int64
	LastWriteError   string
	EndpointsVisited int64
}

// pstoreHandlerType implements store.Visitor.
// Its Visit method writes the latest value for each metric in the store to
// persistent storage. It stores the last value for each endpoint and
// metric it wrote to persistent storage so
// that it can avoid writing the same value twice.
// an pstoreHandlerType instance buffers writes so that each write to
// persistent storage includes at least fPStoreBatchSize metrics.
// pstoreHandlerType is NOT threadsafe.
type pstoreHandlerType struct {
	writer                  pstore.Writer
	appList                 *datastructs.ApplicationList
	iteratorsBeingWritten   []*store.Iterator
	toBeWritten             []pstore.Record
	batchSize               int
	skipped                 int
	idx                     int
	timeSpentWritingDist    *tricorder.CumulativeDistribution
	timeSpentCollectingDist *tricorder.CumulativeDistribution
	totalTimeSpentDist      *tricorder.CumulativeDistribution
	perMetricWriteDist      *tricorder.CumulativeDistribution
	ttWriter                *timeTakenWriter
	startTime               time.Time
	// Protects all fields below
	lock  sync.Mutex
	stats pstoreHandlerData
}

func newPStoreHandler(
	w pstore.Writer,
	appList *datastructs.ApplicationList,
	batchSize int) *pstoreHandlerType {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 1000.0)
	return &pstoreHandlerType{
		writer:                  w,
		appList:                 appList,
		iteratorsBeingWritten:   make([]*store.Iterator, batchSize),
		toBeWritten:             make([]pstore.Record, batchSize),
		batchSize:               batchSize,
		timeSpentWritingDist:    bucketer.NewCumulativeDistribution(),
		timeSpentCollectingDist: bucketer.NewCumulativeDistribution(),
		totalTimeSpentDist:      bucketer.NewCumulativeDistribution(),
		perMetricWriteDist:      bucketer.NewCumulativeDistribution(),
	}
}

func (p *pstoreHandlerType) StartVisit() {
	p.ttWriter = &timeTakenWriter{Writer: p.writer}
	p.startTime = time.Now()
}

func (p *pstoreHandlerType) EndVisit() {
	p.flush()
	totalTimeSpent := time.Now().Sub(p.startTime)
	p.timeSpentWritingDist.Add(p.ttWriter.TimeTaken)
	p.totalTimeSpentDist.Add(totalTimeSpent)
	p.timeSpentCollectingDist.Add(totalTimeSpent - p.ttWriter.TimeTaken)
}

func (p *pstoreHandlerType) Visit(
	theStore *store.Store, endpointId *collector.Endpoint) error {
	iterators := theStore.Iterators(endpointId)

	for i := range iterators {
		info := iterators[i].Info()
		// Skip over metrics that our store won't accept
		if !p.writer.IsTypeSupported(info.Kind()) {
			continue
		}
		ts, value, skipped := iterators[i].Next()
		for ; value != nil; ts, value, skipped = iterators[i].Next() {
			if p.isFull() {
				p.flush()
			}
			p.iteratorsBeingWritten[p.idx] = iterators[i]
			p.toBeWritten[p.idx] = pstore.Record{
				HostName:  endpointId.HostName(),
				Tags:      pstore.TagGroup{pstore.TagAppName: p.appList.ByPort(endpointId.Port()).Name()},
				Path:      strings.Replace(info.Path(), "/", "_", -1),
				Kind:      info.Kind(),
				Unit:      info.Unit(),
				Value:     value,
				Timestamp: trimessages.FloatToTime(ts)}
			p.skipped += skipped
			p.idx++
		}
	}
	p.logVisit()
	return nil
}

func (p *pstoreHandlerType) isFull() bool {
	return p.idx == p.batchSize
}

func (p *pstoreHandlerType) flush() {
	if p.idx == 0 {
		return
	}
	startTime := time.Now()
	err := p.ttWriter.Write(p.toBeWritten[:p.idx])
	p.perMetricWriteDist.Add(time.Now().Sub(startTime) / time.Duration(p.idx))

	if err != nil {
		p.logWriteError(err)
	} else {
		p.logWrite(p.idx, p.skipped)
		for i := 0; i < p.idx; i++ {
			p.iteratorsBeingWritten[i].Commit()
		}
	}
	p.skipped = 0
	p.idx = 0
}

func (p *pstoreHandlerType) RegisterMetrics() (err error) {
	if err = tricorder.RegisterMetric(
		"writer/writeTimePerMetric",
		p.perMetricWriteDist,
		units.Millisecond,
		"Time spent writing each metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/timeSpentCollecting",
		p.timeSpentCollectingDist,
		units.Second,
		"Time spent collecting metrics to write to persistent storage per sweep"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/timeSpentWriting",
		p.timeSpentWritingDist,
		units.Second,
		"Time spent writing metrics to persistent storage per sweep"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/totalTimeSpent",
		p.totalTimeSpentDist,
		units.Second,
		"total time spent per sweep"); err != nil {
		return
	}
	var data pstoreHandlerData
	region := tricorder.RegisterRegion(func() { p.collectData(&data) })
	if err = tricorder.RegisterMetricInRegion(
		"writer/metricsWritten",
		&data.MetricsWritten,
		region,
		units.None,
		"Number of metrics written to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/metricsSkipped",
		&data.MetricsSkipped,
		region,
		units.None,
		"Number of metrics skipped"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/writeAttempts",
		&data.WriteAttempts,
		region,
		units.None,
		"Number of attempts to write to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/successfulWrites",
		&data.SuccessfulWrites,
		region,
		units.None,
		"Number of successful writes to persistent storage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/successfulWriteRatio",
		func() float64 {
			return float64(data.SuccessfulWrites) / float64(data.WriteAttempts)
		},
		region,
		units.None,
		"Ratio of successful writes to write attempts"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/lastWriteError",
		&data.LastWriteError,
		region,
		units.None,
		"Last write error"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/endpointsVisited",
		&data.EndpointsVisited,
		region,
		units.None,
		"Number of endpoints visited"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerEndpoint",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.EndpointsVisited)
		},
		region,
		units.None,
		"Average metrics written per endpoint per cycle."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerBatch",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.SuccessfulWrites)
		},
		region,
		units.None,
		"Average metrics written per batch."); err != nil {
		return
	}
	return
}

func (p *pstoreHandlerType) logWrite(numWritten, numSkipped int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.MetricsWritten += int64(numWritten)
	p.stats.MetricsSkipped += int64(numSkipped)
	p.stats.WriteAttempts++
	p.stats.SuccessfulWrites++
}

func (p *pstoreHandlerType) logWriteError(err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.WriteAttempts++
	p.stats.LastWriteError = err.Error()
}

func (p *pstoreHandlerType) logVisit() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.stats.EndpointsVisited++
}

// collectData collects metrics about this instance.
// Although pstoreHandlerType instances are not threadsafe, the collectData
// method is threadsafe.
func (p *pstoreHandlerType) collectData(data *pstoreHandlerData) {
	p.lock.Lock()
	defer p.lock.Unlock()
	*data = p.stats
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store               *store.Store
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
	HostsPortsAndStore *datastructs.HostsPortsAndStore
	AppList            *datastructs.ApplicationList
}

func (h byEndpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

type timeTakenWriter struct {
	pstore.Writer
	TimeTaken time.Duration
}

func (t *timeTakenWriter) Write(records []pstore.Record) error {
	start := time.Now()
	result := t.Writer.Write(records)
	t.TimeTaken += time.Now().Sub(start)
	return result
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
	logger *log.Logger,
	result *datastructs.HostsPortsAndStore) {
	mdbChannel := mdbd.StartMdbDaemon(*fMdbFile, logger)
	firstEndpoints := make(datastructs.HostsAndPorts)
	addEndpoints(<-mdbChannel, appList.All(), nil, firstEndpoints)
	fmt.Println("Initialization started.")
	// Value interface + float64 = 24 bytes
	result.Init(
		(*fBytesPerPage)/24, computePageCount(), firstEndpoints)
	fmt.Println("Initialization complete.")
	firstStore, _ := result.Get()
	if err := firstStore.RegisterMetrics(); err != nil {
		log.Fatal(err)
	}
	// Endpoint refresher goroutine
	go func() {
		for {
			updateEndpoints(<-mdbChannel, appList.All(), result)
		}
	}()
}

func startCollector(
	hostsPortsAndStore *datastructs.HostsPortsAndStore,
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

func startPStoreLoop(
	hostsPortsAndStore *datastructs.HostsPortsAndStore,
	appList *datastructs.ApplicationList,
	logger *log.Logger) {
	go func() {
		writer, err := newWriter()
		if err != nil {
			logger.Println(err)
			return
		}
		pstoreHandler := newPStoreHandler(writer, appList, *fPStoreBatchSize)
		if err := pstoreHandler.RegisterMetrics(); err != nil {
			log.Fatal(err)
		}

		// persistent storage writing goroutine. Write every 30s by default.
		// Notice that this single goroutine handles all the persistent
		// storage writing as multiple goroutines must not access the
		// pstoreHandler instance. accessing pstoreHandler metrics is the
		// one exception to this rule.
		for {
			metricStore, _ := hostsPortsAndStore.Get()
			writeTime := time.Now()
			pstoreHandler.StartVisit()
			metricStore.VisitAllEndpoints(pstoreHandler)
			pstoreHandler.EndVisit()
			writeDuration := time.Now().Sub(writeTime)
			if writeDuration < *fPStoreUpdateFrequency {
				time.Sleep((*fPStoreUpdateFrequency) - writeDuration)
			}
		}
	}()
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	circularBuffer := logbuf.New(*fLogBufLines)
	logger := log.New(circularBuffer, "", log.LstdFlags)
	applicationList := createApplicationList()
	applicationStats := datastructs.NewApplicationStatuses()
	connectionErrors := newConnectionErrorsType()
	var hostsPortsAndStore datastructs.HostsPortsAndStore
	initHostsPortsAndStore(applicationList, logger, &hostsPortsAndStore)
	startCollector(
		&hostsPortsAndStore,
		applicationList,
		applicationStats,
		connectionErrors)
	startPStoreLoop(
		&hostsPortsAndStore,
		applicationList,
		logger)

	http.Handle(
		"/",
		gzipHandler{&splash.Handler{
			AS:  applicationStats,
			HPS: &hostsPortsAndStore,
			Log: circularBuffer,
		}})
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
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}
