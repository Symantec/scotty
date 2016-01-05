package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/lmm"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	trimessages "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	aTopic                = "metricTopic"
	totalNumberOfMachines = 10000
)

var (
	fBufferSizePerMachine int
	fSpacingPerMachine    int
	fHostFile             string
	fPollCount            int
	fConnectionCount      int
	fCollectionFrequency  time.Duration
	fLmmUpdateFrequency   time.Duration
	fLmmBatchSize         int
)

var (
	machineNames []string
	machinePorts []int
	metricStore  *store.Store
)

func machineNameAndPort(i int) (name string, port int) {
	length := len(machineNames)
	return fmt.Sprintf(
			"%s*%d",
			machineNames[i%length],
			i),
		machinePorts[i%length]
}

// lmmWriterType implementations write to LMM
type lmmWriterType interface {
	Write(records []*store.Record) error
}

// metrics for lmmHandlerType instances
type lmmHandlerData struct {
	MetricsWritten  int64
	WriteCount      int64
	MachinesVisited int64
}

// lmmHandlerType implements store.Visitor.
// Its Visit method writes the latest value for each metric in the store to LMM.
// It stores the last value for each machine and metric it wrote to LMM so
// that it can avoid writing the same value twice.
// an lmmHandlerType instance buffers writes so that each write to LMM
// includes at least fLmmBatchSize metrics.
// lmmHandlerType is NOT threadsafe.
type lmmHandlerType struct {
	writer                  lmmWriterType
	lastValues              map[*collector.Machine]map[*store.MetricInfo]interface{}
	lastValuesThisMachine   map[*store.MetricInfo]interface{}
	toBeWritten             []*store.Record
	session                 *store.Session
	timeSpentWritingDist    *tricorder.Distribution
	timeSpentCollectingDist *tricorder.Distribution
	totalTimeSpentDist      *tricorder.Distribution
	perBatchWriteDist       *tricorder.Distribution
	ttWriter                *timeTakenLmmWriter
	startTime               time.Time
	// Protects all fields below
	lock  sync.Mutex
	stats lmmHandlerData
}

func newLmmHandler(w lmmWriterType) *lmmHandlerType {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 1000.0)
	return &lmmHandlerType{
		writer:                  w,
		lastValues:              make(map[*collector.Machine]map[*store.MetricInfo]interface{}),
		timeSpentWritingDist:    bucketer.NewDistribution(),
		timeSpentCollectingDist: bucketer.NewDistribution(),
		totalTimeSpentDist:      bucketer.NewDistribution(),
		perBatchWriteDist:       bucketer.NewDistribution(),
	}
}

// Do not call this directly!
func (l *lmmHandlerType) Append(r *store.Record) bool {
	kind := r.Info().Kind()
	if !lmm.IsTypeSupported(kind) {
		return false
	}
	if l.lastValuesThisMachine[r.Info()] == r.Value() {
		return false
	}
	l.lastValuesThisMachine[r.Info()] = r.Value()
	l.toBeWritten = append(l.toBeWritten, r)
	return true
}

func (l *lmmHandlerType) StartVisit() {
	l.ttWriter = &timeTakenLmmWriter{Writer: l.writer}
	l.startTime = time.Now()
}

func (l *lmmHandlerType) EndVisit() {
	l.flush()
	totalTimeSpent := time.Now().Sub(l.startTime)
	l.timeSpentWritingDist.Add(l.ttWriter.TimeTaken)
	l.totalTimeSpentDist.Add(totalTimeSpent)
	l.timeSpentCollectingDist.Add(totalTimeSpent - l.ttWriter.TimeTaken)
}

func (l *lmmHandlerType) Visit(
	theStore *store.Store, machineId *collector.Machine) error {
	// Be sure we have a session
	if l.session == nil {
		l.session = theStore.NewSession()
	}
	// Get the last known values for this machine creating the map
	// if necessary.
	l.lastValuesThisMachine = l.lastValues[machineId]
	if l.lastValuesThisMachine == nil {
		l.lastValuesThisMachine = make(
			map[*store.MetricInfo]interface{})
		l.lastValues[machineId] = l.lastValuesThisMachine
	}

	// Get the latest values to write to lmm, but get only the
	// values that changed.
	theStore.LatestByMachine(l.session, machineId, l)

	// If we have enough values to write, write them out to LMM.
	if len(l.toBeWritten) >= fLmmBatchSize {
		l.flush()
	}
	l.logVisit()
	return nil
}

func (l *lmmHandlerType) flush() {
	// If we have no session, check that we have no records to be
	// written and do nothing.
	if l.session == nil {
		if len(l.toBeWritten) > 0 {
			panic("No session but metrics to be written to LMM")
		}
		return
	}
	// Part of flushing is closing the session
	defer func() {
		l.session.Close()
		l.session = nil
	}()
	if len(l.toBeWritten) == 0 {
		return
	}
	startTime := time.Now()
	if err := l.ttWriter.Write(l.toBeWritten); err != nil {
		log.Fatal(err)
	}
	l.perBatchWriteDist.Add(time.Now().Sub(startTime))
	l.logWrite(len(l.toBeWritten))

	// Make toBeWritten be empty while saving on memory allocation
	l.toBeWritten = l.toBeWritten[:0]

}

func (l *lmmHandlerType) RegisterMetrics() {
	tricorder.RegisterMetric(
		"writer/writeTimePerBatch",
		l.perBatchWriteDist,
		units.Millisecond,
		"Time spent writing each batch")
	tricorder.RegisterMetric(
		"writer/timeSpentCollecting",
		l.timeSpentCollectingDist,
		units.Second,
		"Time spent collecting metrics to write to Lmm per sweep")
	tricorder.RegisterMetric(
		"writer/timeSpentWriting",
		l.timeSpentWritingDist,
		units.Second,
		"Time spent writing metrics to Lmm per sweep")
	tricorder.RegisterMetric(
		"writer/totalTimeSpent",
		l.totalTimeSpentDist,
		units.Second,
		"total time spent per sweep")
	var data lmmHandlerData
	region := tricorder.RegisterRegion(func() { l.collectData(&data) })
	tricorder.RegisterMetricInRegion(
		"writer/metricsWritten",
		&data.MetricsWritten,
		region,
		units.None,
		"Number of metrics written to LMM")
	tricorder.RegisterMetricInRegion(
		"writer/writeCount",
		&data.WriteCount,
		region,
		units.None,
		"Number of metrics written to LMM")
	tricorder.RegisterMetricInRegion(
		"writer/machinesVisited",
		&data.MachinesVisited,
		region,
		units.None,
		"Number of machines visited")
	tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerMachine",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.MachinesVisited)
		},
		region,
		units.None,
		"Average metrics written per machine per cycle.")
	tricorder.RegisterMetricInRegion(
		"writer/averageMetricsPerBatch",
		func() float64 {
			return float64(data.MetricsWritten) / float64(data.WriteCount)
		},
		region,
		units.None,
		"Average metrics written per machine per cycle.")
}

func (l *lmmHandlerType) logWrite(numWritten int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.stats.MetricsWritten += int64(numWritten)
	l.stats.WriteCount++
}

func (l *lmmHandlerType) logVisit() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.stats.MachinesVisited++
}

// collectData collects metrics about this instance.
// Although lmmHandlerType instances are not threadsafe, the collectData
// method is threadsafe.
func (l *lmmHandlerType) collectData(data *lmmHandlerData) {
	l.lock.Lock()
	defer l.lock.Unlock()
	*data = l.stats
}

// Statistics for health metric collection.
type collectorData struct {
	WaitingToConnectCount int
	ConnectingCount       int
	WaitingToPollCount    int
	PollingCount          int
	SyncedCount           int
	FailedToConnectCount  int
	FailedToPollCount     int
	StoredMetricsCount    int64
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type logger struct {
	collectionTimes    *tricorder.Distribution
	lock               sync.Mutex
	statusMap          map[collector.Status]int
	errorMap           map[*collector.Machine]*messages.Error
	storedMetricsCount int64
}

func newLogger() *logger {
	bucketer := tricorder.NewGeometricBucketer(1e-4, 100.0)
	return &logger{
		statusMap:       make(map[collector.Status]int),
		errorMap:        make(map[*collector.Machine]*messages.Error),
		collectionTimes: bucketer.NewDistribution()}
}

func (l *logger) LogStateChange(
	m *collector.Machine, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		l.collectionTimes.Add(
			newS.TimeSpentConnecting() + newS.TimeSpentPolling() + newS.TimeSpentWaitingToConnect() + newS.TimeSpentWaitingToPoll())
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	if oldS != nil {
		l.statusMap[oldS.Status()]--
	}
	l.statusMap[newS.Status()]++
}

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

func (l *logger) GetErrors() (result messages.ErrorList) {
	result = make(messages.ErrorList, 0)
	l.lock.Lock()
	for machine := range l.errorMap {
		result = append(result, l.errorMap[machine])
	}
	l.lock.Unlock()
	sort.Sort(byHostName(result))
	return
}

func (l *logger) LogError(m *collector.Machine, err error, state *collector.State) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if err == nil {
		delete(l.errorMap, m)
	} else {
		l.errorMap[m] = &messages.Error{
			HostName:  m.HostName(),
			Timestamp: trimessages.SinceEpoch(state.Timestamp()).String(),
			Error:     err.Error(),
		}
	}
}

func (l *logger) LogResponse(
	m *collector.Machine, metrics trimessages.MetricList, state *collector.State) {
	ts := trimessages.TimeToFloat(state.Timestamp())
	for i := range metrics {
		if metrics[i].Kind == types.Dist {
			continue
		}
		if metricStore.Add(m, ts, metrics[i]) {
			l.incrMetricCount()
		}
	}
}

func (l *logger) RegisterMetrics() {
	tricorder.RegisterMetric(
		"collector/collectionTimes",
		l.collectionTimes,
		units.Second,
		"Collection Times")
	var data collectorData
	region := tricorder.RegisterRegion(func() { l.collectData(&data) })
	tricorder.RegisterMetricInRegion(
		"collector/waitingToConnect",
		&data.WaitingToConnectCount,
		region,
		units.None,
		"Number of goroutines waiting to connect")
	tricorder.RegisterMetricInRegion(
		"collector/connecting",
		&data.ConnectingCount,
		region,
		units.None,
		"Number of goroutines connecting")
	tricorder.RegisterMetricInRegion(
		"collector/waitingToPoll",
		&data.WaitingToPollCount,
		region,
		units.None,
		"Number of goroutines waiting to poll")
	tricorder.RegisterMetricInRegion(
		"collector/polling",
		&data.PollingCount,
		region,
		units.None,
		"Number of goroutines polling")
	tricorder.RegisterMetricInRegion(
		"collector/synced",
		&data.SyncedCount,
		region,
		units.None,
		"Number of machines synced")
	tricorder.RegisterMetricInRegion(
		"collector/connectFailures",
		&data.FailedToConnectCount,
		region,
		units.None,
		"Number of connection failures")
	tricorder.RegisterMetricInRegion(
		"collector/pollFailures",
		&data.FailedToPollCount,
		region,
		units.None,
		"Number of poll failures")
	tricorder.RegisterMetricInRegion(
		"collector/storedMetricsCount",
		&data.StoredMetricsCount,
		region,
		units.None,
		"Number of metrics stored")
}

func (l *logger) incrMetricCount() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.storedMetricsCount++
}

// collectData fetches the statistics.
func (l *logger) collectData(data *collectorData) {
	l.lock.Lock()
	defer l.lock.Unlock()
	data.WaitingToConnectCount =
		l.statusMap[collector.WaitingToConnect]
	data.ConnectingCount = l.statusMap[collector.Connecting]
	data.WaitingToPollCount = l.statusMap[collector.WaitingToPoll]
	data.PollingCount = l.statusMap[collector.Polling]
	data.SyncedCount = l.statusMap[collector.Synced]
	data.FailedToConnectCount =
		l.statusMap[collector.FailedToConnect]
	data.FailedToPollCount = l.statusMap[collector.FailedToPoll]
	data.StoredMetricsCount = l.storedMetricsCount
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

// machineMetricsAppender is an implementation of store.Appender that
// appends to a messages.MachineMetricsList.
// machineMetricsAppender is NOT threadsafe.
// No session instance required to use as this implementation of Appender
// copies from store.Record instances rather than holding onto pointers.
type machineMetricsAppender struct {
	machineMetrics *messages.MachineMetricsList
	lastInfo       *store.MetricInfo
	lastMetric     *messages.MachineMetrics
}

// newMachineMetricsAppender creates a machineMetricsAppender that appends
// to result.
func newMachineMetricsAppender(result *messages.MachineMetricsList) *machineMetricsAppender {
	return &machineMetricsAppender{machineMetrics: result}
}

// This implementation of Append always returns false as it copies data
// from passed store.Record rather than holding onto the pointer.
func (a *machineMetricsAppender) Append(r *store.Record) bool {
	if r.Info() != a.lastInfo {
		a.lastInfo = r.Info()
		_, jsonKind := trimessages.AsJson(nil, a.lastInfo.Kind(), a.lastInfo.Unit())
		a.lastMetric = &messages.MachineMetrics{
			Path:        a.lastInfo.Path(),
			Kind:        jsonKind,
			Description: a.lastInfo.Description(),
			Bits:        a.lastInfo.Bits(),
			Unit:        a.lastInfo.Unit()}
		*a.machineMetrics = append(*a.machineMetrics, a.lastMetric)
	}
	jsonValue, _ := trimessages.AsJson(r.Value(), a.lastInfo.Kind(), a.lastInfo.Unit())
	newTimestampedValue := &messages.TimestampedValue{
		Timestamp: trimessages.SinceEpochFloat(r.Timestamp()).String(),
		Value:     jsonValue,
	}
	a.lastMetric.Values = append(a.lastMetric.Values, newTimestampedValue)
	return false
}

// gatherDataForMachine serves api/hosts pages.
// machine is the machine from which we are getting historical metrics.
// path is the path of the metrics or the empty string for all metrics
// history is the amount of time to go back in minutes.
// If isSingleton is true, fetched metrics have to match path exactly.
// Otherwise fetched metrics have to be found underneath path.
// On no match, gatherDataForMachine returns an empty
// messages.MachineMetricsList instance
func gatherDataForMachine(
	machine *collector.Machine,
	path string,
	history int,
	isSingleton bool) (result messages.MachineMetricsList) {
	result = make(messages.MachineMetricsList, 0)
	now := trimessages.TimeToFloat(time.Now())
	appender := newMachineMetricsAppender(&result)
	if path == "" {
		metricStore.ByMachine(nil, machine, now-60.0*float64(history), math.Inf(1), appender)
	} else {
		metricStore.ByNameAndMachine(
			nil,
			path,
			machine,
			now-60.0*float64(history),
			math.Inf(1),
			appender)
		if !isSingleton {
			metricStore.ByPrefixAndMachine(
				nil,
				path+"/",
				machine,
				now-60.0*float64(history),
				math.Inf(1),
				appender)
		}

	}
	sortMetricsByPath(result)

	return
}

// byPath sorts metrics by path lexographically
type byPath messages.MachineMetricsList

func (b byPath) Len() int {
	return len(b)
}

func (b byPath) Less(i, j int) bool {
	return b[i].Path < b[j].Path
}

func (b byPath) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortMetricsByPath(result messages.MachineMetricsList) {
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
	logger *logger
}

func (h errorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	encodeJson(w, h.logger.GetErrors(), r.Form.Get("format") == "text")
}

// byMachineHandler handles serving api/hosts requests
type byMachineHandler map[string]*collector.Machine

func (h byMachineHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "application/json")
	hostAndPath := strings.SplitN(r.URL.Path, "/", 2)
	var host string
	var path string
	if len(hostAndPath) == 1 {
		host, path = hostAndPath[0], ""
	} else {
		host, path = hostAndPath[0], "/"+hostAndPath[1]
	}
	history, err := strconv.Atoi(r.Form.Get("history"))
	isSingleton := r.Form.Get("singleton") != ""
	if err != nil {
		history = 60
	}
	machine := h[host]
	var data messages.MachineMetricsList
	if machine != nil {
		data = gatherDataForMachine(machine, path, history, isSingleton)
	} else {
		data = make(messages.MachineMetricsList, 0)
	}
	encodeJson(w, data, r.Form.Get("format") == "text")
}

type timeTakenLmmWriter struct {
	Writer    lmmWriterType
	TimeTaken time.Duration
}

func (t *timeTakenLmmWriter) Write(records []*store.Record) error {
	start := time.Now()
	result := t.Writer.Write(records)
	t.TimeTaken += time.Now().Sub(start)
	return result
}

// A fake lmmWriter implementation. This fake simply stalls 50ms with each
// write.
type stallLmmWriter struct {
}

func (s stallLmmWriter) Write(records []*store.Record) error {
	time.Sleep(50 * time.Millisecond)
	return nil
}

func newWriter() (lmmWriterType, error) {
	return stallLmmWriter{}, nil
}

// The *real* lmmWriter implementation is commented out here as it only works
// on certain machines.
/*
func newWriter() (lmmWriterType, error) {
	addresses := []string{
		"192.168.9.36:9092",
		"192.168.9.9:9092",
		"192.168.9.20:9092",
		"192.168.9.22:9092",
		"192.168.9.24:9092",
	}
	return lmm.NewWriter(
		aTopic,
		"8afeb90e049741a8a044c905bb6f3275",
		"afe5e75b-5e29-4f0d-994c-be36dbf54f94",
		addresses)
}
*/

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	metricStore = store.New(fBufferSizePerMachine, fSpacingPerMachine)
	collector.SetConcurrentPolls(fPollCount)
	collector.SetConcurrentConnects(fConnectionCount)
	f, err := os.Open(fHostFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var hostAndPort string
	_, err = fmt.Fscanln(f, &hostAndPort)
	for ; err != io.EOF; _, err = fmt.Fscanln(f, &hostAndPort) {
		if err != nil {
			continue
		}
		splits := strings.SplitN(hostAndPort, ":", 2)
		port, _ := strconv.Atoi(splits[1])
		machineNames = append(machineNames, splits[0])
		machinePorts = append(machinePorts, port)
	}
	fmt.Println(collector.ConcurrentPolls())
	fmt.Println(collector.ConcurrentConnects())
	writer, err := newWriter()
	if err != nil {
		log.Fatal(err)
	}
	lmmHandler := newLmmHandler(writer)
	lmmHandler.RegisterMetrics()
	logger := newLogger()
	logger.RegisterMetrics()
	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewDistribution()
	tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration")
	machines := make(map[string]*collector.Machine)
	for i := 0; i < totalNumberOfMachines; i++ {
		name, port := machineNameAndPort(i)
		if i%1000 == 37 {
			port = 7776
		}
		machines[name] = collector.NewMachine(name, port, logger)
		metricStore.RegisterMachine(machines[name])
		if i%200 == 0 {
			fmt.Print(".")
		}
	}
	fmt.Println("Initialized")
	programStartTime := time.Now()
	elapsedTimeFunc := func() time.Duration {
		return time.Now().Sub(programStartTime)
	}
	tricorder.RegisterMetric(
		"collector/elapsedTime",
		elapsedTimeFunc,
		units.Second,
		"elapsed time")
	// Metric collection goroutine. Collect metrics every minute.
	go func() {
		for {
			sweepTime := time.Now()
			for _, machine := range machines {
				machine.Poll(sweepTime)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			if sweepDuration < fCollectionFrequency {
				time.Sleep(fCollectionFrequency - sweepDuration)
			}
		}
	}()

	// LMM writing goroutine. Write to LMM every minute.
	// Notice that this single goroutine handles all the LMM writing
	// as multiple goroutines must not access the lmmHandler instance.
	// accessing lmmHandler metrics is the one exception to this rule.
	go func() {
		for {
			writeTime := time.Now()
			lmmHandler.StartVisit()
			metricStore.VisitAllMachines(lmmHandler)
			lmmHandler.EndVisit()
			writeDuration := time.Now().Sub(writeTime)
			if writeDuration < fLmmUpdateFrequency {
				time.Sleep(fLmmUpdateFrequency - writeDuration)
			}
		}
	}()
	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{byMachineHandler(machines)}))
	http.Handle(
		"/api/errors/",
		gzipHandler{errorHandler{logger}},
	)
	if err := http.ListenAndServe(":8187", nil); err != nil {
		log.Fatal(err)
	}
}

func init() {
	flag.IntVar(
		&fBufferSizePerMachine,
		"buffer_size_per_machine",
		60000,
		"Buffer size per machine in records")
	flag.IntVar(
		&fSpacingPerMachine,
		"spacing_per_machine",
		1000,
		"Space for new metrics for each machine in records")
	flag.StringVar(
		&fHostFile,
		"host_file",
		"hosts.txt",
		"File containing all the nodes")
	flag.IntVar(
		&fPollCount,
		"poll_count",
		collector.ConcurrentPolls(),
		"Maximum number of concurrent polls")
	flag.IntVar(
		&fConnectionCount,
		"connection_count",
		collector.ConcurrentConnects(),
		"Maximum number of concurrent connections")
	flag.DurationVar(
		&fCollectionFrequency,
		"collection_frequency",
		30*time.Second,
		"Amount of time between metric collections")
	flag.DurationVar(
		&fLmmUpdateFrequency,
		"lmm_update_frequency",
		30*time.Second,
		"Amount of time between writing newest metrics to lmm")
	flag.IntVar(
		&fLmmBatchSize,
		"lmm_batch_size",
		1000,
		"Batch to write at least this many records to Lmm")
}
