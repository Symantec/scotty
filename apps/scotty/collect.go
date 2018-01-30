package main

import (
	"flag"
	"github.com/Symantec/Dominator/lib/log"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/cis"
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/cloudhealthlmm"
	"github.com/Symantec/scotty/cloudwatch"
	"github.com/Symantec/scotty/endpointdata"
	"github.com/Symantec/scotty/lib/dynconfig"
	"github.com/Symantec/scotty/lib/keyedqueue"
	"github.com/Symantec/scotty/lib/trimetrics"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	fPollCount = flag.Uint(
		"concurrentPolls",
		0,
		"Maximum number of concurrent polls. 0 means no limit.")
	fConnectionCount = flag.Uint(
		"connectionCount",
		collector.ConcurrentConnects(),
		"Maximum number of concurrent connections")
	fCisEndpoint = flag.String(
		"cisEndpoint",
		"",
		"The optional CIS endpoint")
	fCisRegex = flag.String(
		"cisRegex",
		"",
		"If provided, host must match regex for scotty to send its data to CIS")
	fDataCenter = flag.String(
		"dataCenter",
		"",
		"Required for CIS writing: The data center name")
	fCisBufferSize = flag.Int(
		"cisBufferSize",
		40,
		"CIS Buffer Size")
)

// toInstanceMap converts a slice of instanceIds to a map of instanceIds.
// An empty map means no instanceIds; the nil map means all instance Ids.
func toInstanceIdMap(instanceIds []string) map[string]bool {
	if len(instanceIds) == 1 && strings.ToUpper(instanceIds[0]) == "ALL" {
		return nil
	}
	result := make(map[string]bool, len(instanceIds))
	for _, id := range instanceIds {
		result[id] = true
	}
	return result
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

// nameSetType represents a set of strings. Instances of this type
// are mutable.
type nameSetType map[string]bool

type totalCountUpdaterType interface {
	Update(s *store.Store, endpointId interface{})
}

// logger implements the scotty.Logger interface
// keeping track of collection statistics
type loggerType struct {
	Store                 *store.Store
	AppStats              *machine.EndpointStore
	App                   *machine.Endpoint
	ConnectionErrors      *connectionErrorsType
	CollectionTimesDist   *tricorder.CumulativeDistribution
	ByProtocolDist        map[string]*tricorder.CumulativeDistribution
	ChangedMetricsDist    *tricorder.CumulativeDistribution
	MetricNameAdder       suggest.Adder
	TotalCounts           totalCountUpdaterType
	CisQueue              *keyedqueue.Queue
	CloudHealthLmmChannel *chpipeline.AgedSnapshotChannel
	CloudHealthChannel    *chpipeline.AgedSnapshotListChannel
	CloudWatchChannel     *chpipeline.AgedSnapshotChannel
	EndpointData          *endpointdata.EndpointData
	EndpointObservations  *machine.EndpointObservations
}

func (l *loggerType) LogStateChange(
	e *collector.Endpoint, oldS, newS *collector.State) {
	if newS.Status() == collector.Synced {
		timeTaken := newS.TimeSpentConnecting()
		timeTaken += newS.TimeSpentPolling()
		timeTaken += newS.TimeSpentWaitingToConnect()
		timeTaken += newS.TimeSpentWaitingToPoll()
		l.CollectionTimesDist.Add(timeTaken)
		dist := l.ByProtocolDist[e.ConnectorName()]
		if dist != nil {
			dist.Add(timeTaken)
		}
	}
	l.AppStats.UpdateState(e, newS)
}

func (l *loggerType) LogError(e *collector.Endpoint, err error, state *collector.State) {
	if err == nil {
		l.ConnectionErrors.Clear(e)
	} else {
		l.ConnectionErrors.Set(e, err, state.Timestamp())
	}
	l.AppStats.ReportError(e, err, state.Timestamp())
}

func (l *loggerType) LogResponse(
	e *collector.Endpoint,
	list metrics.List,
	timestamp time.Time) error {
	ts := duration.TimeToFloat(timestamp)
	added, err := l.Store.AddBatch(
		e,
		ts,
		list)
	if err == nil {
		l.reportNewNamesForSuggest(list)
		l.AppStats.LogChangedMetricCount(e, added)
		l.ChangedMetricsDist.Add(float64(added))
		l.TotalCounts.Update(l.Store, e)
		if e.AppName() == application.HealthAgentName {
			l.EndpointObservations.Save(e.HostName(), metrics.Endpoints(list))
			if l.CisQueue != nil && l.App.M.Aws != nil {
				stats := cis.GetStats(list, l.App.M.Aws.InstanceId)
				if stats != nil {
					l.CisQueue.Add(stats)
				}
			}
		}
		var stats chpipeline.InstanceStats
		var statsOk bool
		chRollup := l.EndpointData.CHRollup
		chStore := l.EndpointData.CHStore
		if (l.CloudHealthChannel != nil || l.CloudHealthLmmChannel != nil) && chRollup != nil && chStore != nil {
			if !statsOk {
				stats = chpipeline.GetStats(list)
				stats.CombineFsStats()
				statsOk = true
			}
			if !chRollup.TimeOk(stats.Ts) {
				snapshot := chRollup.TakeSnapshot()
				chStore.Add(snapshot)
				if err := chStore.Save(); err != nil {
					return err
				}
				if l.CloudHealthChannel != nil {
					l.CloudHealthChannel.Send(chpipeline.AgedSnapshotList{
						Ts:           time.Now(),
						SnapshotList: chStore.GetAll()})
				}
				if l.CloudHealthLmmChannel != nil {
					l.CloudHealthLmmChannel.Send(chpipeline.AgedSnapshot{
						Ts:       time.Now(),
						Snapshot: snapshot})
				}
				chRollup.Clear()
			}
			chRollup.Add(&stats)
		}
		cwRollup := l.EndpointData.CWRollup
		if l.CloudWatchChannel != nil && cwRollup != nil {
			if !statsOk {
				stats = chpipeline.GetStats(list)
				stats.CombineFsStats()
				statsOk = true
			}
			if l.App.M.Aws != nil && l.App.M.Aws.CloudWatchMemoryOnly {
				stats.ClearNonMemory()
			}
			if !cwRollup.TimeOk(stats.Ts) {
				l.CloudWatchChannel.Send(chpipeline.AgedSnapshot{
					Ts:       time.Now(),
					Snapshot: cwRollup.TakeSnapshot()})
				cwRollup.Clear()
			}
			cwRollup.Add(&stats)
		}
	}
	// This error just means that the endpoint was marked inactive
	// during polling.
	if err == store.ErrInactive {
		return nil
	}
	return err
}

func (l *loggerType) reportNewNamesForSuggest(
	list metrics.List) {
	length := list.Len()
	for i := 0; i < length; i++ {
		var value metrics.Value
		list.Index(i, &value)
		if types.FromGoValue(value.Value).CanToFromFloat() {
			if !l.EndpointData.NamesSentToSuggest[value.Path] {
				l.MetricNameAdder.Add(value.Path)
				l.EndpointData.NamesSentToSuggest[value.Path] = true
			}
		}
	}
}

type memoryCheckerType interface {
	Check()
}

type snapshotWriterType interface {
	Write(s *chpipeline.Snapshot) error
}

func newCloudHealthLmmWriter(reader io.Reader) (interface{}, error) {
	var config cloudhealthlmm.Config
	if err := yamlutil.Read(reader, &config); err != nil {
		return nil, err
	}
	var writer snapshotWriterType
	writer, err := cloudhealthlmm.NewWriter(config)
	return writer, err
}

func newCloudHealthWriter(reader io.Reader) (interface{}, error) {
	var config cloudhealth.Config
	if err := yamlutil.Read(reader, &config); err != nil {
		return nil, err
	}
	writer, err := cloudhealth.NewWriter(config)
	return writer, err
}

func newCloudWatchWriter(reader io.Reader) (interface{}, error) {
	var config cloudwatch.Config
	if err := yamlutil.Read(reader, &config); err != nil {
		return nil, err
	}
	var writer snapshotWriterType
	writer, err := cloudwatch.NewWriter(config)
	return writer, err
}

func startCollector(
	endpointStore *machine.EndpointStore,
	connectionErrors *connectionErrorsType,
	totalCounts totalCountUpdaterType,
	metricNameAdder suggest.Adder,
	memoryChecker memoryCheckerType,
	myHostName *stringType,
	logger log.Logger) {
	collector.SetConcurrentPolls(*fPollCount)
	collector.SetConcurrentConnects(*fConnectionCount)

	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	collectionBucketer := tricorder.NewArbitraryBucketer(0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 50.0, 100.0)
	collectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	tricorderCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	changedMetricsPerEndpointDist := tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()

	if err := tricorder.RegisterMetric(
		"collector/collectionTimes",
		collectionTimesDist,
		units.Second,
		"Collection Times"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_tricorder",
		tricorderCollectionTimesDist,
		units.Second,
		"Tricorder Collection Times"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/changedMetricsPerEndpoint",
		changedMetricsPerEndpointDist,
		units.None,
		"Changed metrics per sweep"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration"); err != nil {
		logger.Fatal(err)
	}
	programStartTime := time.Now()
	if err := tricorder.RegisterMetric(
		"collector/elapsedTime",
		func() time.Duration {
			return time.Now().Sub(programStartTime)
		},
		units.Second,
		"elapsed time"); err != nil {
		logger.Fatal(err)
	}

	byProtocolDist := map[string]*tricorder.CumulativeDistribution{
		"tricorder": tricorderCollectionTimesDist,
	}

	var cloudHealthChannel *chpipeline.AgedSnapshotListChannel
	var cloudHealthConfig *dynconfig.DynConfig

	cloudHealthConfigFile := path.Join(*fConfigDir, "cloudhealth.yaml")
	if _, err := os.Stat(cloudHealthConfigFile); err == nil {
		cloudHealthConfig, err = dynconfig.NewInitialized(
			cloudHealthConfigFile,
			newCloudHealthWriter,
			"cloudHealth",
			logger)
		if err != nil {
			logger.Fatal(err)
		}
		cloudHealthChannel = chpipeline.NewAgedSnapshotListChannel(10000)
	}

	var cloudHealthLmmChannel *chpipeline.AgedSnapshotChannel
	var cloudHealthLmmConfig *dynconfig.DynConfig

	cloudHealthLmmConfigFile := path.Join(*fConfigDir, "cloudhealthlmm.yaml")
	if _, err := os.Stat(cloudHealthLmmConfigFile); err == nil {
		cloudHealthLmmConfig, err = dynconfig.NewInitialized(
			cloudHealthLmmConfigFile,
			newCloudHealthLmmWriter,
			"cloudHealthLmm",
			logger)
		if err != nil {
			logger.Fatal(err)
		}
		cloudHealthLmmChannel = chpipeline.NewAgedSnapshotChannel(10000)
	}

	var cloudWatchChannel *chpipeline.AgedSnapshotChannel
	var cloudWatchConfig *dynconfig.DynConfig

	cloudWatchConfigFile := path.Join(*fConfigDir, "cloudwatch.yaml")
	if _, err := os.Stat(cloudWatchConfigFile); err == nil {
		var err error
		cloudWatchConfig, err = dynconfig.NewInitialized(
			cloudWatchConfigFile,
			newCloudWatchWriter,
			"cloudwatch",
			logger)
		if err != nil {
			logger.Fatal(err)
		}
		cloudWatchChannel = chpipeline.NewAgedSnapshotChannel(10000)
	}

	var bulkCisClient *cis.Buffered
	var cisRegex *regexp.Regexp
	var cisQueue *keyedqueue.Queue

	if *fCisEndpoint != "" && *fDataCenter != "" {
		var err error
		var cisClient *cis.Client
		// TODO: move to config file when ready
		cisClient, err = cis.NewClient(
			cis.Config{
				Endpoint:   *fCisEndpoint,
				DataCenter: *fDataCenter,
			})
		bulkCisClient = cis.NewBuffered(*fCisBufferSize, cisClient)
		if err != nil {
			logger.Fatal(err)
		}
		cisQueue = keyedqueue.New()
		if *fCisRegex != "" {
			var err error
			cisRegex, err = regexp.Compile(*fCisRegex)
			if err != nil {
				logger.Fatal(err)
			}
		}
	}

	// Metric collection goroutine. Collect metrics periodically.
	go func() {
		endpointToData := make(
			map[*collector.Endpoint]*endpointdata.EndpointData)
		endpointObservations := machine.NewEndpointObservations()
		for {
			endpoints, metricStore := endpointStore.AllActiveWithStore()
			sweepTime := time.Now()
			for _, endpoint := range endpoints {
				endpointData := endpointToData[endpoint.App.EP]
				if endpointData == nil {
					endpointData = endpointdata.NewEndpointData()
				}
				if cloudHealthChannel != nil || cloudHealthLmmChannel != nil {
					endpointData = endpointData.UpdateForCloudHealth(endpoint)
				}
				if cloudWatchChannel != nil {
					endpointData = endpointData.UpdateForCloudWatch(endpoint)
				}
				endpointToData[endpoint.App.EP] = endpointData

				maybeCisQueue := cisQueue
				// If there is a regex filter and our machine doesn't match
				// nil out the cisQueue so that we don't send to CIS.
				if cisRegex != nil && !cisRegex.MatchString(endpoint.App.EP.HostName()) {
					maybeCisQueue = nil
				}

				pollLogger := &loggerType{
					Store:                 metricStore,
					AppStats:              endpointStore,
					App:                   endpoint,
					ConnectionErrors:      connectionErrors,
					CollectionTimesDist:   collectionTimesDist,
					ByProtocolDist:        byProtocolDist,
					ChangedMetricsDist:    changedMetricsPerEndpointDist,
					MetricNameAdder:       metricNameAdder,
					TotalCounts:           totalCounts,
					CisQueue:              maybeCisQueue,
					CloudHealthChannel:    cloudHealthChannel,
					CloudHealthLmmChannel: cloudHealthLmmChannel,
					CloudWatchChannel:     cloudWatchChannel,
					EndpointData:          endpointData,
					EndpointObservations:  endpointObservations,
				}

				portNum := endpoint.App.Port
				isTLS := endpoint.App.IsTLS
				endpoint.App.EP.Poll(sweepTime, isTLS, portNum, pollLogger)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			memoryChecker.Check()
			if sweepDuration < *fCollectionFrequency {
				time.Sleep((*fCollectionFrequency) - sweepDuration)
			}
			if myHostNameStr := myHostName.String(); myHostNameStr != "" {
				endpointObservations.MaybeAddApp(myHostNameStr, *fName, *fPort)
			}
			endpointStore.UpdateEndpoints(
				duration.TimeToFloat(time.Now()),
				endpointObservations.GetAll())
		}
	}()

	if cisQueue != nil && bulkCisClient != nil {
		startCisLoop(cisQueue, bulkCisClient, programStartTime, logger)
	}

	if cloudHealthConfig != nil && cloudHealthChannel != nil {
		startCloudHealthLoop(cloudHealthConfig, cloudHealthChannel, logger)
	}

	if cloudHealthLmmConfig != nil && cloudHealthLmmChannel != nil {
		startSnapshotLoop(
			"cloudhealthlmm",
			cloudHealthLmmConfig,
			cloudHealthLmmChannel,
			logger)
	}

	if cloudWatchConfig != nil && cloudWatchChannel != nil {
		startSnapshotLoop(
			"cloudwatch", cloudWatchConfig, cloudWatchChannel, logger)
	}
}

func startSnapshotLoop(
	parentDir string,
	config *dynconfig.DynConfig,
	channel *chpipeline.AgedSnapshotChannel,
	logger log.Logger) {

	if err := tricorder.RegisterMetric(
		parentDir+"/channelLen",
		func() int {
			return len(channel.Channel)
		},
		units.None,
		"Length of channel"); err != nil {
		logger.Fatal(err)
	}
	channelCap := cap(channel.Channel)
	if err := tricorder.RegisterMetric(
		parentDir+"/channelCap",
		&channelCap,
		units.None,
		"Capacity of channel"); err != nil {
		logger.Fatal(err)
	}
	var latency trimetrics.Duration
	if err := tricorder.RegisterMetric(
		parentDir+"/latency",
		latency.Get,
		units.Second,
		"latency of last write"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		parentDir+"/overflows",
		channel.Overflows,
		units.None,
		"number of channel overflows"); err != nil {
		logger.Fatal(err)
	}

	writerMetrics, err := trimetrics.NewWriterMetrics(parentDir + "/writer")
	if err != nil {
		logger.Fatal(err)
	}

	go func() {
		for {
			agedSnapshot := <-channel.Channel
			snapshot := agedSnapshot.Snapshot
			latency.Set(agedSnapshot.Age())
			writer := config.Get().(snapshotWriterType)
			writeStartTime := time.Now()
			if err := writer.Write(snapshot); err != nil {
				logger.Printf("%s: %v", parentDir, err)
				writerMetrics.LogError(time.Since(writeStartTime), 1, err)
			} else {
				writerMetrics.LogSuccess(time.Since(writeStartTime), 1)
			}
		}
	}()

}

func startCloudHealthLoop(
	cloudHealthConfig *dynconfig.DynConfig,
	cloudHealthChannel *chpipeline.AgedSnapshotListChannel,
	logger log.Logger) {

	if err := tricorder.RegisterMetric(
		"cloudhealth/channelLen",
		func() int {
			return len(cloudHealthChannel.Channel)
		},
		units.None,
		"Length of channel"); err != nil {
		logger.Fatal(err)
	}
	channelCap := cap(cloudHealthChannel.Channel)
	if err := tricorder.RegisterMetric(
		"cloudhealth/channelCap",
		&channelCap,
		units.None,
		"Capacity of channel"); err != nil {
		logger.Fatal(err)
	}
	var latency trimetrics.Duration
	if err := tricorder.RegisterMetric(
		"cloudhealth/latency",
		latency.Get,
		units.Second,
		"latency of last write"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"cloudhealth/overflows",
		cloudHealthChannel.Overflows,
		units.None,
		"number of channel overflows"); err != nil {
		logger.Fatal(err)
	}

	writerMetrics, err := trimetrics.NewWriterMetrics("cloudhealth/writer")
	if err != nil {
		logger.Fatal(err)
	}

	go func() {
		for {
			agedSnapshots := <-cloudHealthChannel.Channel
			snapshots := agedSnapshots.SnapshotList
			latency.Set(agedSnapshots.Age())
			writer := cloudHealthConfig.Get().(*cloudhealth.Writer)
			// holds stuff to be sent to writer
			buffer := cloudhealth.NewBuffer()
			for _, snapshot := range snapshots {
				call := chpipeline.NewCloudHealthInstanceCall(snapshot)
				newCall, fsCalls := call.Split()
				if len(fsCalls) > 0 {
					// Current snapshot too big to send to cloudhealth

					// Flush the buffer
					flushCloudHealthBuffer(
						buffer, writer, writerMetrics, logger)

					// Write instance data and first part of file system
					// data
					cloudHealthWrite(
						writer,
						[]cloudhealth.InstanceData{newCall.Instance},
						newCall.Fss,
						writerMetrics,
						logger)

					// Write remaining file system data
					for _, fsCall := range fsCalls {
						cloudHealthWrite(
							writer,
							nil,
							fsCall,
							writerMetrics,
							logger)
					}
				} else {
					// Current snapshot small enough to send to cloud health
					if !buffer.Add(newCall.Instance, newCall.Fss) {
						// Buffer full. Flush it first.
						flushCloudHealthBuffer(
							buffer, writer, writerMetrics, logger)

						// Adding snapshot to empty buffer should succeed
						if !buffer.Add(newCall.Instance, newCall.Fss) {
							panic("Oops, cloudhealth write call too big to buffer")
						}
					}
				} // send snapshot
			} // send all snapshots
			flushCloudHealthBuffer(buffer, writer, writerMetrics, logger)
		}
	}()
}

func cloudHealthWrite(
	writer *cloudhealth.Writer,
	instances []cloudhealth.InstanceData,
	fss []cloudhealth.FsData,
	metrics *trimetrics.WriterMetrics,
	logger log.Logger) {
	writeStartTime := time.Now()
	if _, err := writer.Write(instances, fss); err != nil {
		logger.Printf("Error writing to cloud health: %s", err)
		metrics.LogError(
			time.Since(writeStartTime), uint64(len(instances)), err)
	} else {
		metrics.LogSuccess(time.Since(writeStartTime), uint64(len(instances)))
	}
}

func flushCloudHealthBuffer(
	buffer *cloudhealth.Buffer,
	writer *cloudhealth.Writer,
	metrics *trimetrics.WriterMetrics,
	logger log.Logger) {
	if buffer.IsEmpty() {
		return
	}
	instances, fss := buffer.Get()
	cloudHealthWrite(writer, instances, fss, metrics, logger)
	buffer.Clear()
}

func startCisLoop(
	cisQueue *keyedqueue.Queue,
	bulkCisClient *cis.Buffered,
	programStartTime time.Time,
	logger log.Logger) {
	lastSuccessfulWriteTime := time.Now()

	if err := tricorder.RegisterMetric(
		"cis/timeSinceLastWrite",
		func() time.Duration {
			return time.Since(lastSuccessfulWriteTime)
		},
		units.Second,
		"Time since last successful write"); err != nil {
		logger.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"cis/queueSize",
		cisQueue.Len,
		units.None,
		"Length of queue"); err != nil {
		logger.Fatal(err)
	}
	timeBetweenWritesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err := tricorder.RegisterMetric(
		"cis/timeBetweenWrites",
		timeBetweenWritesDist,
		units.Second,
		"elapsed time between CIS updates"); err != nil {
		logger.Fatal(err)
	}
	writeTimesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err := tricorder.RegisterMetric(
		"cis/writeTimes",
		writeTimesDist,
		units.Millisecond,
		"elapsed time between CIS updates"); err != nil {
		logger.Fatal(err)
	}
	var lastWriteError string
	if err := tricorder.RegisterMetric(
		"cis/lastWriteError",
		&lastWriteError,
		units.None,
		"Last CIS write error"); err != nil {
		logger.Fatal(err)
	}
	var successfulWrites uint64
	if err := tricorder.RegisterMetric(
		"cis/successfulWrites",
		&successfulWrites,
		units.None,
		"Successful write count"); err != nil {
		logger.Fatal(err)
	}
	var totalWrites uint64
	if err := tricorder.RegisterMetric(
		"cis/totalWrites",
		&totalWrites,
		units.None,
		"total write count"); err != nil {
		logger.Fatal(err)
	}

	// CIS loop
	go func() {
		lastTimeStampByKey := make(map[interface{}]time.Time)
		for {
			if cisQueue.Len() == 0 {
				writeStartTime := time.Now()
				numWritten, err := bulkCisClient.Flush()
				if err != nil {
					logger.Printf("Error writing to CIS: %v", err)
					lastWriteError = err.Error()
				} else {
					successfulWrites += uint64(numWritten)
					if numWritten > 0 {
						lastSuccessfulWriteTime = time.Now()
					}
				}
				totalWrites += uint64(numWritten)
				if numWritten > 0 {
					timeElapsed := time.Since(writeStartTime)
					timePerWrite := timeElapsed / time.Duration(numWritten)
					for i := 0; i < numWritten; i++ {
						writeTimesDist.Add(timePerWrite)
					}
				}
			}
			stat := cisQueue.Remove().(*cis.Stats)
			key := stat.Key()
			if lastTimeStamp, ok := lastTimeStampByKey[key]; ok {
				timeBetweenWritesDist.Add(stat.TimeStamp.Sub(lastTimeStamp))
			} else {
				// On first write, just use time elapsed since start of
				// scotty
				timeBetweenWritesDist.Add(time.Now().Sub(programStartTime))
			}
			lastTimeStampByKey[key] = stat.TimeStamp
			writeStartTime := time.Now()
			numWritten, err := bulkCisClient.Write(*stat)
			if err != nil {
				logger.Printf("Error writing to CIS: %v", err)
				lastWriteError = err.Error()
			} else {
				successfulWrites += uint64(numWritten)
				if numWritten > 0 {
					lastSuccessfulWriteTime = time.Now()
				}
			}
			totalWrites += uint64(numWritten)
			if numWritten > 0 {
				timeElapsed := time.Since(writeStartTime)
				timePerWrite := timeElapsed / time.Duration(numWritten)
				for i := 0; i < numWritten; i++ {
					writeTimesDist.Add(timePerWrite)
				}
			}
		}
	}()
}
