package main

import (
	"flag"
	"github.com/Symantec/Dominator/lib/flagutil"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/cis"
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/lib/keyedqueue"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/messages"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"log"
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
	fCombineFileSystemIds = flagutil.StringList{"ALL"}
)

func init() {
	flag.Var(
		&fCombineFileSystemIds,
		"combineFileSystemIds",
		"comma separated aws instance Ids on which file system cloudhealth stats must be combined. 'all' means all instances")
}

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
	Store               *store.Store
	AppList             *datastructs.ApplicationList
	AppStats            *datastructs.ApplicationStatuses
	ConnectionErrors    *connectionErrorsType
	CollectionTimesDist *tricorder.CumulativeDistribution
	ByProtocolDist      map[string]*tricorder.CumulativeDistribution
	ChangedMetricsDist  *tricorder.CumulativeDistribution
	NamesSentToSuggest  nameSetType
	CloudHealthStats    *chpipeline.RollUpStats
	CombineFileSystems  bool
	MetricNameAdder     suggest.Adder
	TotalCounts         totalCountUpdaterType
	CisQueue            *keyedqueue.Queue
	CloudHealthChannel  chan chpipeline.CloudHealthInstanceCall
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
	l.AppStats.Update(e, newS)
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
		if e.Port() == 6910 {
			if l.CisQueue != nil {
				var instanceId string
				if app := l.AppStats.ByEndpointId(e); app != nil {
					instanceId = app.InstanceId()
				}
				stats := cis.GetStats(list, instanceId)
				if stats != nil {
					l.CisQueue.Add(stats)
				}
			}
			if l.CloudHealthChannel != nil && l.CloudHealthStats != nil {
				stats := chpipeline.GetStats(list)
				if l.CombineFileSystems {
					stats = stats.WithCombinedFsStats()
				}
				if !l.CloudHealthStats.TimeOk(stats.Ts) {
					l.CloudHealthChannel <- l.CloudHealthStats.CloudHealth()
					l.CloudHealthStats.Clear()
				}
				l.CloudHealthStats.Add(stats)
			}
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
			if !l.NamesSentToSuggest[value.Path] {
				l.MetricNameAdder.Add(value.Path)
				l.NamesSentToSuggest[value.Path] = true
			}
		}
	}
}

type memoryCheckerType interface {
	Check()
}

func createCloudHealthWriter(path string) *cloudhealth.Writer {
	var config cloudhealth.Config
	if err := yamlutil.ReadFromFile(path, &config); err != nil {
		log.Fatal(err)
	}
	return cloudhealth.NewWriter(config)
}

func startCollector(
	appStats *datastructs.ApplicationStatuses,
	connectionErrors *connectionErrorsType,
	totalCounts totalCountUpdaterType,
	metricNameAdder suggest.Adder,
	memoryChecker memoryCheckerType) {
	collector.SetConcurrentPolls(*fPollCount)
	collector.SetConcurrentConnects(*fConnectionCount)

	sweepDurationDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	collectionBucketer := tricorder.NewGeometricBucketer(1e-4, 100.0)
	collectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	tricorderCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	snmpCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	jsonCollectionTimesDist := collectionBucketer.NewCumulativeDistribution()
	changedMetricsPerEndpointDist := tricorder.NewGeometricBucketer(1.0, 10000.0).NewCumulativeDistribution()

	if err := tricorder.RegisterMetric(
		"collector/collectionTimes",
		collectionTimesDist,
		units.Second,
		"Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_tricorder",
		tricorderCollectionTimesDist,
		units.Second,
		"Tricorder Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_snmp",
		snmpCollectionTimesDist,
		units.Second,
		"SNMP Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/collectionTimes_json",
		jsonCollectionTimesDist,
		units.Second,
		"JSON Collection Times"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/changedMetricsPerEndpoint",
		changedMetricsPerEndpointDist,
		units.None,
		"Changed metrics per sweep"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"collector/sweepDuration",
		sweepDurationDist,
		units.Millisecond,
		"Sweep duration"); err != nil {
		log.Fatal(err)
	}
	programStartTime := time.Now()
	if err := tricorder.RegisterMetric(
		"collector/elapsedTime",
		func() time.Duration {
			return time.Now().Sub(programStartTime)
		},
		units.Second,
		"elapsed time"); err != nil {
		log.Fatal(err)
	}

	byProtocolDist := map[string]*tricorder.CumulativeDistribution{
		"tricorder": tricorderCollectionTimesDist,
		"snmp":      snmpCollectionTimesDist,
		"json":      jsonCollectionTimesDist,
	}

	var cloudHealthWriter *cloudhealth.Writer
	var cloudHealthChannel chan chpipeline.CloudHealthInstanceCall

	cloudHealthConfig := path.Join(*fConfigDir, "cloudhealth.yaml")
	if _, err := os.Stat(cloudHealthConfig); err == nil {
		// TODO: Revisit this.
		cloudHealthChannel = make(chan chpipeline.CloudHealthInstanceCall, 10000)
		cloudHealthWriter = createCloudHealthWriter(cloudHealthConfig)
	}

	var cisClient *cis.Client
	var cisRegex *regexp.Regexp
	var cisQueue *keyedqueue.Queue

	if *fCisEndpoint != "" && *fDataCenter != "" {
		var err error
		// TODO: move to config file when ready
		if strings.HasPrefix(*fCisEndpoint, "debug:") {
			cisClient, err = cis.NewClient(
				cis.Config{
					Endpoint:   (*fCisEndpoint)[6:],
					DataCenter: *fDataCenter,
				})
		} else {
			cisClient, err = cis.NewClient(cis.Config{
				Endpoint:   *fCisEndpoint,
				Name:       "cis",
				DataCenter: *fDataCenter,
			})
		}
		if err != nil {
			log.Fatal(err)
		}
		cisQueue = keyedqueue.New()
		if *fCisRegex != "" {
			var err error
			cisRegex, err = regexp.Compile(*fCisRegex)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	combineFsMap := toInstanceIdMap(fCombineFileSystemIds)

	// Metric collection goroutine. Collect metrics periodically.
	go func() {
		// We assign each endpoint its very own nameSetType instance
		// to store metric names already sent to suggest.
		// Only that endpoint's fetch goroutine reads and modifies
		// the contents of its nameSetType instance. Although
		// this goroutine creates nameSetType instances and manages
		// the references to them, it never reads or modifies the
		// contents of any nameSetType instance after creating it.
		endpointToNamesSentToSuggest := make(
			map[*collector.Endpoint]nameSetType)
		endpointToCloudHealthStats := make(
			map[*collector.Endpoint]*chpipeline.RollUpStats)
		for {
			endpoints, metricStore := appStats.ActiveEndpointIds()
			sweepTime := time.Now()
			for _, endpoint := range endpoints {
				namesSentToSuggest := endpointToNamesSentToSuggest[endpoint]
				if namesSentToSuggest == nil {
					namesSentToSuggest = make(nameSetType)
					endpointToNamesSentToSuggest[endpoint] = namesSentToSuggest
				}
				cloudHealthStats := endpointToCloudHealthStats[endpoint]
				if cloudHealthStats == nil {
					app := appStats.ByEndpointId(endpoint)
					cloudHealthStats = chpipeline.NewRollUpStats(
						app.AccountNumber(), app.InstanceId(), time.Hour)
					endpointToCloudHealthStats[endpoint] = cloudHealthStats
				}
				combineFileSystems := true
				if combineFsMap != nil {
					combineFileSystems = combineFsMap[cloudHealthStats.InstanceId()]
				}

				maybeCisQueue := cisQueue
				// If there is a regex filter and our machine doesn't match
				// nil out the cisQueue so that we don't send to CIS.
				if cisRegex != nil && !cisRegex.MatchString(endpoint.HostName()) {
					maybeCisQueue = nil
				}

				logger := &loggerType{
					Store:               metricStore,
					AppStats:            appStats,
					ConnectionErrors:    connectionErrors,
					CollectionTimesDist: collectionTimesDist,
					ByProtocolDist:      byProtocolDist,
					ChangedMetricsDist:  changedMetricsPerEndpointDist,
					NamesSentToSuggest:  namesSentToSuggest,
					CloudHealthStats:    cloudHealthStats,
					CombineFileSystems:  combineFileSystems,
					MetricNameAdder:     metricNameAdder,
					TotalCounts:         totalCounts,
					CisQueue:            maybeCisQueue,
					CloudHealthChannel:  cloudHealthChannel,
				}

				endpoint.Poll(sweepTime, logger)
			}
			sweepDuration := time.Now().Sub(sweepTime)
			sweepDurationDist.Add(sweepDuration)
			memoryChecker.Check()
			if sweepDuration < *fCollectionFrequency {
				time.Sleep((*fCollectionFrequency) - sweepDuration)
			}
		}
	}()

	if cisQueue != nil && cisClient != nil {
		startCisLoop(cisQueue, cisClient, programStartTime)
	}

	if cloudHealthWriter != nil && cloudHealthChannel != nil {
		startCloudFireLoop(cloudHealthWriter, cloudHealthChannel)
	}
}

func startCloudFireLoop(
	cloudHealthWriter *cloudhealth.Writer,
	cloudHealthChannel chan chpipeline.CloudHealthInstanceCall) {
	writeTimesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	var successfulWrites uint64
	if err := tricorder.RegisterMetric(
		"cloudhealth/successfulWrites",
		&successfulWrites,
		units.None,
		"Successful write count"); err != nil {
		log.Fatal(err)
	}
	var overflowWrites uint64
	if err := tricorder.RegisterMetric(
		"cloudhealth/overflowWrites",
		&overflowWrites,
		units.None,
		"overflow write count"); err != nil {
		log.Fatal(err)
	}
	var errorWrites uint64
	if err := tricorder.RegisterMetric(
		"cloudhealth/errorWrites",
		&errorWrites,
		units.None,
		"error write count"); err != nil {
		log.Fatal(err)
	}
	if err := tricorder.RegisterMetric(
		"cloudhealth/writeTimes",
		writeTimesDist,
		units.Millisecond,
		"cloud health write times"); err != nil {
		log.Fatal(err)
	}
	var lastWriteError string
	if err := tricorder.RegisterMetric(
		"cloudhealth/lastWriteError",
		&lastWriteError,
		units.None,
		"Last CloudHealth write error"); err != nil {
		log.Fatal(err)
	}
	var totalWrites uint64
	if err := tricorder.RegisterMetric(
		"cloudhealth/totalWrites",
		&totalWrites,
		units.None,
		"total write count"); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			call := <-cloudHealthChannel
			newCall, fsCalls := call.Split()
			writeStartTime := time.Now()
			if _, err := cloudHealthWriter.Write(
				[]cloudhealth.InstanceData{newCall.Instance},
				newCall.Fss); err != nil {
				lastWriteError = err.Error()
				errorWrites++
			} else {
				successfulWrites++
			}
			totalWrites++
			writeTimesDist.Add(time.Since(writeStartTime))
			for _, fsCall := range fsCalls {
				writeStartTime := time.Now()
				if _, err := cloudHealthWriter.Write(nil, fsCall); err != nil {
					lastWriteError = err.Error()
					errorWrites++
				} else {
					overflowWrites++
				}
				totalWrites++
				writeTimesDist.Add(time.Since(writeStartTime))
			}
		}
	}()

}

func startCisLoop(
	cisQueue *keyedqueue.Queue,
	cisClient *cis.Client,
	programStartTime time.Time) {
	if err := tricorder.RegisterMetric(
		"cis/queueSize",
		cisQueue.Len,
		units.None,
		"Length of queue"); err != nil {
		log.Fatal(err)
	}
	timeBetweenWritesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err := tricorder.RegisterMetric(
		"cis/timeBetweenWrites",
		timeBetweenWritesDist,
		units.Second,
		"elapsed time between CIS updates"); err != nil {
		log.Fatal(err)
	}
	writeTimesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err := tricorder.RegisterMetric(
		"cis/writeTimes",
		writeTimesDist,
		units.Millisecond,
		"elapsed time between CIS updates"); err != nil {
		log.Fatal(err)
	}
	var lastWriteError string
	if err := tricorder.RegisterMetric(
		"cis/lastWriteError",
		&lastWriteError,
		units.None,
		"Last CIS write error"); err != nil {
		log.Fatal(err)
	}
	var successfulWrites uint64
	if err := tricorder.RegisterMetric(
		"cis/successfulWrites",
		&successfulWrites,
		units.None,
		"Successful write count"); err != nil {
		log.Fatal(err)
	}
	var totalWrites uint64
	if err := tricorder.RegisterMetric(
		"cis/totalWrites",
		&totalWrites,
		units.None,
		"total write count"); err != nil {
		log.Fatal(err)
	}

	// CIS loop
	go func() {
		lastTimeStampByKey := make(map[interface{}]time.Time)
		for {
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
			if err := cisClient.Write(stat); err != nil {
				lastWriteError = err.Error()
			} else {
				successfulWrites++
			}
			totalWrites++
			writeTimesDist.Add(time.Since(writeStartTime))
		}
	}()
}
