package store

import (
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"strings"
	"sync"
	"time"
)

// This file contains the code for keeping a collection of series per
// endpoint.

// metricInfoStoreType keeps a pool of pointers to unique MetricInfo instances
type metricInfoStoreType struct {
	ByInfo map[MetricInfo]*MetricInfo
	ByName map[string][]*MetricInfo
}

func (m *metricInfoStoreType) Init() {
	m.ByInfo = make(map[MetricInfo]*MetricInfo)
	m.ByName = make(map[string][]*MetricInfo)
}

// Register returns the correct MetricInfo instance from the pool for
// passed in metric. Register will always return a non nil value.
func (m *metricInfoStoreType) Register(metric *messages.Metric) (
	result *MetricInfo) {
	infoStruct := MetricInfo{
		path:        metric.Path,
		description: metric.Description,
		unit:        metric.Unit,
		kind:        metric.Kind,
		bits:        metric.Bits,
		groupId:     metric.GroupId}
	result, alreadyExists := m.ByInfo[infoStruct]
	if alreadyExists {
		return
	}
	result = &infoStruct
	m.ByInfo[infoStruct] = result
	m.ByName[infoStruct.path] = append(m.ByName[infoStruct.path], result)
	return
}

// timeSeriesCollectionType represents all the values and timestamps for
// a particular endpoint.
type timeSeriesCollectionType struct {
	applicationId interface{}
	metrics       *storeMetricsType
	// A function must hold this lock when changing the status of
	// any time series in this instance or when adding a new time series
	// to ensure that when it returns, the status of each time series
	// is consistent with the status of its corresponding pages.
	// If this lock is acquired, it must be acquired before the normal
	// lock of this instance.
	statusChangeLock sync.Mutex
	// Normal lock of this instance.
	lock            sync.Mutex
	timeSeries      map[*MetricInfo]*timeSeriesType
	timestampSeries map[int]*timestampSeriesType
	metricInfoStore metricInfoStoreType
	active          bool
	iterators       map[string]*namedIteratorDataType
}

func newTimeSeriesCollectionType(
	app interface{},
	metrics *storeMetricsType) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId:   app,
		metrics:         metrics,
		timeSeries:      make(map[*MetricInfo]*timeSeriesType),
		timestampSeries: make(map[int]*timestampSeriesType),
		active:          true,
		iterators:       make(map[string]*namedIteratorDataType),
	}
	result.metricInfoStore.Init()
	return result
}

func (c *timeSeriesCollectionType) NewNamedIterator(
	name string, maxFrames int) NamedIterator {
	var startTimes map[int]float64
	var completed map[*timeSeriesType]float64
	c.lock.Lock()
	defer c.lock.Unlock()
	snapshot := c.iterators[name]
	if snapshot != nil {
		startTimes = snapshot.startTimeStamps
		completed = snapshot.completed
	}
	timesByGroup := make(map[int][]float64, len(c.timestampSeries))
	for groupId, series := range c.timestampSeries {
		timesByGroup[groupId] = series.FindAfter(
			startTimes[groupId], maxFrames)
	}
	return &namedIteratorType{
		name:                 name,
		timeSeriesCollection: c,
		startTimeStamps:      startTimes,
		timestamps:           timesByGroup,
		completed:            copyCompleted(completed),
		timeSeries:           c.tsAll(),
	}
}

func (c *timeSeriesCollectionType) saveProgress(
	name string, progress *namedIteratorDataType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.iterators[name] = progress
}

func (c *timeSeriesCollectionType) tsAll() (
	result []*timeSeriesType) {
	for _, ts := range c.timeSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) tsAllTimeStamps() (
	result []*timestampSeriesType) {
	for _, ts := range c.timestampSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) TsAllAndTimeStampsMarkingInactive() (
	[]*timeSeriesType, []*timestampSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.active = false
	return c.tsAll(), c.tsAllTimeStamps()
}

func (c *timeSeriesCollectionType) TsByName(name string) (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	infoList := c.metricInfoStore.ByName[name]
	for _, info := range infoList {
		result = append(result, c.timeSeries[info])
	}
	return
}

func (c *timeSeriesCollectionType) TsByPrefix(prefix string) (
	result []*timeSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, info := range c.metricInfoStore.ByInfo {
		if strings.HasPrefix(info.Path(), prefix) {
			result = append(result, c.timeSeries[info])
		}
	}
	return
}

// LookupBatch looks up all the metrics in one go and returns the
// following:
// fetched: timeSeries already in this collection keyed by Metric.
//  values must be added to these manually.
// newOnes: timeSeries just added as a result of this lookup. Since these
// are new, the first value added automatically.
// notFetched: timeSeries in this collection but not fetched. These
// are the time series that should be marked inactive.
// ok is true if metrics can be added to this instance or false if this
// instance is inactive and closed for new metrics.
func (c *timeSeriesCollectionType) LookupBatch(
	timestamp float64, metrics messages.MetricList) (
	fetched map[*timeSeriesType]interface{},
	newOnes, notFetched []*timeSeriesType,
	fetchedTimeStamps map[*timestampSeriesType]float64,
	newTimeStampsByGroupId map[int]float64,
	notFetchedTimeStamps []*timestampSeriesType,
	ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.active {
		return
	}
	ok = true
	valueByMetric := make(map[*MetricInfo]interface{})
	timestampByGroupId := make(map[int]float64)
	fetched = make(map[*timeSeriesType]interface{})
	fetchedTimeStamps = make(map[*timestampSeriesType]float64)
	newTimeStampsByGroupId = make(map[int]float64)
	for i := range metrics {
		// TODO: Allow distribution metrics later.
		if metrics[i].Kind == types.Dist {
			continue
		}
		id := c.metricInfoStore.Register(metrics[i])
		valueByMetric[id] = metrics[i].Value
		if metrics[i].TimeStamp == nil {
			timestampByGroupId[id.GroupId()] = timestamp
		} else {
			timestampByGroupId[id.GroupId()] = messages.TimeToFloat(metrics[i].TimeStamp.(time.Time))
		}
	}
	// populate notFetched
	for id, series := range c.timeSeries {
		if _, ok := valueByMetric[id]; !ok {
			notFetched = append(notFetched, series)
		}
	}
	// populate fetched and newOnes
	for id, value := range valueByMetric {
		if c.timeSeries[id] == nil {
			thisTs := timestampByGroupId[id.GroupId()]
			c.timeSeries[id] = newTimeSeriesType(
				id, thisTs, value, c.metrics)
			newOnes = append(newOnes, c.timeSeries[id])
		} else {
			fetched[c.timeSeries[id]] = value
		}
	}
	// populate notFetchedTimeStamps
	for groupId, series := range c.timestampSeries {
		if _, ok := timestampByGroupId[groupId]; !ok {
			notFetchedTimeStamps = append(
				notFetchedTimeStamps, series)
		}
	}
	// populate fetchedTimeStamps and newTimeStampsByGroupId
	for groupId, ts := range timestampByGroupId {
		if c.timestampSeries[groupId] == nil {
			c.timestampSeries[groupId] = newTimeStampSeriesType(
				groupId, ts, c.metrics)
			newTimeStampsByGroupId[groupId] = ts
		} else {
			fetchedTimeStamps[c.timestampSeries[groupId]] = ts
		}
	}
	return
}

func (c *timeSeriesCollectionType) MarkActive() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.active = true
}

// Marks this instance inactive.
// timestamp is the timestamp of scotty.
// supplier is the page queue.
func (c *timeSeriesCollectionType) MarkInactive(
	timestamp float64, supplier *pageQueueType) {
	var reclaimHighList []pageListType
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	timeSeriesList, timestampSeriesList := c.TsAllAndTimeStampsMarkingInactive()
	var inactiveCount int
	for i := range timeSeriesList {
		// If it was active
		if inactivateTimeSeries(
			timeSeriesList[i], timestamp, supplier) {
			reclaimHighList = append(
				reclaimHighList,
				timeSeriesList[i].PageList())
			inactiveCount++
		}
	}
	for i := range timestampSeriesList {
		justInactivated, _ := inactivateTimeStampSeries(
			timestampSeriesList[i], supplier)
		// If it was active
		if justInactivated {
			reclaimHighList = append(
				reclaimHighList,
				timestampSeriesList[i].PageList())
		}
	}

	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	c.metrics.AddUniqueValues(inactiveCount)

	return
}

// Add batch of values.
// timestamp is the timestamp of scotty.
func (c *timeSeriesCollectionType) AddBatch(
	timestamp float64,
	metrics messages.MetricList,
	supplier *pageQueueType) (result int, ok bool) {
	var reclaimLowList, reclaimHighList []pageListType
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	fetched, newOnes, notFetched, tsFetched, timestamps, tsNotFetched, ok := c.LookupBatch(timestamp, metrics)
	if !ok {
		return
	}
	addedCount := len(newOnes)

	// Do a dry run of adding timestamps so that we can finish
	// populating the timestamps map. We don't actually add the
	// timestamps because they must be added AFTER the values for
	// iteration when writing to LMM to work correctly.
	for timestampSeries, tsValue := range tsFetched {
		neededToAdd := timestampSeries.AddDryRun(tsValue)
		if neededToAdd {
			timestamps[timestampSeries.GroupId()] = tsValue
		}
	}

	// Do a dry run of inactivating a timestamp series
	for i := range tsNotFetched {
		justInactivated, adjustedTsValue := tsNotFetched[i].InactivateDryRun()
		if justInactivated {
			timestamps[tsNotFetched[i].GroupId()] = adjustedTsValue
		}
	}

	// For each metric in fetched, manually add its value to its
	// time series
	for timeSeries, value := range fetched {
		thisTimeStamp, ok := timestamps[timeSeries.GroupId()]
		if ok {
			needToAdd, justActivated := addToTimeSeries(
				timeSeries,
				thisTimeStamp,
				value,
				supplier)

			if needToAdd {
				addedCount++
			}
			// If status went from inactive to active.
			if justActivated {
				reclaimLowList = append(
					reclaimLowList,
					timeSeries.PageList())
			}
		}
	}

	var inactiveCount int
	for i := range notFetched {
		thisTimeStamp, ok := timestamps[notFetched[i].GroupId()]
		if ok && inactivateTimeSeries(
			notFetched[i],
			thisTimeStamp,
			supplier) {
			reclaimHighList = append(
				reclaimHighList,
				notFetched[i].PageList())
			inactiveCount++
		}
	}

	// Add each timestamp to its timestamp series. This must be done
	// after adding the values.
	for timestampSeries, tsValue := range tsFetched {
		_, justActivated := addToTimeStampSeries(
			timestampSeries, tsValue, supplier)
		if justActivated {
			reclaimLowList = append(
				reclaimLowList,
				timestampSeries.PageList())
		}
	}

	// Inactivate any missed timestamp series.
	// Must be done after adding the values
	for i := range tsNotFetched {
		justInactivated, _ := inactivateTimeStampSeries(tsNotFetched[i], supplier)
		if justInactivated {
			reclaimHighList = append(
				reclaimHighList,
				notFetched[i].PageList())
		}
	}

	// reclaimHighList has all the page lists that should be reclaimed with
	// high priority
	supplier.ReclaimHigh(reclaimHighList)

	// recliamLowList has all the page lists that should be reclaimed with
	// low priority
	supplier.ReclaimLow(reclaimLowList)

	result = inactiveCount + addedCount
	c.metrics.AddUniqueValues(result)
	return
}

// Unique values by metric name in descending order happening before end and
// continuing until on or before start.
func (c *timeSeriesCollectionType) ByName(
	name string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByName(name) {
		timeSeries.Fetch(c.applicationId, start, end, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

// Unique values by metric prefix in descending order happening before end and
// continuing until on or before start.
func (c *timeSeriesCollectionType) ByPrefix(
	prefix string, start, end float64, result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByPrefix(prefix) {
		timeSeries.Fetch(c.applicationId, start, end, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

// Latest values for this instance.
func (c *timeSeriesCollectionType) Latest(result Appender) {
	doneAppender := &doneAppenderType{Wrapped: result}
	for _, timeSeries := range c.TsByPrefix("") {
		timeSeries.Fetch(
			c.applicationId, kPlusInf, kPlusInf, doneAppender)
		if doneAppender.Done {
			return
		}
	}
}
