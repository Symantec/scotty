package store

import (
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"strings"
	"sync"
)

// This file contains the code for keeping a collection of series per
// endpoint.

// metricInfoStoreType keeps a pool of pointers to unique MetricInfo instances
type metricInfoStoreType struct {
	ByInfo      map[MetricInfo]*MetricInfo
	ByName      map[string][]*MetricInfo
	rangesCache rangesCacheType
}

func (m *metricInfoStoreType) Init() {
	m.ByInfo = make(map[MetricInfo]*MetricInfo)
	m.ByName = make(map[string][]*MetricInfo)
	m.rangesCache.Init()
}

// Register returns the correct MetricInfo instance from the pool for
// passed in metric and type. Register will always return a non nil value.
func (m *metricInfoStoreType) Register(
	metric *metrics.Value, kind, subType types.Type) (
	result *MetricInfo) {
	if kind == types.Unknown {
		panic("Got Unknown type")
	}
	if kind.UsesSubType() && subType == types.Unknown {
		panic("Got unknown sub-type when it is required")
	}
	var ranges *Ranges
	var isNotCumulative bool
	if kind == types.Dist {
		// TODO: Maybe later this shouldn't be tricorder specific,
		// but for now tricorder is the only thing we know that
		// has distributions.
		distribution := metric.Value.(*messages.Distribution)
		isNotCumulative = distribution.IsNotCumulative
		upperLimitSlice := distExtractUpperLimits(distribution)
		ranges = m.rangesCache.Get(metric.Path, upperLimitSlice)
	}
	infoStruct := MetricInfo{
		path:            metric.Path,
		description:     metric.Description,
		unit:            metric.Unit,
		kind:            kind,
		subType:         subType,
		ranges:          ranges,
		isNotCumulative: isNotCumulative,
		groupId:         metric.GroupId}
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
	lock                        sync.Mutex
	timeSeries                  map[*MetricInfo]*timeSeriesType
	timestampSeries             map[int]*timestampSeriesType
	metricInfoStore             metricInfoStoreType
	active                      bool
	iterators                   map[string]*namedIteratorDataType
	distributionRollOversByPath map[string]*distributionRollOverType
}

func newTimeSeriesCollectionType(
	app interface{},
	metrics *storeMetricsType) *timeSeriesCollectionType {
	result := &timeSeriesCollectionType{
		applicationId:               app,
		metrics:                     metrics,
		timeSeries:                  make(map[*MetricInfo]*timeSeriesType),
		timestampSeries:             make(map[int]*timestampSeriesType),
		active:                      true,
		iterators:                   make(map[string]*namedIteratorDataType),
		distributionRollOversByPath: make(map[string]*distributionRollOverType),
	}
	result.metricInfoStore.Init()
	return result
}

func (c *timeSeriesCollectionType) NamedIteratorInfo(name string) (
	startTimes map[int]float64,
	completed map[*MetricInfo]float64,
	timestampSeries []*timestampSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	timestampSeries = c.tsAllTimeStamps()
	snapshot := c.iterators[name]
	if snapshot != nil {
		startTimes = snapshot.startTimeStamps
		completed = snapshot.completed
	}
	return
}

func (c *timeSeriesCollectionType) NewNamedIterator(
	name string,
	maxFrames int,
	strategy MetricGroupingStrategy) (NamedIterator, IteratorData) {
	startTimes,
		completed,
		timestampSeries := c.NamedIteratorInfo(name)
	allTimeSeries := c.TsAll()
	timesByGroup := make(map[int][]float64, len(timestampSeries))
	var skipped bool
	for _, series := range timestampSeries {
		groupId := series.GroupId()
		timesByGroup[groupId] = series.FindAfter(
			startTimes[groupId], maxFrames)
		if startTimes[groupId] > 0.0 && len(timesByGroup[groupId]) > 0 && timesByGroup[groupId][0] == series.Earliest() {
			skipped = true
		}
	}
	strategy.orderedPartition(allTimeSeries)
	result := &namedIteratorType{
		name:                 name,
		timeSeriesCollection: c,
		startTimeStamps:      startTimes,
		timestamps:           timesByGroup,
		completed:            copyCompleted(completed),
		timeSeries:           allTimeSeries,
		strategy:             strategy,
	}
	timeLeft, percentCaughtUp := timeLeft(timestampSeries, timesByGroup)
	return result, IteratorData{timeLeft, percentCaughtUp, skipped}
}

func timeLeft(
	timestampSeries []*timestampSeriesType,
	timesByGroup map[int][]float64) (float64, FloatVar) {
	result := 0.0
	percentCaughtUpSum := 0.0
	percentCaughtUpCount := uint64(0)
	for _, series := range timestampSeries {
		times := timesByGroup[series.GroupId()]
		timesLen := len(times)
		if timesLen > 0 {
			current := series.Latest() - times[timesLen-1]
			if current > result {
				result = current
			}
			earliest := series.Earliest()
			latest := series.Latest()
			diff := latest - earliest
			if diff > 0.0 {
				percentCaughtUpSum += (times[0] - earliest) / diff * 100.0
				percentCaughtUpCount++
			}
		}
	}
	return result, FloatVar{Sum: percentCaughtUpSum, Count: percentCaughtUpCount}
}

func (c *timeSeriesCollectionType) NewNamedIteratorRollUp(
	name string,
	duration float64,
	maxFrames int,
	strategy MetricGroupingStrategy) (
	NamedIterator, IteratorData) {
	startTimes,
		completed,
		timestampSeries := c.NamedIteratorInfo(name)
	allTimeSeries := c.TsAll()
	timesByGroup := make(map[int][]float64, len(timestampSeries))
	var skipped bool
	for _, series := range timestampSeries {
		groupId := series.GroupId()
		nextTimes := series.FindAfter(startTimes[groupId], 0)
		if startTimes[groupId] > 0.0 && len(nextTimes) > 0 && nextTimes[0] == series.Earliest() {
			skipped = true
		}
		chopForRollUp(duration, maxFrames, &nextTimes)
		timesByGroup[groupId] = nextTimes
	}
	strategy.orderedPartition(allTimeSeries)
	result := &rollUpNamedIteratorType{
		namedIteratorType: &namedIteratorType{
			name:                 name,
			timeSeriesCollection: c,
			startTimeStamps:      startTimes,
			timestamps:           timesByGroup,
			completed:            copyCompleted(completed),
			timeSeries:           allTimeSeries,
			strategy:             strategy,
		},
		strategy: strategy,
		interval: duration,
	}
	timeLeft, percentCaughtUp := timeLeft(timestampSeries, timesByGroup)
	return result, IteratorData{timeLeft, percentCaughtUp, skipped}
}

func (c *timeSeriesCollectionType) StartAtBeginning(names []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, name := range names {
		delete(c.iterators, name)
	}
}

func (c *timeSeriesCollectionType) SetIteratorTo(destName, srcName string) {
	if destName == srcName {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	srcProgress, ok := c.iterators[srcName]
	if !ok {
		delete(c.iterators, destName)
	} else {
		c.iterators[destName] = srcProgress
	}
}

func (c *timeSeriesCollectionType) SaveProgress(
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

func (c *timeSeriesCollectionType) TsAll() []*timeSeriesType {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.tsAll()
}

func (c *timeSeriesCollectionType) tsAllTimeStamps() (
	result []*timestampSeriesType) {
	for _, ts := range c.timestampSeries {
		result = append(result, ts)
	}
	return
}

func (c *timeSeriesCollectionType) TsAllTimeStamps() []*timestampSeriesType {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.tsAllTimeStamps()
}

func (c *timeSeriesCollectionType) TsAllAndTimeStampsMarkingInactive() (
	valueSeries []*timeSeriesType,
	timestampSeries []*timestampSeriesType,
	markedInactive bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.active {
		return
	}
	c.active = false
	return c.tsAll(), c.tsAllTimeStamps(), true
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

func (c *timeSeriesCollectionType) TsAndTimeStampsByName(name string) (
	result []*timeSeriesType, resultTs map[int]*timestampSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	infoList := c.metricInfoStore.ByName[name]
	resultTs = make(map[int]*timestampSeriesType)
	for _, info := range infoList {
		ts := c.timeSeries[info]
		groupId := ts.id.GroupId()
		result = append(result, ts)
		resultTs[groupId] = c.timestampSeries[groupId]
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

func (c *timeSeriesCollectionType) TsAndTimeStampsByPrefix(prefix string) (
	result []*timeSeriesType, resultTs map[int]*timestampSeriesType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	resultTs = make(map[int]*timestampSeriesType)
	for _, info := range c.metricInfoStore.ByInfo {
		if strings.HasPrefix(info.Path(), prefix) {
			ts := c.timeSeries[info]
			groupId := ts.id.GroupId()
			result = append(result, ts)
			resultTs[groupId] = c.timestampSeries[groupId]
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
	timestamp float64, mlist metrics.List) (
	fetched map[*timeSeriesType]interface{},
	newOnes, notFetched []*timeSeriesType,
	fetchedTimeStamps map[*timestampSeriesType]float64,
	newTs []*timestampSeriesType,
	notFetchedTimeStamps []*timestampSeriesType,
	err error) {
	if err = metrics.VerifyList(mlist); err != nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.active {
		err = ErrInactive
		return
	}
	valueByMetric := make(map[*MetricInfo]interface{})
	timestampByGroupId := make(map[int]float64)
	groupIds := make(map[int]bool)
	fetched = make(map[*timeSeriesType]interface{})
	fetchedTimeStamps = make(map[*timestampSeriesType]float64)
	mlen := mlist.Len()
	for i := 0; i < mlen; i++ {
		var avalue metrics.Value
		mlist.Index(i, &avalue)
		kind, subType := types.FromGoValueWithSubType(avalue.Value)
		id := c.metricInfoStore.Register(&avalue, kind, subType)
		if kind == types.Dist {
			distributionRollOvers := c.distributionRollOversByPath[avalue.Path]
			if distributionRollOvers == nil {
				distributionRollOvers = &distributionRollOverType{}
				c.distributionRollOversByPath[avalue.Path] = distributionRollOvers
			}
			distribution := avalue.Value.(*messages.Distribution)
			rollOverCount := distributionRollOvers.UpdateAndFetchRollOverCount(
				id.Ranges(), distribution.Generation)
			currentDistributionTotals := &DistributionTotals{
				Counts:        distExtractCounts(distribution),
				Sum:           distribution.Sum,
				RollOverCount: rollOverCount,
			}
			valueByMetric[id] = currentDistributionTotals
		} else {
			valueByMetric[id] = avalue.Value
		}
		groupIds[id.GroupId()] = true
		if !avalue.TimeStamp.IsZero() {
			timestampByGroupId[id.GroupId()] = duration.TimeToFloat(avalue.TimeStamp)
		}
	}
	// If a group ID is missing a timestamp, give it the
	// timestamp from scotty.
	for groupId := range groupIds {
		if _, ok := timestampByGroupId[groupId]; !ok {
			timestampByGroupId[groupId] = timestamp
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
	// populate fetchedTimeStamps and newTs
	for groupId, ts := range timestampByGroupId {
		if c.timestampSeries[groupId] == nil {
			c.timestampSeries[groupId] = newTimeStampSeriesType(
				groupId, ts, c.metrics)
			newTs = append(newTs, c.timestampSeries[groupId])
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

func (c *timeSeriesCollectionType) IsActive() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.active
}

func (c *timeSeriesCollectionType) updateTimeStampSeriesAndTimeSeries(
	newOnes []*timeSeriesType,
	fetched map[*timeSeriesType]interface{},
	notFetched []*timeSeriesType,
	newTs []*timestampSeriesType,
	tsFetched map[*timestampSeriesType]float64,
	tsNotFetched []*timestampSeriesType,
	supplier *pageQueueType) (result int) {
	var reclaimLowList, reclaimHighList []pageListType
	addedCount := len(newOnes)
	timestamps := make(
		map[int]float64,
		len(newTs)+len(tsFetched)+len(tsNotFetched))
	for i := range newTs {
		timestamps[newTs[i].GroupId()] = newTs[i].Latest()
	}

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

// Marks this instance inactive.
// timestamp is the timestamp of scotty and is currently unused.
// supplier is the page queue.
func (c *timeSeriesCollectionType) MarkInactive(
	unusedTimestamp float64, supplier *pageQueueType) {
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	timeSeriesList, timestampSeriesList, ok := c.TsAllAndTimeStampsMarkingInactive()
	if ok {
		c.updateTimeStampSeriesAndTimeSeries(
			nil, nil, timeSeriesList,
			nil, nil, timestampSeriesList,
			supplier)
	}
}

// Add batch of values.
// timestamp is the timestamp of scotty.
func (c *timeSeriesCollectionType) AddBatch(
	timestamp float64,
	mlist metrics.List,
	supplier *pageQueueType) (result uint, err error) {
	c.statusChangeLock.Lock()
	defer c.statusChangeLock.Unlock()
	fetched, newOnes, notFetched, tsFetched, newTs, tsNotFetched, err := c.LookupBatch(timestamp, mlist)
	if err != nil {
		return
	}
	result = uint(c.updateTimeStampSeriesAndTimeSeries(
		newOnes, fetched, notFetched,
		newTs, tsFetched, tsNotFetched,
		supplier))
	return
}

func (c *timeSeriesCollectionType) byWhateverGroupBy(
	timeSeries []*timeSeriesType,
	partition partitionType,
	start, end float64,
	result Appender) {
	// no-op if start exceeds end
	if start >= end {
		return
	}
	merger := newMerger()
	tslen := len(timeSeries)
	doneAppender := &doneAppenderType{Wrapped: result}
	for startIdx, endIdx := 0, 0; startIdx < tslen; startIdx = endIdx {
		endIdx = nextSubset(partition, startIdx)
		group := timeSeries[startIdx:endIdx]
		merger.MergeNewestFirst(
			group,
			func(ts *timeSeriesType, appender Appender) {
				ts.Fetch(
					c.applicationId,
					start,
					end,
					appender)
			},
			doneAppender)
		if doneAppender.Done {
			return
		}
	}
}

func (c *timeSeriesCollectionType) earliest(
	timeSeries []*timeSeriesType,
	timestampSeries map[int]*timestampSeriesType) (result float64) {

	for _, ts := range timeSeries {
		groupId := ts.id.GroupId()
		if timestampSeries[groupId].GivenUpPages() {
			earliest := timestampSeries[groupId].Earliest()
			if earliest > result {
				result = earliest
			}
		}
	}
	return
}

// Earliest returns the earliest time for which a given opentsdb time series
// is valid. name is the name of the time series. If given time series is
// new enough that no data has been evicted, Earliest just returns 0.0. If
// no time series with  name exists, Earliest just returns 0.0.
func (c *timeSeriesCollectionType) Earliest(name string) float64 {
	timeSeries, timestampSeries := c.TsAndTimeStampsByName(name)
	if len(timeSeries) == 0 {
		return 0.0
	}
	partition := GroupMetricByPathAndNumeric.orderedPartition(timeSeries)
	tslen := len(timeSeries)
	// Find the numeric one
	for startIdx, endIdx := 0, 0; startIdx < tslen; startIdx = endIdx {
		endIdx = nextSubset(partition, startIdx)
		if timeSeries[startIdx].id.Kind().CanToFromFloat() {
			return c.earliest(timeSeries[startIdx:endIdx], timestampSeries)
		}
	}
	return 0.0
}

func (c *timeSeriesCollectionType) tsdbTimeSeries(
	timeSeries []*timeSeriesType,
	timestampSeries map[int]*timestampSeriesType,
	start float64,
	end float64) (result tsdb.TimeSeries) {
	if start >= end {
		return
	}
	timestamps := make(map[int][]float64)
	merger := newMerger()
	merger.MergeOldestFirst(
		timeSeries,
		func(ts *timeSeriesType, appender Appender) {
			groupId := ts.id.GroupId()
			if timestamps[groupId] == nil {
				timestamps[groupId] = timestampSeries[groupId].FindBetween(start, end)
			}
			ts.FetchForwardWithTimeStamps(
				c.applicationId,
				timestamps[groupId],
				appender)
		},
		(*tsdbTsValueAppenderType)(&result))
	return
}

// Unique values by metric name in descending order happening before end and
// continuing until on or before start.
func (c *timeSeriesCollectionType) ByName(
	name string,
	start, end float64,
	strategy MetricGroupingStrategy,
	result Appender) {
	timeSeries := c.TsByName(name)
	partition := strategy.orderedPartition(timeSeries)
	c.byWhateverGroupBy(timeSeries, partition, start, end, result)
}

func (c *timeSeriesCollectionType) TsdbTimeSeries(
	name string, start, end float64) (tsdb.TimeSeries, bool) {
	timeSeries, timestampSeries := c.TsAndTimeStampsByName(name)
	if len(timeSeries) == 0 {
		return nil, false
	}
	partition := GroupMetricByPathAndNumeric.orderedPartition(timeSeries)
	tslen := len(timeSeries)
	// Find the numeric one
	for startIdx, endIdx := 0, 0; startIdx < tslen; startIdx = endIdx {
		endIdx = nextSubset(partition, startIdx)
		if timeSeries[startIdx].id.Kind().CanToFromFloat() {
			return c.tsdbTimeSeries(
				timeSeries[startIdx:endIdx], timestampSeries, start, end), true
		}
	}
	return nil, false
}

// Unique values by metric prefix in descending order happening before end and
// continuing until on or before start.
func (c *timeSeriesCollectionType) ByPrefix(
	prefix string,
	start, end float64,
	strategy MetricGroupingStrategy,
	result Appender) {
	timeSeries := c.TsByPrefix(prefix)
	partition := strategy.orderedPartition(timeSeries)
	c.byWhateverGroupBy(timeSeries, partition, start, end, result)
}

// Latest values for this instance.
func (c *timeSeriesCollectionType) LatestByPrefix(
	prefix string, strategy MetricGroupingStrategy, result Appender) {
	timeSeries, timestampSeries := c.TsAndTimeStampsByPrefix(prefix)
	partition := strategy.orderedPartition(timeSeries)
	tslen := len(timeSeries)
	latestRecordSlice := make([]Record, 1)
	latestTimeStamps := make(map[int][]float64)
	merger := newMerger()
	for startIdx, endIdx := 0, 0; startIdx < tslen; startIdx = endIdx {
		endIdx = nextSubset(partition, startIdx)
		group := timeSeries[startIdx:endIdx]
		latestRecordSlice := latestRecordSlice[:0]
		merger.MergeNewestFirst(
			group,
			func(ts *timeSeriesType, appender Appender) {
				groupId := ts.id.GroupId()
				if latestTimeStamps[groupId] == nil {
					latestTimeStamps[groupId] = []float64{
						timestampSeries[groupId].Latest(),
					}
				}
				ts.FetchForwardWithTimeStamps(
					c.applicationId,
					latestTimeStamps[groupId],
					appender)
			},
			AppenderLimit(AppendTo(&latestRecordSlice), 1),
		)
		if len(latestRecordSlice) >= 1 {
			if !result.Append(&latestRecordSlice[0]) {
				return
			}
		}
	}
}
