package store_test

import (
	"errors"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/trisource"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"math"
	"reflect"
	"testing"
	"time"
)

var (
	kEndpoint0 = scotty.NewEndpointWithConnector(
		"host1", 1001, sources.ConnectorList{trisource.GetConnector()})
	kEndpoint1 = scotty.NewEndpointWithConnector(
		"host2", 1002, sources.ConnectorList{trisource.GetConnector()})
	kEndpoint2 = scotty.NewEndpointWithConnector(
		"host3", 1001, sources.ConnectorList{trisource.GetConnector()})
	kEndpoint3 = scotty.NewEndpointWithConnector(
		"host4", 1002, sources.ConnectorList{trisource.GetConnector()})
	kError          = errors.New("An error")
	kUsualTimeStamp = time.Date(2016, 7, 8, 14, 11, 0, 0, time.Local)
	kNoMetaData     = newExpectedMetaData()
)

type playbackType struct {
	valueCount     int
	nameToIndexMap map[string]int
	timeStamps     map[int][]float64
	values         [][]interface{}
	original       metrics.SimpleList
}

func countItems(iter store.Iterator) (result int) {
	var r store.Record
	for iter.Next(&r) {
		result++
	}
	return result
}

func newPlaybackType(mlist metrics.List, valueCount int) *playbackType {
	length := mlist.Len()
	original := make(metrics.SimpleList, length)
	nameToIndexMap := make(map[string]int, length)
	for i := range original {
		mlist.Index(i, &original[i])
		if _, ok := nameToIndexMap[original[i].Path]; ok {
			panic("path names must be unique.")
		}
		nameToIndexMap[original[i].Path] = i
	}
	return &playbackType{
		valueCount:     valueCount,
		nameToIndexMap: nameToIndexMap,
		timeStamps:     make(map[int][]float64),
		values:         make([][]interface{}, length),
		original:       original,
	}
}

func (p *playbackType) AddTimes(groupId int, times ...float64) {
	if len(times) != p.valueCount {
		panic("Wrong number of times supplied.")
	}
	p.timeStamps[groupId] = times
}

func (p *playbackType) Add(path string, values ...interface{}) {
	if len(values) != p.valueCount {
		panic("Wrong number of values supplied.")
	}
	idx, ok := p.nameToIndexMap[path]
	if !ok {
		panic("Invalid path to Add()")
	}
	p.values[idx] = values
}

func (p *playbackType) Play(aStore *store.Store, endpointId interface{}) error {
	for i := 0; i < p.valueCount; i++ {
		var cmetrics metrics.SimpleList
		for j := range p.values {
			if p.values[j] == nil {
				panic("Values not supplied for all metrics.")
			}
			if p.values[j][i] != nil {
				aValue := p.original[j]
				aValue.Value = p.values[j][i]
				groupId := aValue.GroupId
				if p.timeStamps[groupId] == nil {
					panic("Timestamps not supplied for all groups.")
				}
				aValue.TimeStamp = duration.FloatToTime(p.timeStamps[groupId][i])
				cmetrics = append(cmetrics, aValue)
			}
		}
		if _, err := aStore.AddBatch(endpointId, 1000.0, cmetrics.Sorted()); err != nil {
			return err
		}
	}
	return nil
}

type limitIteratorType struct {
	limit   int
	wrapped store.Iterator
}

func (l *limitIteratorType) Next(r *store.Record) bool {
	if l.limit == 0 {
		return false
	}
	l.limit--
	return l.wrapped.Next(r)
}

func iteratorLimit(wrapped store.Iterator, limit int) store.Iterator {
	return &limitIteratorType{limit: limit, wrapped: wrapped}
}

type sumMetricsType int64

func (s *sumMetricsType) Append(r *store.Record) bool {
	switch i := r.Value.(type) {
	case int64:
		*s += sumMetricsType(i)
	case int32:
		*s += sumMetricsType(i)
	default:
		panic("Bad type")
	}
	return true
}

func (s *sumMetricsType) Visit(
	astore *store.Store, e interface{}) error {
	astore.ByEndpoint(e, 0, 1000.0, s)
	return nil
}

type countAppenderType int

func (c *countAppenderType) Append(r *store.Record) bool {
	(*c)++
	return true
}

type errVisitor int

func (e *errVisitor) Visit(
	astore *store.Store, ee interface{}) error {
	return kError
}

type appenderToTestClientType struct {
	expectedCalls int
	actualCalls   int
}

// runAppenderClientTest ensures that clients of appenders are well behaved.
// That is, as soon as the Appender returns false, the client stops appending.
func runAppenderClientTest(t *testing.T, underTest func(a store.Appender)) {
	keepGoing := true
	expectedCalls := 0
	for keepGoing {
		expectedCalls++
		appender := &appenderToTestClientType{
			expectedCalls: expectedCalls}
		underTest(appender)
		keepGoing = appender.Verify(t)
	}
}

func (a *appenderToTestClientType) Append(r *store.Record) bool {
	a.actualCalls++
	return a.actualCalls != a.expectedCalls
}

// Verify verifies appender and returns true if test should continue
func (a *appenderToTestClientType) Verify(t *testing.T) bool {
	// We are done testing
	if a.actualCalls < a.expectedCalls {
		return false
	}
	if a.actualCalls > a.expectedCalls {
		t.Errorf(
			"Expected %d but got %d calls to Append",
			a.expectedCalls, a.actualCalls)
	}
	return true
}

func newStore(
	t *testing.T,
	testName string,
	valueCount,
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *store.Store {
	result := store.NewStore(
		valueCount, pageCount, inactiveThreshhold, degree)
	dirSpec, err := tricorder.RegisterDirectory("/" + testName)
	if err != nil {
		t.Fatalf("Duplicate test: %s", testName)
	}
	result.RegisterMetrics(dirSpec)
	return result
}

func TestVisitorError(t *testing.T) {
	aStore := newStore(t, "TestVisitorError", 1, 8, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)
	var ev errVisitor
	assertValueEquals(t, kError, aStore.VisitAllEndpoints(&ev))
}

func TestAggregateAppenderAndVisitor(t *testing.T) {
	aStore := newStore(
		t, "TestAggregateAppenderAndVisitor", 10, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)

	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
		},
	}

	aMetric[0].Value = int64(1)
	aStore.AddBatch(kEndpoint0, 100.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(2)
	aStore.AddBatch(kEndpoint0, 107.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(3)
	aStore.AddBatch(kEndpoint0, 114.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(4)
	aStore.AddBatch(kEndpoint0, 121.0, aMetric[:].Sorted())

	aMetric[0].Value = int64(11)
	aStore.AddBatch(kEndpoint1, 100.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(12)
	aStore.AddBatch(kEndpoint1, 107.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(13)
	aStore.AddBatch(kEndpoint1, 114.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(14)
	aStore.AddBatch(kEndpoint1, 121.0, aMetric[:].Sorted())

	var total sumMetricsType

	aStore.VisitAllEndpoints(&total)
	assertValueEquals(t, 60, int(total))

	total = 0
	aStore.ByEndpoint(kEndpoint0, 0, 1000.0, &total)
	assertValueEquals(t, 10, int(total))

	total = 0
	aStore.ByNameAndEndpoint("/foo/bar", kEndpoint1, 0, 1000.0, &total)
	assertValueEquals(t, 50, int(total))

	total = 0
	aStore.LatestByEndpoint(kEndpoint1, &total)
	assertValueEquals(t, 14, int(total))

	aMetric[0].Value = int32(17)
	aStore.AddBatch(kEndpoint1, 128.0, aMetric[:].Sorted())

	var result []store.Record
	aStore.LatestByEndpoint(kEndpoint1, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))

	result = nil
	aStore.LatestByPrefixAndEndpointStrategy(
		"/foo/bar",
		kEndpoint1,
		store.GroupMetricByPathAndNumeric,
		store.AppendTo(&result))
	assertValueEquals(t, 1, len(result))
}

type descendingCheckerType struct {
	visited       map[interface{}]bool
	visiting      interface{}
	lastTimeStamp float64
	strategy      store.MetricGroupingStrategy
}

func newDescendingChecker(
	strategy store.MetricGroupingStrategy) *descendingCheckerType {
	return &descendingCheckerType{
		visited:  make(map[interface{}]bool),
		strategy: strategy}
}

func (c *descendingCheckerType) Check(t *testing.T, r *store.Record) {
	key := c.strategy(r.Info)
	if key == c.visiting {
		if r.TimeStamp >= c.lastTimeStamp {
			t.Errorf(
				"Timestamp %f comes after %f",
				r.TimeStamp, c.lastTimeStamp)
		} else {
			c.lastTimeStamp = r.TimeStamp
		}
	} else if c.visited[key] {
		t.Errorf("Records for %s not contiguous.", r.Info.Path())
	} else {
		if c.visiting != nil {
			c.visited[c.visiting] = true
		}
		c.visiting = key
		c.lastTimeStamp = r.TimeStamp
	}
}

type sanityCheckerType struct {
	lastTsMap map[interface{}]float64
	strategy  store.MetricGroupingStrategy
}

func newSanityChecker(
	strategy store.MetricGroupingStrategy) *sanityCheckerType {
	var result sanityCheckerType
	result.Init(strategy)
	return &result
}

func (c *sanityCheckerType) Init(strategy store.MetricGroupingStrategy) {
	c.lastTsMap = make(map[interface{}]float64)
	c.strategy = strategy
}

func (c *sanityCheckerType) copyTo(dest *sanityCheckerType) {
	dest.lastTsMap = make(map[interface{}]float64, len(c.lastTsMap))
	for k, v := range c.lastTsMap {
		dest.lastTsMap[k] = v
	}
	dest.strategy = c.strategy
}

func (c *sanityCheckerType) Check(
	t *testing.T, r *store.Record) (lastTs float64, ok bool) {
	ts := r.TimeStamp
	key := c.strategy(r.Info)
	lastTs, ok = c.lastTsMap[key]
	c.lastTsMap[key] = ts
	if ok && ts <= lastTs {
		t.Errorf(
			"(%s %f) does not come after %f",
			r.Info.Path(),
			ts,
			lastTs)
	}
	return
}

type expectedMetaDataType struct {
	descriptions     map[string]string
	units            map[string]units.Unit
	kinds            map[string]types.Type
	subTypes         map[string]types.Type
	isNotCumulatives map[string]bool
	bits             map[string]int
	groupIds         map[string]int
}

func newExpectedMetaData() *expectedMetaDataType {
	return &expectedMetaDataType{
		descriptions:     make(map[string]string),
		units:            make(map[string]units.Unit),
		kinds:            make(map[string]types.Type),
		subTypes:         make(map[string]types.Type),
		isNotCumulatives: make(map[string]bool),
		bits:             make(map[string]int),
		groupIds:         make(map[string]int),
	}
}

func (e *expectedMetaDataType) AddDescription(path string, description string) {
	e.descriptions[path] = description
}

func (e *expectedMetaDataType) AddUnit(path string, unit units.Unit) {
	e.units[path] = unit
}

func (e *expectedMetaDataType) AddKind(path string, kind types.Type) {
	e.kinds[path] = kind
}

func (e *expectedMetaDataType) AddSubType(
	path string, subType types.Type) {
	e.subTypes[path] = subType
}

func (e *expectedMetaDataType) AddIsNotCumulative(
	path string, isNotCumulative bool) {
	e.isNotCumulatives[path] = isNotCumulative
}

func (e *expectedMetaDataType) AddBits(path string, bits int) {
	e.bits[path] = bits
}

func (e *expectedMetaDataType) AddGroupId(path string, groupId int) {
	e.groupIds[path] = groupId
}

func (e *expectedMetaDataType) Verify(t *testing.T, m *store.MetricInfo) {
	path := m.Path()
	if desc, ok := e.descriptions[path]; ok {
		assertValueEquals(t, desc, m.Description())
	}
	if unit, ok := e.units[path]; ok {
		assertValueEquals(t, unit, m.Unit())
	}
	if kind, ok := e.kinds[path]; ok {
		assertValueEquals(t, kind, m.Kind())
	}
	if subType, ok := e.subTypes[path]; ok {
		assertValueEquals(t, subType, m.SubType())
	}
	if isNotCumulative, ok := e.isNotCumulatives[path]; ok {
		assertValueEquals(t, isNotCumulative, m.IsNotCumulative())
	}
	if bits, ok := e.bits[path]; ok {
		assertValueEquals(t, bits, m.Bits())
	}
	if groupId, ok := e.groupIds[path]; ok {
		assertValueEquals(t, groupId, m.GroupId())
	}
}

type nameAndTsType struct {
	Name string
	Ts   float64
}

type interfaceAndActiveType struct {
	Value  interface{}
	Active bool
}

type expectedTsValuesType struct {
	metaData      *expectedMetaDataType
	sanityChecker sanityCheckerType
	values        map[nameAndTsType]interfaceAndActiveType
}

func newExpectedTsValues() *expectedTsValuesType {
	return newExpectedTsValuesWithMetaDataAndStrategy(
		kNoMetaData, store.GroupMetricByPathAndNumeric)
}

func newExpectedTsValuesWithMetaData(
	metaData *expectedMetaDataType) *expectedTsValuesType {
	return newExpectedTsValuesWithMetaDataAndStrategy(
		metaData, store.GroupMetricByPathAndNumeric)
}

func newExpectedTsValuesWithMetaDataAndStrategy(
	metaData *expectedMetaDataType,
	strategy store.MetricGroupingStrategy) *expectedTsValuesType {
	result := &expectedTsValuesType{
		metaData: metaData,
		values:   make(map[nameAndTsType]interfaceAndActiveType),
	}
	result.sanityChecker.Init(strategy)
	return result
}

func (e *expectedTsValuesType) copyTo(dest *expectedTsValuesType) {
	dest.metaData = e.metaData
	e.sanityChecker.copyTo(&dest.sanityChecker)
	dest.values = make(
		map[nameAndTsType]interfaceAndActiveType, len(e.values))
	for k, v := range e.values {
		dest.values[k] = v
	}
}

func (e *expectedTsValuesType) Checkpoint() (checkpoint interface{}) {
	var result expectedTsValuesType
	e.copyTo(&result)
	return &result
}

func (e *expectedTsValuesType) Restore(checkpoint interface{}) {
	source := checkpoint.(*expectedTsValuesType)
	source.copyTo(e)
}

func (e *expectedTsValuesType) Add(
	name string, ts float64, value interface{}) {
	e.values[nameAndTsType{name, ts}] = interfaceAndActiveType{
		Value: value, Active: true}
}

func (e *expectedTsValuesType) AddInactive(
	name string, ts float64, value interface{}) {
	e.values[nameAndTsType{name, ts}] = interfaceAndActiveType{
		Value: value, Active: false}
}

func (e *expectedTsValuesType) checkContents(
	t *testing.T, r *store.Record) {
	name := r.Info.Path()
	ts := r.TimeStamp
	nameTs := nameAndTsType{name, ts}
	value := r.Value
	active := r.Active
	expectedVal, ok := e.values[nameTs]
	if !ok {
		t.Errorf("(%s, %f) = %v not expected", name, ts, value)
	} else {
		if !r.Info.ValuesAreEqual(expectedVal.Value, value) {
			t.Errorf(
				"Expected %v for (%s, %f) got %v",
				expectedVal.Value,
				name,
				ts,
				value)
		}
		if active != expectedVal.Active {
			if expectedVal.Active {
				t.Errorf(
					"Expected active for (%s, %f)",
					name,
					ts)
			} else {
				t.Errorf(
					"Expected inactive for (%s, %f)",
					name,
					ts)
			}

		}
		delete(e.values, nameTs)
	}
}

// checks that results slice is equivalent to this instance. Unlike Iterate,
// this method does not change the state of this instance.
func (e *expectedTsValuesType) CheckSlice(
	t *testing.T, results []store.Record) (count int) {
	descendingChecker := newDescendingChecker(e.sanityChecker.strategy)
	ecopy := newExpectedTsValues()
	e.copyTo(ecopy)
	var lastInfo *store.MetricInfo
	for i := range results {
		descendingChecker.Check(t, &results[i])
		if results[i].Info != lastInfo {
			ecopy.metaData.Verify(t, results[i].Info)
			lastInfo = results[i].Info
		}
		ecopy.checkContents(t, &results[i])
	}
	ecopy.VerifyDone(t)
	return len(results)
}

func (e *expectedTsValuesType) Iterate(
	t *testing.T, iterator store.Iterator) (count int) {
	var r store.Record
	var lastInfo *store.MetricInfo
	for iterator.Next(&r) {
		e.sanityChecker.Check(t, &r)
		if r.Info != lastInfo {
			e.metaData.Verify(t, r.Info)
			lastInfo = r.Info
		}
		count++
		e.checkContents(t, &r)
	}
	return
}

func (e *expectedTsValuesType) VerifyDone(t *testing.T) {
	for nameTs, val := range e.values {
		t.Errorf("Expected (%s, %f) = %v", nameTs.Name, nameTs.Ts, val.Value)
	}
}

type iteratorPageEvictionTestType struct {
	MinTimeStamp float64
	MaxTimeStamp float64
	Count        int
}

func (c *iteratorPageEvictionTestType) Iterate(
	t *testing.T, iter store.Iterator) {
	*c = iteratorPageEvictionTestType{}
	sanityChecker := newSanityChecker(store.GroupMetricByPathAndNumeric)
	var r store.Record
	for iter.Next(&r) {
		lastTs, ok := sanityChecker.Check(t, &r)
		if ok && r.TimeStamp-lastTs != 10.0 {
			t.Error("Expected no gaps in timestamps")
		}
		var expectedValue interface{}
		if r.Info.Path() == "Alice" {
			expectedValue = 2 * (int64(r.TimeStamp) / 20)
		} else if r.Info.Path() == "Bob" {
			expectedValue = 2*(int64(r.TimeStamp)/20) + 1
		} else {
			t.Fatalf("Unexpected name %s encountered", r.Info.Path())
		}
		if r.Value != expectedValue {
			t.Errorf(
				"Expected %v, got %v for (%s %f)",
				expectedValue,
				r.Value,
				r.Info.Path(),
				r.TimeStamp,
			)
		}
		c.Count++
		if c.MinTimeStamp == 0 || r.TimeStamp < c.MinTimeStamp {
			c.MinTimeStamp = r.TimeStamp
		}
		if r.TimeStamp > c.MaxTimeStamp {
			c.MaxTimeStamp = r.TimeStamp
		}
	}
}

func TestIteratorPageEviction(t *testing.T) {
	var consumer iteratorPageEvictionTestType
	// max 13 pages. 2 records per page. What this will hold is hard
	// to predict, but we expect it to hold 6 to 7 unique values for
	// each metric along with with 12 to 14 timestamps.
	// (3 pages for values for each metric) * (2 metrics) = 6 pages
	// plus 7 pages for 14 timestamps) = 7 pages
	// 6 pages + 7 pages = 13 pages
	aStore := newStore(t, "TestIteratorPageEviction", 2, 13, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "Alice",
			Description: "A description",
		},
		{
			Path:        "Bob",
			Description: "A description",
		},
	}
	// 2 endpoints 5 distinct values per endpoint = 2 * 2 = 4 pages
	// 10 timestamps = 5 pages
	for ts := 100; ts < 200; ts += 10 {
		aMetric[0].Value = int64(2 * (ts / 20))
		aMetric[1].Value = int64(2*(ts/20) + 1)
		aStore.AddBatch(kEndpoint0, float64(ts), aMetric[:].Sorted())
	}
	iterator, _ := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	consumer.Iterate(t, iterator)
	assertValueEquals(t, 20, consumer.Count)

	iterator.Commit()

	iterator, _ = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	consumer.Iterate(t, iterator)
	assertValueEquals(t, 0, consumer.Count)

	for ts := 200; ts < 300; ts += 10 {
		aMetric[0].Value = int64(2 * (ts / 20))
		aMetric[1].Value = int64(2*(ts/20) + 1)
		aStore.AddBatch(kEndpoint0, float64(ts), aMetric[:].Sorted())
	}

	iterator, _ = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 5)
	consumer.Iterate(t, iterator)
	// 2 time series * max 5 value, timestamp pairs each
	assertValueEquals(t, 10, consumer.Count)
	// We only get to timestamp 240
	assertValueEquals(t, 240.0, consumer.MaxTimeStamp)

	iterator.Commit()

	// Insert lots of new values to evict some pages
	for ts := 300; ts < 500; ts += 10 {
		aMetric[0].Value = int64(2 * (ts / 20))
		aMetric[1].Value = int64(2*(ts/20) + 1)
		aStore.AddBatch(kEndpoint0, float64(ts), aMetric[:].Sorted())
	}
	iterator, _ = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	consumer.Iterate(t, iterator)
	if consumer.MinTimeStamp < 360.0 { // max 14 timestamps
		t.Error("Expected some values to be skipped.")
	}
	if consumer.MinTimeStamp > 380.0 { // at least 12 timestamps
		t.Error("Expected at least 12 timestamps")
	}
	assertValueEquals(t, 490.0, consumer.MaxTimeStamp)
	if consumer.Count < 24 { // at least 12*2 values
		t.Error("Expected at least 24 values")
	}

	// Now remove all pages. This should actually remove all pages save 1.
	aStore.LessenPageCount(1.0)

	anotherIterator, _ := aStore.NamedIteratorForEndpoint(
		"anotherIterator", kEndpoint0, 0)
	var anotherConsumer iteratorPageEvictionTestType
	anotherConsumer.Iterate(t, anotherIterator)
	assertValueEquals(t, 490.0, anotherConsumer.MaxTimeStamp)
	// The most values we can get now is 6. That would be if the timestamp
	// series has the remaining page which means it can hold 3 timestamps.
	// 2 values * 3 timestamps = 6 values.
	if anotherConsumer.Count > 6 {
		t.Error("Expected no more than 6 values")
	}

	// If we add new values, we should at least get those back
	aMetric[0].Value = int64(1234)
	aMetric[1].Value = int64(1235)
	aStore.AddBatch(kEndpoint0, 600.0, aMetric[:].Sorted())

	var result []store.Record
	aStore.ByEndpoint(
		kEndpoint0, 600.0, 601.0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
}

func floatToTime(f float64) time.Time {
	return duration.FloatToTime(f)
}

func TestRollUpIterator(t *testing.T) {
	aStore := newStore(t, "TestRollUpIterator", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "Int",
			Description: "An int",
			GroupId:     0,
		},
		{
			Path:        "Float",
			Description: "A float",
			GroupId:     2,
		},
		{
			Path:        "String",
			Description: "A string",
			GroupId:     2,
		},
		{
			Path:        "Inactive",
			Description: "A description",
			GroupId:     0,
		},
	}
	playback := newPlaybackType(aMetric[:], 10)
	playback.AddTimes(
		0,
		119940.0, 119999.5, 120059.0, // 1st interval
		120190.0, 120240.0, // 2nd interval
		120340.0, 120350.0, 120360.0, 120370.0, // 3rd interval
		120440.0, // 4th interval
	)
	playback.Add(
		"Int",
		int64(23000), int64(29000), int64(28000), // 1st
		int64(31000), int64(32000), // 2nd
		int64(35000), int64(17000), int64(19000), int64(22001), // 3rd
		int64(27000), // 4th 10 total
	)
	playback.Add(
		"Inactive",
		nil, 21.0, 27.0, // 1st
		nil, nil, // 2nd
		9.0, nil, 13.0, nil, // 3rd
		8.3, // 4th 10 total
	)

	playback.AddTimes(
		2,
		95940.0, 95972.0, 96004.0, 96036.0, // 1st interval
		96180.0,          // 2nd interval
		96340.0, 96360.0, // 3rd interval
		96440.0,          // 4th interval
		96540.0, 96550.0, // 5th interval
	)
	playback.Add(
		"Float",
		4.75, 5.25, 6.25, 6.75, // 1st
		9.125,        // 2nd
		1.375, 2.375, // 3rd
		3.1875,     // 4th
		10.0, 11.0, // 5th
	)
	playback.Add(
		"String",
		"hello", "goodbye", "solong", "seeya", // 1st
		"bee",           // 2nd
		"long", "short", // 3rd
		"near",       // 4th
		"far", "too", // 5th
	)
	// Assume 0 time left when we haven't added anything
	assertValueEquals(t, 0.0, aStore.TimeLeft("anIterator"))

	playback.Play(aStore, kEndpoint0)

	// 96610.0 - 96000 > 120500.0 - 120000
	assertValueEquals(t, 610.0, aStore.TimeLeft("anIterator"))

	expected := newExpectedTsValues()
	expected.Add("Int", 120000.0, int64(26667))
	expected.Add("Float", 96000.0, 5.75)
	expected.Add("String", 96000.0, "hello")
	expected.Add("Inactive", 120000.0, 24.0)

	expected.Add("Int", 120240.0, int64(31500))
	expected.Add("Float", 96240.0, 9.125)
	expected.Add("String", 96240.0, "bee")

	expected.Add("Int", 120360.0, int64(23250))
	expected.Add("Float", 96360.0, 1.875)
	expected.Add("String", 96360.0, "long")
	expected.Add("Inactive", 120360.0, 11.0)

	expected.Add("Float", 96480.0, 3.1875)
	expected.Add("String", 96480.0, "near")

	beginning := expected.Checkpoint()

	iterator, timeAfterIterator := aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
	assertValueEquals(t, 110.0, timeAfterIterator)

	// We never committed progress, so timeLeft remains unchanged
	assertValueEquals(t, 610.0, aStore.TimeLeft("anIterator"))

	// Shouldn't get anything else off this iterator
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)

	// Now iterate again but in chunks. In this test, don't commit
	// every 2nd iteration to show that in that case the next iterator
	// starts at the same place.
	expected.Restore(beginning)

	// max 3 times per metric
	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		3,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.Restore(beginning)
	iterator, timeAfterIterator = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		3,
		store.GroupMetricByPathAndNumeric)
	assertValueEquals(t, 11, expected.Iterate(t, iterator))
	assertValueEquals(t, 190.0, timeAfterIterator)
	iterator.Commit()

	assertValueEquals(t, 190.0, aStore.TimeLeft("anIterator"))

	checkpoint := expected.Checkpoint()
	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		3,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.Restore(checkpoint)
	iterator, timeAfterIterator = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		3,
		store.GroupMetricByPathAndNumeric)
	assertValueEquals(t, 2, expected.Iterate(t, iterator))
	assertValueEquals(t, 110.0, timeAfterIterator)
	iterator.Commit()

	assertValueEquals(t, 110.0, aStore.TimeLeft("anIterator"))

	expected.VerifyDone(t)

	iterator, timeAfterIterator = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		3,
		store.GroupMetricByPathAndNumeric)
	assertValueEquals(t, 110.0, timeAfterIterator)
	// Shouldn't get anything off iterator
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)

	// Now iterate again but test iterating 8 at a time, committing
	// and creating a new iterator. Since we committed the previous
	// iterator, we have to use a new name to start from the beginning.
	expected.Restore(beginning)
	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anotherIterator",
		kEndpoint0,
		2*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)
	assertValueEquals(t, 8, expected.Iterate(
		t, iteratorLimit(iterator, 8)))
	iterator.Commit()

	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anotherIterator",
		kEndpoint0,
		2*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)
	assertValueEquals(t, 5, expected.Iterate(
		t, iteratorLimit(iterator, 8)))
	iterator.Commit()
	expected.VerifyDone(t)

	// Verify that incomplete intervals aren't lost.
	playback = newPlaybackType(aMetric[:3], 2)

	playback.AddTimes(
		0,
		120460.0, // 4th cont
		120540.0, // 5th
	)
	playback.Add(
		"Int",
		int64(29000), // 4th cont
		int64(33600), // 5th
	)

	playback.AddTimes(
		2,
		96563.0, // 5th cont
		96660.0, // 6th
	)
	playback.Add(
		"Float",
		15.0, // 5th cont
		12.5, // 6th
	)
	playback.Add(
		"String",
		"dog",   // 5th cont
		"mouse", // 6th
	)

	playback.Play(aStore, kEndpoint0)

	expected = newExpectedTsValues()
	expected.Add("Int", 120480.0, int64(28000))
	expected.Add("Float", 96600.0, 12.0)
	expected.Add("String", 96600.0, "far")
	expected.Add("Inactive", 120480.0, 8.3)

	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		2*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)

	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
}

func TestRollUpIteratorBool(t *testing.T) {
	aStore := newStore(t, "TestRollUpIteratorBool", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "path",
			Description: "A bool",
		},
	}
	playback := newPlaybackType(aMetric[:], 8)
	playback.AddTimes(
		0,
		29850.0, 29950.0, 30050.0, // 1st interval
		30150.0, 30250.0, 30350.0, // 2nd interval
		30750.0, // 3rd interval
		31050.0, // 4th interval
	)
	playback.Add(
		"path",
		true, false, false, // 1st
		false, true, true, // 2nd
		true,  // 3rd
		false, // 4th
	)
	playback.Play(aStore, kEndpoint0)

	expected := newExpectedTsValues()
	expected.Add("path", 30000.0, true)
	expected.Add("path", 30300.0, false)
	expected.Add("path", 30900.0, true)

	iterator, _ := aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		5*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
}

func TestRollUpIteratorInt8(t *testing.T) {
	aStore := newStore(t, "TestRollUpIteratorInt8", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "path",
			Description: "An int",
		},
	}
	playback := newPlaybackType(aMetric[:], 8)
	playback.AddTimes(
		0,
		29850.0, 29950.0, 30050.0, // 1st interval
		30150.0, 30250.0, 30350.0, // 2nd interval
		30750.0, // 3rd interval
		31050.0, // 4th interval
	)
	playback.Add(
		"path",
		int8(-128), int8(127), int8(5), // 1st interval
		int8(-128), int8(-128), int8(-128), // 2nd interval
		int8(127), // 3rd
		int8(0),   // 4th
	)
	playback.Play(aStore, kEndpoint0)

	expected := newExpectedTsValues()
	expected.Add("path", 30000.0, int8(1))
	expected.Add("path", 30300.0, int8(-128))
	expected.Add("path", 30900.0, int8(127))

	iterator, _ := aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		5*time.Minute,
		0,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
}

func TestIteratorSamePathDifferentTypeRollUp(t *testing.T) {
	aStore := newStore(
		t, "TestIteratorSamePathDifferentTypeRollUp", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "foo",
			Description: "An int64 or float64 or string",
		},
	}
	playback := newPlaybackType(aMetric, 13)
	playback.AddTimes(
		0,
		1170.0, 1180.0, 1190.0, 1200.0, 1210.0, 1220.0,
		1230.0, 1240.0, 1250.0, 1260.0,
		1300.0, 1310.0,
		// Here to force first two periods to get written out
		1350.0,
	)
	playback.Add(
		"foo",
		int64(1000), int64(1010), int64(1020),
		float64(2030.0), float64(2040.0), float64(2050.0),
		float64(2060.0), float64(2070.0), float64(2080.0), int64(1110),
		"hello", "how",
		"yo",
	)
	playback.Play(aStore, kEndpoint0)

	expected := newExpectedTsValues()

	expected.Add("foo", 1200.0, int64(1525))
	expected.Add("foo", 1260.0, float64(1830.0))
	expected.Add("foo", 1320.0, "hello")

	iterator, _ := aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		60*time.Second,
		0,
		store.GroupMetricByPathAndNumeric)
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
}

func TestIteratorSamePathDifferentType(t *testing.T) {
	aStore := newStore(
		t, "TestIteratorSamePathDifferentType", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "foo",
			Description: "An int64 or int32",
		},
		{
			Path:        "bar",
			Description: "an int 16",
		},
	}
	playback := newPlaybackType(aMetric, 3)
	playback.AddTimes(
		0,
		1010.0, 1020.0, 1030.0,
	)
	playback.Add(
		"foo",
		int64(10), int32(20), int64(30),
	)
	playback.Add(
		"bar",
		int16(11), int16(21), int16(31),
	)
	playback.Play(aStore, kEndpoint0)

	expected := newExpectedTsValues()

	expected.Add("foo", 1010.0, int64(10))
	expected.Add("bar", 1010.0, int16(11))
	expected.Add("foo", 1020.0, int32(20))
	expected.Add("bar", 1020.0, int16(21))
	expected.Add("foo", 1030.0, int64(30))
	expected.Add("bar", 1030.0, int16(31))

	iterator, _ := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)
}

func TestTsdbTimeSeries(t *testing.T) {
	aStore := newStore(t, "TestTsdbTimeSeries", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "Alice",
			Description: "A description",
			GroupId:     1,
		},
	}
	playback := newPlaybackType(aMetric, 10)
	playback.AddTimes(
		1,
		100.0, 200.0, 300.0, 400.0, 500.0,
		600.0, 700.0, 800.0, 900.0, 1000.0,
	)
	playback.Add(
		"Alice",
		int32(10), int64(20), 30.0, 40.0, uint8(50.0),
		int32(60), int64(70), uint8(80), uint16(90), 100.0)
	playback.Play(aStore, kEndpoint0)
	aMetric = metrics.SimpleList{
		{
			Path:        "Alice",
			Description: "A description",
			GroupId:     3,
		},
	}
	playback = newPlaybackType(aMetric, 8)
	playback.AddTimes(
		3,
		150.0, 250.0, 350.0, 450.0,
		550.0, 650.0, 750.0, 850.0,
	)
	playback.Add(
		"Alice",
		uint32(15), uint64(25), uint8(35), uint16(45),
		uint32(55), uint64(65), uint8(75), uint16(85))
	playback.Play(aStore, kEndpoint0)

	var timeSeries tsdb.TimeSeries
	var ok bool
	if timeSeries, ok = aStore.TsdbTimeSeries(
		"Alice", kEndpoint0, 0, 1200.0); !ok {
		t.Fatal("Expected to find metric Alice")
	}
	expectedTimeSeries := tsdb.TimeSeries{
		{100.0, 10.0}, {150.0, 15.0}, {200.0, 20.0}, {250.0, 25.0},
		{300.0, 30.0}, {350.0, 35.0}, {400.0, 40.0}, {450.0, 45.0},
		{500.0, 50.0}, {550.0, 55.0}, {600.0, 60.0}, {650.0, 65.0},
		{700.0, 70.0}, {750.0, 75.0}, {800.0, 80.0}, {850.0, 85.0},
		{900.0, 90.0}, {1000.0, 100.0}}
	assertValueDeepEquals(t, expectedTimeSeries, timeSeries)
	if timeSeries, ok = aStore.TsdbTimeSeries(
		"Alice", kEndpoint0, 150.0, 300.0); !ok {
		t.Fatal("Expected to find metric Alice")
	}
	expectedTimeSeries = tsdb.TimeSeries{
		{150.0, 15.0}, {200.0, 20.0}, {250.0, 25.0}}
	assertValueDeepEquals(t, expectedTimeSeries, timeSeries)
	if timeSeries, ok = aStore.TsdbTimeSeries(
		"Alice", kEndpoint0, 10000.0, 11000.0); !ok {
		t.Fatal("Expected to find metric Alice")
	}
	assertValueEquals(t, 0, len(timeSeries))
	_, ok = aStore.TsdbTimeSeries("Bob", kEndpoint0, 500.0, 600.0)
	if ok {
		t.Error("Expected not to find time series bob.")
	}
}

func TestLatest(t *testing.T) {
	aStore := newStore(t, "TestLatest", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "Alice",
			Description: "A description",
			GroupId:     0,
		},
	}
	playback := newPlaybackType(aMetric[:], 3)
	playback.AddTimes(
		0,
		100.0, 200.0, 300.0,
	)
	playback.Add(
		"Alice",
		int64(357), int64(357), int64(357),
	)
	playback.Play(aStore, kEndpoint0)
	expected := newExpectedTsValues()
	expected.Add("Alice", 300.0, int64(357))
	var results []store.Record
	aStore.LatestByPrefixAndEndpointStrategy(
		"Alice",
		kEndpoint0,
		store.GroupMetricByPathAndNumeric,
		store.AppendTo(&results))
	expected.CheckSlice(t, results)
}

func TestIterator(t *testing.T) {
	aStore := newStore(t, "TestIterator", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "Alice",
			Description: "A description",
			GroupId:     0,
		},
		{
			Path:        "Bob",
			Description: "A description",
			GroupId:     0,
		},
		{
			Path:        "Charlie",
			Description: "A description",
			GroupId:     2,
		},
		{
			Path:        "FoxTrot",
			Description: "A description",
			GroupId:     2,
		},
	}
	playback := newPlaybackType(aMetric[:], 11)
	playback.AddTimes(
		0,
		100.0, 200.0, 300.0, 400.0, 500.0,
		500.0, 500.0, 800.0, 900.0, 1000.0,
		1100.0,
	)
	playback.Add(
		"Alice",
		int64(0), int64(0), int64(200), int64(200), int64(400),
		int64(400), nil, int64(700), nil, int64(900),
		nil,
	)
	playback.Add(
		"Bob",
		int64(1), int64(101), int64(201), int64(201), int64(401),
		int64(401), int64(401), nil, nil, int64(901),
		nil,
	)
	playback.AddTimes(
		2,
		102.0, 202.0, 302.0, 402.0, 502.0,
		602.0, 702.0, 802.0, 902.0, 1002.0,
		1102.0,
	)
	playback.Add(
		"Charlie",
		int32(2), int32(2), int32(202), int32(302), int32(402),
		int32(402), int32(402), int32(402), int32(402), nil,
		nil,
	)
	playback.Add(
		"FoxTrot",
		int64(3), int64(3), int64(203), int64(203), int64(403),
		int64(503), int64(603), int64(703), int64(803), nil,
		nil,
	)
	playback.Play(aStore, kEndpoint0)

	// Test tsdbTimeSeries
	timeSeries, _ := aStore.TsdbTimeSeries("FoxTrot", kEndpoint0, 202.0, 502.0)
	expectedTimeSeries := tsdb.TimeSeries{
		{202.0, 3.0}, {302.0, 203.0}, {402.0, 203.0}}
	assertValueDeepEquals(t, expectedTimeSeries, timeSeries)

	// Test with missing values
	timeSeries, _ = aStore.TsdbTimeSeries("FoxTrot", kEndpoint0, 600.0, 2000.0)
	expectedTimeSeries = tsdb.TimeSeries{
		{602.0, 503.0}, {702.0, 603.0}, {802.0, 703.0}, {902.0, 803.0}}
	assertValueDeepEquals(t, expectedTimeSeries, timeSeries)

	expected := newExpectedTsValues()
	expected.Add("Alice", 100.0, int64(0))
	expected.Add("Bob", 100.0, int64(1))
	expected.Add("Charlie", 102.0, int32(2))
	expected.Add("FoxTrot", 102.0, int64(3))

	expected.Add("Alice", 200.0, int64(0))
	expected.Add("Bob", 200.0, int64(101))
	expected.Add("Charlie", 202.0, int32(2))
	expected.Add("FoxTrot", 202.0, int64(3))

	expected.Add("Alice", 300.0, int64(200))
	expected.Add("Bob", 300.0, int64(201))
	expected.Add("Charlie", 302.0, int32(202))
	expected.Add("FoxTrot", 302.0, int64(203))

	expected.Add("Alice", 400.0, int64(200))
	expected.Add("Bob", 400.0, int64(201))
	expected.Add("Charlie", 402.0, int32(302))
	expected.Add("FoxTrot", 402.0, int64(203))

	expected.Add("Alice", 500.0, int64(400))
	expected.Add("Bob", 500.0, int64(401))
	expected.Add("Charlie", 502.0, int32(402))
	expected.Add("FoxTrot", 502.0, int64(403))

	expected.Add("Alice", 500.0, int64(400))
	expected.Add("Bob", 500.0, int64(401))
	expected.Add("Charlie", 602.0, int32(402))
	expected.Add("FoxTrot", 602.0, int64(503))

	// Even though alice goes missing, we don't log a missing value
	// because we already have value 400 for alice at the same
	// timestamp
	expected.Add("Alice", 500.0, int64(400))
	expected.Add("Bob", 500.0, int64(401))
	expected.Add("Charlie", 702.0, int32(402))
	expected.Add("FoxTrot", 702.0, int64(603))

	expected.Add("Alice", 800.0, int64(700))
	expected.AddInactive("Bob", 800.0, int64(0))
	expected.Add("Charlie", 802.0, int32(402))
	expected.Add("FoxTrot", 802.0, int64(703))

	expected.AddInactive("Alice", 800.001, int64(0))
	// The Iterator is simple and does not try to filter consecutive
	// missing flags.
	expected.AddInactive("Bob", 800.001, int64(0))
	expected.Add("Charlie", 902.0, int32(402))
	expected.Add("FoxTrot", 902.0, int64(803))

	expected.Add("Alice", 1000.0, int64(900))
	expected.Add("Bob", 1000.0, int64(901))
	expected.AddInactive("Charlie", 902.001, int32(0))
	expected.AddInactive("FoxTrot", 902.001, int64(0))

	expected.AddInactive("Alice", 1000.001, int64(0))
	expected.AddInactive("Bob", 1000.001, int64(0))
	expected.AddInactive("Charlie", 902.001, int32(0))
	expected.AddInactive("FoxTrot", 902.001, int64(0))

	beginning := expected.Checkpoint()

	iterator, timeAfterIterator := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	assertValueEquals(t, 0.0, timeAfterIterator)

	expected.Iterate(t, iterator)
	expected.VerifyDone(t)

	// Shouldn't get anything else off this iterator
	expected.Iterate(t, iterator)
	expected.VerifyDone(t)

	// Now iterate again but in chunks. In this test, don't commit
	// every 2nd iteration to show that in that case the next iterator
	// starts at the same place.
	expected.Restore(beginning)

	// max 2 times per metric
	iterator, timeAfterIterator = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	assertValueEquals(t, 800.001, timeAfterIterator)
	expected.Iterate(t, iterator)
	expected.Restore(beginning)
	iterator, _ = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	valueCount := expected.Iterate(t, iterator)
	iterator.Commit()
	for valueCount > 0 {
		// 8 = 4 metrics * 2 times per metric
		if valueCount > 8 {
			t.Error("Got too many values")
		}
		iterator, _ = aStore.NamedIteratorForEndpoint(
			"anIterator", kEndpoint0, 2)
		checkpoint := expected.Checkpoint()
		expected.Iterate(t, iterator)
		expected.Restore(checkpoint)
		iterator, _ = aStore.NamedIteratorForEndpoint(
			"anIterator", kEndpoint0, 2)
		valueCount = expected.Iterate(t, iterator)
		iterator.Commit()
	}
	expected.VerifyDone(t)

	iterator, timeAfterIterator = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	assertValueEquals(t, 0.0, timeAfterIterator)
	expected.Iterate(t, iterator)
	iterator.Commit()
	expected.VerifyDone(t)

	// Now iterate again but test iterating 5 at a time, committing
	// and creating a new iterator. Since we committed the previous
	// iterator, we have to use a new name to start from the beginning.
	expected.Restore(beginning)

	iterator, _ = aStore.NamedIteratorForEndpoint(
		"anotherIterator", kEndpoint0, 0)
	valueCount = expected.Iterate(t, iteratorLimit(iterator, 5))
	iterator.Commit()
	for valueCount > 0 {
		iterator, _ = aStore.NamedIteratorForEndpoint(
			"anotherIterator", kEndpoint0, 0)
		newValueCount := expected.Iterate(t, iteratorLimit(iterator, 5))
		if !(valueCount == 5 || (valueCount < 5 && newValueCount == 0)) {
			t.Error("Expected exactly 5 values for each chunk except the very last.")
		}
		valueCount = newValueCount
		iterator.Commit()
	}
	expected.VerifyDone(t)

	// Now test filtering
	filteredExpected := newExpectedTsValues()
	filteredExpected.Add("Alice", 100.0, int64(0))
	filteredExpected.Add("Alice", 200.0, int64(0))
	filteredExpected.Add("Alice", 300.0, int64(200))

	iterator, _ = aStore.NamedIteratorForEndpoint(
		"aThirdIterator", kEndpoint0, 0)
	filteredIterator := store.NamedIteratorFilterFunc(
		iterator,
		func(r *store.Record) bool {
			return r.TimeStamp < 400 && r.Info.Path() == "Alice"
		})
	filteredExpected.Iterate(t, filteredIterator)
	filteredExpected.VerifyDone(t)
	filteredIterator.Commit()

	iterator, _ = aStore.NamedIteratorForEndpoint(
		"aThirdIterator", kEndpoint0, 0)
	filteredExpected.Iterate(t, iterator)

	// Test StartAtBeginning
	toStartAtBeginningIterator0, _ := aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning0", kEndpoint0, 0)
	toStartAtBeginningIterator1, _ := aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning1", kEndpoint0, 0)
	toStartAtBeginningIterator2, _ := aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning2", kEndpoint0, 0)
	count0 := countItems(toStartAtBeginningIterator0)
	count1 := countItems(toStartAtBeginningIterator1)
	count2 := countItems(toStartAtBeginningIterator2)

	if count0 == 0 || count1 == 0 || count2 == 0 {
		t.Fatal("Expected items to iterate over")
	}

	// Commit all progress
	toStartAtBeginningIterator0.Commit()
	toStartAtBeginningIterator1.Commit()
	toStartAtBeginningIterator2.Commit()

	// Start just 0 and 1 at beginning
	aStore.StartAtBeginning(
		kEndpoint0, "toStartAtBeginning0", "toStartAtBeginning1")

	toStartAtBeginningIterator0, _ = aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning0", kEndpoint0, 0)
	toStartAtBeginningIterator1, _ = aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning1", kEndpoint0, 0)
	toStartAtBeginningIterator2, _ = aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning2", kEndpoint0, 0)
	assertValueEquals(t, count0, countItems(toStartAtBeginningIterator0))
	assertValueEquals(t, count1, countItems(toStartAtBeginningIterator1))
	assertValueEquals(t, 0, countItems(toStartAtBeginningIterator2))

	toStartAtBeginningIterator0.Commit()
	toStartAtBeginningIterator1.Commit()

	toStartAtBeginningIterator0, _ = aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning0", kEndpoint0, 0)
	toStartAtBeginningIterator1, _ = aStore.NamedIteratorForEndpoint(
		"toStartAtBeginning1", kEndpoint0, 0)

	assertValueEquals(t, 0, countItems(toStartAtBeginningIterator0))
	assertValueEquals(t, 0, countItems(toStartAtBeginningIterator1))

	// Test SetIteratorTo
	multipleCommitsIterator, _ := aStore.NamedIteratorForEndpoint(
		"multipleCommitsIterator", kEndpoint0, 0)
	totalItems := countItems(multipleCommitsIterator)
	if totalItems == 0 {
		t.Error("Oops didn't get any records")
	}

	multipleCommitsIterator, _ = aStore.NamedIteratorForEndpoint(
		"multipleCommitsIterator", kEndpoint0, 0)
	aStore.SetIteratorTo(kEndpoint0, "copy", "multipleCommitsIterator")
	var r store.Record
	for multipleCommitsIterator.Next(&r) {
		copyIterator, _ := aStore.NamedIteratorForEndpoint(
			"copy", kEndpoint0, 0)
		assertValueEquals(t, totalItems, countItems(copyIterator))
		totalItems--
		multipleCommitsIterator.Commit()
		aStore.SetIteratorTo(kEndpoint0, "copy", "multipleCommitsIterator")
	}
}

func TestMissingValue(t *testing.T) {
	aStore := newStore(t, "TestMissingValue", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	// Missing value.
	aMetric := metrics.SimpleList{
		{
			Path:        "No value",
			Description: "no value",
		},
	}
	if _, err := aStore.AddBatch(
		kEndpoint0, 1000.0, aMetric[:].Sorted()); err == nil {
		t.Error("Expected error, missing value")
	}
}

func TestDuplicateValue(t *testing.T) {
	aStore := newStore(t, "TestDuplicateValue", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	// duplicate value.
	aMetric := metrics.SimpleList{
		{
			Path:        "Duplicate",
			Description: "duplicate",
			Value:       int64(97),
		},
		{
			Path:        "Duplicate",
			Description: "duplicate",
			Value:       int64(97),
		},
	}
	if _, err := aStore.AddBatch(
		kEndpoint0, 1000.0, aMetric[:].Sorted()); err == nil {
		t.Error("Expected error, duplicate value")
	}
}

func TestBadValue(t *testing.T) {
	aStore := newStore(t, "TestBadValue", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	// bad value.
	aMetric := metrics.SimpleList{
		{
			Path:        "Bad value",
			Description: "bad value",
			Value:       92,
		},
	}
	if _, err := aStore.AddBatch(
		kEndpoint0, 1000.0, aMetric[:].Sorted()); err == nil {
		t.Error("Expected error, bad value")
	}
}

func TestMetaData(t *testing.T) {
	aStore := newStore(t, "TestMetaData", 2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "None",
			Description: "none",
		},
		{
			Path:        "Second",
			Description: "second",
			Unit:        units.Second,
			GroupId:     3,
		},
	}
	playback := newPlaybackType(aMetric[:], 1)
	playback.AddTimes(
		0,
		1000.0,
	)
	playback.Add(
		"None",
		uint8(17),
	)
	playback.AddTimes(
		3,
		1000.0,
	)
	playback.Add(
		"Second",
		float32(62.5),
	)
	playback.Play(aStore, kEndpoint0)

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"None", kEndpoint0, 0.0, 2000.0, store.AppendTo(&result))
	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, "None", result[0].Info.Path())
	assertValueEquals(t, 8, result[0].Info.Bits())
	assertValueEquals(t, "none", result[0].Info.Description())
	assertValueEquals(t, 0, result[0].Info.GroupId())
	assertValueEquals(t, types.Uint8, result[0].Info.Kind())
	assertValueEquals(t, units.Unknown, result[0].Info.Unit())

	result = nil
	aStore.ByNameAndEndpoint(
		"Second", kEndpoint0, 0.0, 2000.0, store.AppendTo(&result))
	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, "Second", result[0].Info.Path())
	assertValueEquals(t, 32, result[0].Info.Bits())
	assertValueEquals(t, "second", result[0].Info.Description())
	assertValueEquals(t, 3, result[0].Info.GroupId())
	assertValueEquals(t, types.Float32, result[0].Info.Kind())
	assertValueEquals(t, units.Second, result[0].Info.Unit())
}

func TestIndivMetricGoneInactive(t *testing.T) {
	aStore := newStore(t, "TestIndivMetricGoneInactive", 1, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
		},
		{
			Path:        "/foo/32bit",
			Description: "A description",
		},
	}
	aMetric[0].Value = int64(3)
	aMetric[1].Value = int64(8)
	aStore.AddBatch(kEndpoint0, 1000, aMetric[0:2].Sorted())
	aMetric[0].Value = int64(13)
	aMetric[1].Value = int64(18)
	aStore.AddBatch(kEndpoint0, 1010, aMetric[0:2].Sorted())
	aMetric[0].Value = int64(23)
	aMetric[1].Value = int64(28)
	aStore.AddBatch(kEndpoint0, 1020, aMetric[0:2].Sorted())

	// foo/bar metric inactive now
	aMetric[1].Value = int64(38)
	aMetric[2].Value = int32(39)
	aStore.AddBatch(kEndpoint0, 1030, aMetric[1:3].Sorted())

	// foo/32bit inactive now
	aMetric[0].Value = int64(43)
	aMetric[1].Value = int64(48)
	aStore.AddBatch(kEndpoint0, 1040, aMetric[0:2].Sorted())

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 1020.0, 1041.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 1040.0, result[0].TimeStamp)
	assertValueEquals(t, int64(43), result[0].Value)
	assertValueEquals(t, true, result[0].Active)
	assertValueEquals(t, 1030.0, result[1].TimeStamp)
	assertValueEquals(t, int64(0), result[1].Value)
	assertValueEquals(t, false, result[1].Active)
	assertValueEquals(t, 1020.0, result[2].TimeStamp)
	assertValueEquals(t, int64(23), result[2].Value)
	assertValueEquals(t, true, result[2].Active)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/32bit", kEndpoint0, 1020.0, 1041.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 1040.0, result[0].TimeStamp)
	assertValueEquals(t, int32(0), result[0].Value)
	assertValueEquals(t, false, result[0].Active)
	assertValueEquals(t, 1030.0, result[1].TimeStamp)
	assertValueEquals(t, int32(39), result[1].Value)
	assertValueEquals(t, true, result[1].Active)
}

func TestMachineGoneInactive(t *testing.T) {
	aStore := newStore(t, "TestMachineGoneInactive", 1, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
		},
	}
	aMetric[0].Value = int64(6)
	aMetric[1].Value = int64(8)
	aStore.AddBatch(kEndpoint0, 900, aMetric[:].Sorted())
	aMetric[0].Value = int64(16)
	aMetric[1].Value = int64(18)
	aStore.AddBatch(kEndpoint0, 910, aMetric[:].Sorted())

	aMetric[0].Value = int64(1)
	aMetric[1].Value = int64(2)
	aStore.AddBatch(kEndpoint1, 1900, aMetric[:].Sorted())
	aMetric[0].Value = int64(11)
	aMetric[1].Value = int64(12)
	aStore.AddBatch(kEndpoint1, 1910, aMetric[:].Sorted())
	aMetric[0].Value = int64(11)
	aMetric[1].Value = int64(22)
	aStore.AddBatch(kEndpoint1, 1920, aMetric[:].Sorted())

	// The timestamp here doesn't matter.
	// To be consistent with individual metrics going inactive we just
	// add 1ms to last known timestamp.
	aStore.MarkEndpointInactive(1915.0, kEndpoint1)

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 900.0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 910.0, result[0].TimeStamp)
	assertValueEquals(t, int64(16), result[0].Value)
	assertValueEquals(t, true, result[0].Active)
	assertValueEquals(t, 900.0, result[1].TimeStamp)
	assertValueEquals(t, int64(6), result[1].Value)
	assertValueEquals(t, true, result[1].Active)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 1900.0, 2000.0, store.AppendTo(&result))

	if assertValueEquals(t, 3, len(result)) {
		assertValueEquals(t, 1920.001, result[0].TimeStamp)
		assertValueEquals(t, int64(0), result[0].Value)
		assertValueEquals(t, false, result[0].Active)
		assertValueEquals(t, 1910.0, result[1].TimeStamp)
		assertValueEquals(t, int64(11), result[1].Value)
		assertValueEquals(t, true, result[1].Active)
		assertValueEquals(t, 1900.0, result[2].TimeStamp)
		assertValueEquals(t, int64(1), result[2].Value)
		assertValueEquals(t, true, result[2].Active)
	}

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 1900.0, 2000.0, store.AppendTo(&result))

	if assertValueEquals(t, 4, len(result)) {
		assertValueEquals(t, 1920.001, result[0].TimeStamp)
		assertValueEquals(t, int64(0), result[0].Value)
		assertValueEquals(t, false, result[0].Active)
		assertValueEquals(t, 1920.0, result[1].TimeStamp)
		assertValueEquals(t, int64(22), result[1].Value)
		assertValueEquals(t, true, result[1].Active)
		assertValueEquals(t, 1910.0, result[2].TimeStamp)
		assertValueEquals(t, int64(12), result[2].Value)
		assertValueEquals(t, true, result[2].Active)
		assertValueEquals(t, 1900.0, result[3].TimeStamp)
		assertValueEquals(t, int64(2), result[3].Value)
		assertValueEquals(t, true, result[3].Active)
	}

	expectedTsValues := newExpectedTsValues()
	expectedTsValues.Add("/foo/bar", 1900.0, int64(1))
	expectedTsValues.Add("/foo/bar", 1910.0, int64(11))
	expectedTsValues.Add("/foo/bar", 1920.0, int64(11))
	expectedTsValues.AddInactive("/foo/bar", 1920.001, int64(0))
	expectedTsValues.Add("/foo/baz", 1900.0, int64(2))
	expectedTsValues.Add("/foo/baz", 1910.0, int64(12))
	expectedTsValues.Add("/foo/baz", 1920.0, int64(22))
	expectedTsValues.AddInactive("/foo/baz", 1920.001, int64(0))

	iterator, _ := aStore.NamedIteratorForEndpoint("aname", kEndpoint1, 0)

	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)

	var noMetrics metrics.SimpleList

	if _, err := aStore.AddBatch(kEndpoint1, 2000.0, noMetrics.Sorted()); err != store.ErrInactive {
		t.Error("Expected AddBatch to fail")
	}
	aStore.MarkEndpointActive(kEndpoint1)
	if _, err := aStore.AddBatch(kEndpoint1, 2000.0, noMetrics.Sorted()); err != nil {
		t.Error("Expected AddBatch to succeed")
	}
}

func TestSomeMissingSomePresentTimeStamps(t *testing.T) {
	aStore := newStore(
		t, "TestSomeMissingSomePresentTimeStamps", 1, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "/zero/noTimeStamp",
			Description: "No time stamp",
			Value:       int64(1),
		},
		{
			Path:        "/zero/yesTimeStamp",
			Description: "Yes time stamp",
			TimeStamp:   kUsualTimeStamp,
			Value:       int64(2),
		},
		{
			Path:        "/zero/noTimeStamp2",
			Description: "No time stamp",
			Value:       int64(3),
		},
		{
			Path:        "/zero/noTimeStamp3",
			Description: "No time stamp",
			Value:       int64(4),
		},
		{
			Path:        "/zero/noTimeStamp4",
			Description: "No time stamp",
			Value:       int64(5),
		},
		{
			Path:        "/one/noTimeStamp",
			Description: "no time stamp",
			GroupId:     1,
			Value:       int64(101),
		},
		{
			Path:        "/one/noTimeStamp2",
			Description: "no time stamp",
			GroupId:     1,
			Value:       int64(102),
		},
	}

	aStore.AddBatch(kEndpoint0, 1300.0, aMetric.Sorted())
	expectedTsValues := newExpectedTsValues()
	expectedTsValues.Add(
		"/zero/noTimeStamp",
		duration.TimeToFloat(kUsualTimeStamp),
		int64(1))
	expectedTsValues.Add(
		"/zero/noTimeStamp3",
		duration.TimeToFloat(kUsualTimeStamp),
		int64(4))
	expectedTsValues.Add(
		"/zero/noTimeStamp4",
		duration.TimeToFloat(kUsualTimeStamp),
		int64(5))
	expectedTsValues.Add(
		"/zero/noTimeStamp2",
		duration.TimeToFloat(kUsualTimeStamp),
		int64(3))
	expectedTsValues.Add(
		"/zero/yesTimeStamp",
		duration.TimeToFloat(kUsualTimeStamp),
		int64(2))
	expectedTsValues.Add(
		"/one/noTimeStamp",
		1300.0,
		int64(101))
	expectedTsValues.Add(
		"/one/noTimeStamp2",
		1300.0,
		int64(102))
	iterator, _ := aStore.NamedIteratorForEndpoint("aname", kEndpoint0, 0)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)
}

func TestLMMDropOffEarlyTimestamps(t *testing.T) {
	// Four pages 2 values/timestamps each.
	// We create 1 metric and give it 5 distinct values so that 2 pages go
	// to the values and 2 pages go to the timestamps.
	// We then re-add the 5th value with a newer timestamp. This will
	// force a new timestamp page and evict the oldest value page.
	// The result will be that the timestamps go earlier than the values.
	// We want to be sure that the iterator starts at the earliest value
	// not the earliest timestamp.
	aStore := newStore(t, "TestLMMDropOffEarlyTimestamps", 2, 4, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
		},
	}
	aMetric[0].Value = int64(12)
	aStore.AddBatch(kEndpoint0, 1200.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(13)
	aStore.AddBatch(kEndpoint0, 1300.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(14)
	aStore.AddBatch(kEndpoint0, 1400.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(15)
	aStore.AddBatch(kEndpoint0, 1500.0, aMetric[:].Sorted())
	aMetric[0].Value = int64(16)
	aStore.AddBatch(kEndpoint0, 1600.0, aMetric[:].Sorted())
	// Re-add 5th value. Oldest value now 14, not 12.
	aStore.AddBatch(kEndpoint0, 1700.0, aMetric[:].Sorted())

	expectedTsValues := newExpectedTsValues()

	// Even though we request 2 values per metric, we get nothing
	// because the first 2 timestamps don't match any value.
	iterator, _ := aStore.NamedIteratorForEndpoint("aname", kEndpoint0, 2)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)

	expectedTsValues = newExpectedTsValues()
	expectedTsValues.Add("/foo/bar", 1400.0, int64(14))
	expectedTsValues.Add("/foo/bar", 1500.0, int64(15))
	expectedTsValues.Add("/foo/bar", 1600.0, int64(16))
	expectedTsValues.Add("/foo/bar", 1700.0, int64(16))

	iterator, _ = aStore.NamedIteratorForEndpoint("aname", kEndpoint0, 0)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)
}

func TestDistributionTotalsCount(t *testing.T) {
	totals := &store.DistributionTotals{
		Counts: []uint64{2, 3, 5},
	}
	assertValueEquals(t, uint64(10), totals.Count())
}

func TestWithDistributions(t *testing.T) {
	aStore := newStore(t, "TestWithDistributions", 10, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := metrics.SimpleList{
		{
			Path:    "/dist/cumulative",
			GroupId: 5,
		},
		{
			Path:    "/dist/noncumulative",
			GroupId: 6,
		},
		{
			Path:    "mint",
			GroupId: 7,
		},
	}
	// cumulative distribution has two different types of buckets
	cumulativeA := []*messages.Distribution{
		{
			Generation: 1,
			Sum:        200.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 9,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 15,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 10,
				},
				{
					Lower: 10.0,
					Count: 3,
				},
			},
		},
		{
			Generation: 3,
			Sum:        270.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 13,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 21,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 15,
				},
				{
					Lower: 10.0,
					Count: 5,
				},
			},
		},
		{
			Generation: 2,
			Sum:        6.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 2,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 3,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 0,
				},
				{
					Lower: 10.0,
					Count: 0,
				},
			},
		},
		{
			Generation: 4,
			Sum:        36.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 2,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 5,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 0,
				},
				{
					Lower: 10.0,
					Count: 1,
				},
			},
		},
		{
			Generation: 6,
			Sum:        36.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 2,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 5,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 0,
				},
				{
					Lower: 10.0,
					Count: 1,
				},
			},
		},
		{
			Generation: 1,
			Sum:        35.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 0,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 0,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 1,
				},
				{
					Lower: 10.0,
					Count: 2,
				},
			},
		},
		{
			Generation: 11,
			Sum:        38.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 2.0,
					Count: 2,
				},
				{
					Lower: 2.0,
					Upper: 5.0,
					Count: 1,
				},
				{
					Lower: 5.0,
					Upper: 10.0,
					Count: 1,
				},
				{
					Lower: 10.0,
					Count: 2,
				},
			},
		},
	}

	cumulativeB := []*messages.Distribution{
		{
			Generation: 1,
			Sum:        360.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 30.0,
					Count: 6,
				},
				{
					Lower: 30.0,
					Upper: 60.0,
					Count: 4,
				},
				{
					Lower: 60.0,
					Count: 2,
				},
			},
		},
		{
			Generation: 11,
			Sum:        2860.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 30.0,
					Count: 18,
				},
				{
					Lower: 30.0,
					Upper: 60.0,
					Count: 4,
				},
				{
					Lower: 60.0,
					Count: 22,
				},
			},
		},
		{
			Generation: 21,
			Sum:        3080.0,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 30.0,
					Count: 18,
				},
				{
					Lower: 30.0,
					Upper: 60.0,
					Count: 5,
				},
				{
					Lower: 60.0,
					Count: 25,
				},
			},
		},
	}
	nonCumulativeA := []*messages.Distribution{
		{
			Generation:      1,
			Sum:             500.0,
			IsNotCumulative: true,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 50.0,
					Count: 6,
				},
				{
					Lower: 50.0,
					Count: 4,
				},
			},
		},
		{
			Generation:      2,
			Sum:             420.0,
			IsNotCumulative: true,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 50.0,
					Count: 1,
				},
				{
					Lower: 50.0,
					Count: 5,
				},
			},
		},
		{
			Generation:      3,
			Sum:             450.0,
			IsNotCumulative: true,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 50.0,
					Count: 2,
				},
				{
					Lower: 50.0,
					Count: 6,
				},
			},
		},
	}
	nonCumulativeB := []*messages.Distribution{
		{
			Generation:      1,
			Sum:             1200.0,
			IsNotCumulative: true,
			Ranges: []*messages.RangeWithCount{
				{
					Upper: 70.0,
					Count: 9,
				},
				{
					Lower: 70.0,
					Count: 8,
				},
			},
		},
	}

	playback := newPlaybackType(aMetric, 10)
	playback.AddTimes(
		5,
		2750.0, 2850.0,
		2950.0, 3050.0, 3150.0,
		3250.0, 3350.0, 3450.0,
		3550.0, 3650.0)
	playback.AddTimes(
		6,
		2750.0, 2850.0,
		2950.0, 3050.0, 3150.0,
		3250.0, 3350.0, 3450.0,
		3550.0, 3650.0)
	playback.AddTimes(
		7,
		2750.0, 2850.0,
		2950.0, 3050.0, 3150.0,
		3250.0, 3350.0, 3450.0,
		3550.0, 3650.0)
	playback.Add(
		"/dist/cumulative",
		cumulativeA[0], cumulativeA[1],
		cumulativeA[2], cumulativeA[3], cumulativeA[4],
		cumulativeB[0], cumulativeB[1], cumulativeB[2],
		cumulativeA[5], cumulativeA[6],
	)
	playback.Add(
		"/dist/noncumulative",
		nonCumulativeA[0], nonCumulativeA[1],
		nonCumulativeA[2], nonCumulativeA[2], nonCumulativeA[2],
		nonCumulativeA[2], nonCumulativeA[2], nonCumulativeA[2],
		nonCumulativeA[2], nonCumulativeB[0],
	)
	playback.Add(
		"mint",
		int64(50), int64(50),
		int64(70), int64(90), int64(110),
		int64(130), int64(130), int64(130),
		int64(130), int64(130),
	)

	playback.Play(aStore, kEndpoint0)

	expectedMetaData := newExpectedMetaData()
	expectedMetaData.AddBits("/dist/cumulative", 0)
	expectedMetaData.AddGroupId("/dist/cumulative", 5)
	expectedMetaData.AddKind("/dist/cumulative", types.Dist)
	expectedMetaData.AddSubType("/dist/cumulative", types.Unknown)
	expectedMetaData.AddIsNotCumulative("/dist/cumulative", false)
	expectedMetaData.AddBits("/dist/noncumulative", 0)
	expectedMetaData.AddGroupId("/dist/noncumulative", 6)
	expectedMetaData.AddKind("/dist/noncumulative", types.Dist)
	expectedMetaData.AddSubType("/dist/noncumulative", types.Unknown)
	expectedMetaData.AddIsNotCumulative("/dist/noncumulative", true)

	// Use GroupMetricByPathAndNumeric strategy so that /dist/cumulative
	// is considered one logical metric even though the buckets change.
	expectedTsValues := newExpectedTsValuesWithMetaDataAndStrategy(
		expectedMetaData, store.GroupMetricByPathAndNumeric)
	expectedTsValues.Add(
		"/dist/cumulative",
		2750.0,
		&store.DistributionTotals{
			Counts:        []uint64{9, 15, 10, 3},
			Sum:           200.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		2850.0,
		&store.DistributionTotals{
			Counts:        []uint64{13, 21, 15, 5},
			Sum:           270.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		2950.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 3, 0, 0},
			Sum:           6.0,
			RollOverCount: 2,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3050.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 5, 0, 1},
			Sum:           36.0,
			RollOverCount: 2,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3250.0,
		&store.DistributionTotals{
			Counts:        []uint64{6, 4, 2},
			Sum:           360.0,
			RollOverCount: 3,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3350.0,
		&store.DistributionTotals{
			Counts:        []uint64{18, 4, 22},
			Sum:           2860.0,
			RollOverCount: 3,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3450.0,
		&store.DistributionTotals{
			Counts:        []uint64{18, 5, 25},
			Sum:           3080.0,
			RollOverCount: 3,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3550.0,
		&store.DistributionTotals{
			Counts:        []uint64{0, 0, 1, 2},
			Sum:           35.0,
			RollOverCount: 4,
		})
	expectedTsValues.Add(
		"/dist/cumulative",
		3650.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 1, 1, 2},
			Sum:           38.0,
			RollOverCount: 4,
		})

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByNameAndEndpointStrategy(
				"/dist/cumulative",
				kEndpoint0,
				0,
				10000.0,
				store.GroupMetricByPathAndNumeric,
				a)
		},
	)

	var result []store.Record
	appender := store.AppendTo(&result)
	aStore.ByNameAndEndpointStrategy(
		"/dist/cumulative",
		kEndpoint0,
		0,
		10000.0,
		store.GroupMetricByPathAndNumeric,
		appender)
	expectedTsValues.CheckSlice(t, result)

	// We have to chack on the ranges, so extract record at with ts=3450
	// and ts=3650
	var ts3450, ts3650 store.Record
	for i := range result {
		if result[i].TimeStamp == 3450.0 {
			ts3450 = result[i]
		} else if result[i].TimeStamp == 3650.0 {
			ts3650 = result[i]
		}
	}
	assertValueDeepEquals(
		t,
		[]float64{2.0, 5.0, 10.0},
		ts3650.Info.Ranges().UpperLimits)
	assertValueDeepEquals(
		t,
		[]float64{30.0, 60.0},
		ts3450.Info.Ranges().UpperLimits)

	expectedTsValues.Add(
		"/dist/noncumulative",
		2750.0,
		&store.DistributionTotals{
			Counts:        []uint64{6, 4},
			Sum:           500.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		2850.0,
		&store.DistributionTotals{
			Counts:        []uint64{1, 5},
			Sum:           420.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		2950.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3650.0,
		&store.DistributionTotals{
			Counts:        []uint64{9, 8},
			Sum:           1200.0,
			RollOverCount: 2,
		})

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByPrefixAndEndpointStrategy(
				"/dist",
				kEndpoint0,
				0,
				10000.0,
				store.GroupMetricByPathAndNumeric,
				a)
		},
	)

	result = nil
	appender = store.AppendTo(&result)
	aStore.ByPrefixAndEndpointStrategy(
		"/dist",
		kEndpoint0,
		0,
		10000.0,
		store.GroupMetricByPathAndNumeric,
		appender)
	expectedTsValues.CheckSlice(t, result)

	expectedTsValues.Add(
		"mint",
		2750.0,
		int64(50))
	expectedTsValues.Add(
		"mint",
		2950.0,
		int64(70))
	expectedTsValues.Add(
		"mint",
		3050.0,
		int64(90))
	expectedTsValues.Add(
		"mint",
		3150.0,
		int64(110))
	expectedTsValues.Add(
		"mint",
		3250.0,
		int64(130))

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByEndpointStrategy(
				kEndpoint0,
				0,
				10000.0,
				store.GroupMetricByPathAndNumeric,
				a)
		},
	)

	result = nil
	appender = store.AppendTo(&result)
	aStore.ByEndpointStrategy(
		kEndpoint0,
		0,
		10000.0,
		store.GroupMetricByPathAndNumeric,
		appender)
	expectedTsValues.CheckSlice(t, result)

	// Now test iteration. Since iteration shows all data points even ones
	// that don't change we have to add in all those redundant data points

	expectedTsValues.Add(
		"/dist/cumulative",
		3150.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 5, 0, 1},
			Sum:           36.0,
			RollOverCount: 2,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3050.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3150.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3250.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3350.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3450.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3550.0,
		&store.DistributionTotals{
			Counts:        []uint64{2, 6},
			Sum:           450.0,
			RollOverCount: 1,
		})

	expectedTsValues.Add(
		"mint",
		2850.0,
		int64(50))
	expectedTsValues.Add(
		"mint",
		3350.0,
		int64(130))
	expectedTsValues.Add(
		"mint",
		3450.0,
		int64(130))
	expectedTsValues.Add(
		"mint",
		3550.0,
		int64(130))
	expectedTsValues.Add(
		"mint",
		3650.0,
		int64(130))

	// Test iterating. Iterators should include
	// distribution values
	iterator, _ := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)

	// Remember that we don't know if the time window starting at 3500
	// is complete, so we don't emit rolled up metrics for it.
	expectedTsValues = newExpectedTsValues()
	expectedTsValues.Add(
		"/dist/cumulative",
		3000.0,
		&store.DistributionTotals{
			Counts:        []uint64{9, 15, 10, 3},
			Sum:           200.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"/dist/noncumulative",
		3000.0,
		&store.DistributionTotals{
			Counts:        []uint64{6, 4},
			Sum:           500.0,
			RollOverCount: 1,
		})
	expectedTsValues.Add(
		"mint",
		3000.0,
		int64(74))

	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"anIterator",
		kEndpoint0,
		500*time.Second,
		0,
		store.GroupMetricByPathAndNumeric)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)
}

func TestWithLists(t *testing.T) {
	aStore := newStore(t, "TestWithLists", 10, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)

	aMetric := metrics.SimpleList{
		{
			Path:    "/list/uint32",
			GroupId: 5,
		},
		{
			Path:    "/list/string",
			GroupId: 7,
		},
	}
	expectedMetaData := newExpectedMetaData()
	expectedMetaData.AddBits("/list/uint32", 32)
	expectedMetaData.AddGroupId("/list/uint32", 5)
	expectedMetaData.AddKind("/list/uint32", types.List)
	expectedMetaData.AddSubType("/list/uint32", types.Uint32)
	expectedMetaData.AddBits("/list/string", 0)
	expectedMetaData.AddGroupId("/list/string", 7)
	expectedMetaData.AddKind("/list/string", types.List)
	expectedMetaData.AddSubType("/list/string", types.String)

	playback := newPlaybackType(aMetric[:], 8)
	playback.AddTimes(
		5,
		1650.0, 1700.0, 1750.0, 1800.0, 1850.0,
		1900.0, 1950.0, 2000.0)
	var nilUint32List []uint32
	playback.Add(
		"/list/uint32",
		nilUint32List,
		[]uint32{2, 3, 5, 7},
		[]uint32{2, 3, 5, 7},
		nilUint32List,
		nilUint32List,
		nil,
		nil,
		nil,
	)
	playback.AddTimes(
		7,
		2650.0, 2700.0, 2750.0, 2800.0, 2850.0,
		2900.0, 2950.0, 3000.0)
	playback.Add(
		"/list/string",
		[]string{"hello", "goodbye"},
		[]string{"foo", "bar", "baz"},
		[]string{"foo", "bar", "baz"},
		[]string{"foo", "bar", "baz"},
		nil,
		nil,
		nil,
		[]string{"hello", "goodbye"},
	)
	var nilStringList []string
	playback.Play(aStore, kEndpoint0)
	expectedTsValues := newExpectedTsValuesWithMetaData(expectedMetaData)
	expectedTsValues.Add("/list/uint32", 1650.0, nilUint32List)
	expectedTsValues.Add("/list/uint32", 1700.0, []uint32{2, 3, 5, 7})
	expectedTsValues.Add("/list/uint32", 1800.0, nilUint32List)
	// no time reported for group
	expectedTsValues.AddInactive(
		"/list/uint32", 1850.001, nilUint32List)

	expectedTsValues.Add(
		"/list/string", 2650.0, []string{"hello", "goodbye"})
	expectedTsValues.Add(
		"/list/string", 2700.0, []string{"foo", "bar", "baz"})
	// no time reported for group
	expectedTsValues.AddInactive(
		"/list/string", 2800.001, nilStringList)
	expectedTsValues.Add(
		"/list/string", 3000.0, []string{"hello", "goodbye"})

	var result []store.Record
	aStore.ByEndpoint(
		kEndpoint0, 0, 10000.0, store.AppendTo(&result))
	expectedTsValues.CheckSlice(t, result)

	// Test iterator
	expectedTsValues = newExpectedTsValuesWithMetaData(expectedMetaData)

	expectedTsValues.Add("/list/uint32", 1650.0, nilUint32List)
	expectedTsValues.Add("/list/uint32", 1700.0, []uint32{2, 3, 5, 7})
	expectedTsValues.Add("/list/uint32", 1750.0, []uint32{2, 3, 5, 7})
	expectedTsValues.Add("/list/uint32", 1800.0, nilUint32List)
	expectedTsValues.Add("/list/uint32", 1850.0, nilUint32List)
	expectedTsValues.AddInactive("/list/uint32", 1850.001, nilUint32List)

	expectedTsValues.Add(
		"/list/string", 2650.0, []string{"hello", "goodbye"})
	expectedTsValues.Add(
		"/list/string", 2700.0, []string{"foo", "bar", "baz"})
	expectedTsValues.Add(
		"/list/string", 2750.0, []string{"foo", "bar", "baz"})
	expectedTsValues.Add(
		"/list/string", 2800.0, []string{"foo", "bar", "baz"})
	expectedTsValues.AddInactive(
		"/list/string", 2800.001, nilStringList)
	expectedTsValues.Add(
		"/list/string", 3000.0, []string{"hello", "goodbye"})

	iterator, _ := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)

	// Test rolled up iterator
	expectedTsValues = newExpectedTsValuesWithMetaData(expectedMetaData)

	// Remember, iterator never emits rolled up value from last time
	// period as more values could come in.
	expectedTsValues.Add("/list/uint32", 1700.0, nilUint32List)
	expectedTsValues.Add("/list/uint32", 1800.0, []uint32{2, 3, 5, 7})

	expectedTsValues.Add(
		"/list/string", 2700.0, []string{"hello", "goodbye"})
	expectedTsValues.Add(
		"/list/string", 2800.0, []string{"foo", "bar", "baz"})

	iterator, _ = aStore.NamedIteratorForEndpointRollUp(
		"aRollUpIterator",
		kEndpoint0,
		100*time.Second,
		0,
		store.GroupMetricByPathAndNumeric)
	expectedTsValues.Iterate(t, iterator)
	expectedTsValues.VerifyDone(t)
}

func TestByNameAndEndpointStrategy(t *testing.T) {
	astore := newStore(
		t, "TestByNameAndEndpointMergeGroups", 10, 100, 1.0, 10)
	astore.RegisterEndpoint(kEndpoint0)
	firstMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
			GroupId:     5,
		},
	}
	secondMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
			GroupId:     7,
		},
	}

	firstMetric[0].Value = int64(0)
	firstMetric[0].TimeStamp = duration.FloatToTime(1000.0)
	astore.AddBatch(kEndpoint0, 100.0, firstMetric.Sorted())
	firstMetric[0].Value = int64(10)
	firstMetric[0].TimeStamp = duration.FloatToTime(1010.0)
	astore.AddBatch(kEndpoint0, 100.0, firstMetric.Sorted())

	secondMetric[0].Value = int64(20)
	secondMetric[0].TimeStamp = duration.FloatToTime(1020.0)
	astore.AddBatch(kEndpoint0, 100.0, secondMetric.Sorted())
	secondMetric[0].Value = int64(30)
	secondMetric[0].TimeStamp = duration.FloatToTime(1030.0)
	astore.AddBatch(kEndpoint0, 100.0, secondMetric.Sorted())

	firstMetric[0].Value = int64(40)
	firstMetric[0].TimeStamp = duration.FloatToTime(1040.0)
	astore.AddBatch(kEndpoint0, 100.0, firstMetric.Sorted())
	firstMetric[0].Value = int64(50)
	firstMetric[0].TimeStamp = duration.FloatToTime(1050.0)
	astore.AddBatch(kEndpoint0, 100.0, firstMetric.Sorted())

	expected := newExpectedTsValuesWithMetaDataAndStrategy(
		kNoMetaData, store.GroupMetricByKey)
	expected.Add("/foo/bar", 1000.0, int64(0))
	expected.Add("/foo/bar", 1010.0, int64(10))
	expected.AddInactive("/foo/bar", 1010.001, int64(0))
	expected.Add("/foo/bar", 1020.0, int64(20))
	expected.Add("/foo/bar", 1030.0, int64(30))
	expected.AddInactive("/foo/bar", 1030.001, int64(0))
	expected.Add("/foo/bar", 1040.0, int64(40))
	expected.Add("/foo/bar", 1050.0, int64(50))

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			astore.ByNameAndEndpointStrategy(
				"/foo/bar",
				kEndpoint0,
				0.0,
				10000.0,
				store.GroupMetricByKey,
				a)
		},
	)

	var result []store.Record
	astore.ByNameAndEndpointStrategy(
		"/foo/bar",
		kEndpoint0,
		0.0,
		10000.0,
		store.GroupMetricByKey,
		store.AppendTo(&result))
	expected.CheckSlice(t, result)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			astore.ByEndpointStrategy(
				kEndpoint0,
				0.0,
				10000.0,
				store.GroupMetricByKey,
				a)
		},
	)

	result = nil
	astore.ByEndpointStrategy(
		kEndpoint0,
		0.0,
		10000.0,
		store.GroupMetricByKey,
		store.AppendTo(&result))
	expected.CheckSlice(t, result)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			astore.ByPrefixAndEndpointStrategy(
				"/foo",
				kEndpoint0,
				0.0,
				10000.0,
				store.GroupMetricByKey,
				a)
		},
	)

	result = nil
	astore.ByPrefixAndEndpointStrategy(
		"/foo",
		kEndpoint0,
		0.0,
		10000.0,
		store.GroupMetricByKey,
		store.AppendTo(&result))
	expected.CheckSlice(t, result)
}

func TestByNameAndEndpointAndEndpoint(t *testing.T) {
	aStore := newStore(
		t, "TestByNameAndEndpointAndEndpoint", 2, 10, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)

	var result []store.Record

	result = nil
	aStore.ByEndpoint(kEndpoint1, 0.0, 100.0, store.AppendTo(&result))
	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 0.0, 100.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.LatestByEndpoint(kEndpoint1, store.AppendTo(&result))
	assertValueEquals(t, 0, len(result))

	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/bar",
			Description: "A description",
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
		},
	}

	// On each of two endpoints:
	// 2 time series, 8 unique values, 5 unique timestamps
	// paged values per endpoint = 8 - 2 = 6
	// paged timestamps per endpoint = 5 - 1 = 4
	// total: 6*2 = 12 paged values and 4*2 = 8 paged timestamps
	// Need 12 / 2 = 6 pages for values and 8 / 2 = 4 pages for timestamps
	aMetric[0].Value = int64(0)
	aMetric[1].Value = int64(1)
	addBatch(t, aStore, kEndpoint0, 100.0, aMetric[:2].Sorted(), 2)
	aMetric[1].Value = int64(11)
	addBatch(t, aStore, kEndpoint0, 110.0, aMetric[:2].Sorted(), 1)
	aMetric[0].Value = int64(20)
	aMetric[1].Value = int64(21)
	addBatch(t, aStore, kEndpoint0, 120.0, aMetric[:2].Sorted(), 2)
	aMetric[1].Value = int64(31)
	addBatch(t, aStore, kEndpoint0, 130.0, aMetric[:2].Sorted(), 1)
	aMetric[0].Value = int64(40)
	aMetric[1].Value = int64(41)
	addBatch(t, aStore, kEndpoint0, 140.0, aMetric[:2].Sorted(), 2)

	aMetric[0].Value = int64(5)
	aMetric[1].Value = int64(6)
	addBatch(t, aStore, kEndpoint1, 105.0, aMetric[:2].Sorted(), 2)
	aMetric[1].Value = int64(16)
	addBatch(t, aStore, kEndpoint1, 115.0, aMetric[:2].Sorted(), 1)
	aMetric[0].Value = int64(25)
	aMetric[1].Value = int64(26)
	addBatch(t, aStore, kEndpoint1, 125.0, aMetric[:2].Sorted(), 2)
	aMetric[1].Value = int64(36)
	addBatch(t, aStore, kEndpoint1, 135.0, aMetric[:2].Sorted(), 1)
	aMetric[0].Value = int64(45)
	aMetric[1].Value = int64(46)
	addBatch(t, aStore, kEndpoint1, 145.0, aMetric[:2].Sorted(), 2)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 140.0, 140.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 140.0, 141.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, kEndpoint0, result[0].EndpointId)
	assertValueEquals(t, "/foo/bar", result[0].Info.Path())
	assertValueEquals(t, 140.0, result[0].TimeStamp)
	assertValueEquals(t, int64(40), result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 134.0, 130.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 130.0, 131.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 120.0, result[0].TimeStamp)
	assertValueEquals(t, int64(20), result[0].Value)

	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/bar", kEndpoint0, 120.0, 121.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 120.0, result[0].TimeStamp)
	assertValueEquals(t, int64(20), result[0].Value)

	// Now we should get 1 from foo/bar and one from foo/baz.
	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/ba", kEndpoint0, 130.0, 131.0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, kEndpoint0, result[0].EndpointId)
	assertValueEquals(t, kEndpoint0, result[1].EndpointId)
	if !reflect.DeepEqual(
		map[string]bool{"/foo/bar": true, "/foo/baz": true},
		map[string]bool{
			result[0].Info.Path(): true,
			result[1].Info.Path(): true}) {
		t.Error(
			"Expected /foo/bar and /foo/baz, got %s and %s",
			result[0].Info.Path(),
			result[1].Info.Path())
	}

	// Now we should get nothing
	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/bat", kEndpoint0, 130.0, 131.0, store.AppendTo(&result))
	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 90.0, 100.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/notthere",
		kEndpoint0, 100.0, 130.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 0.0, 140.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 120.0, result[0].TimeStamp)
	assertValueEquals(t, int64(20), result[0].Value)
	assertValueEquals(t, 100.0, result[1].TimeStamp)
	assertValueEquals(t, int64(0), result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 5, len(result))
	assertValueEquals(t, 140.0, result[0].TimeStamp)
	assertValueEquals(t, int64(41), result[0].Value)
	assertValueEquals(t, 130.0, result[1].TimeStamp)
	assertValueEquals(t, int64(31), result[1].Value)
	assertValueEquals(t, 120.0, result[2].TimeStamp)
	assertValueEquals(t, int64(21), result[2].Value)
	assertValueEquals(t, 110.0, result[3].TimeStamp)
	assertValueEquals(t, int64(11), result[3].Value)
	assertValueEquals(t, 100.0, result[4].TimeStamp)
	assertValueEquals(t, int64(1), result[4].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, int64(45), result[0].Value)
	assertValueEquals(t, 125.0, result[1].TimeStamp)
	assertValueEquals(t, int64(25), result[1].Value)
	assertValueEquals(t, 105.0, result[2].TimeStamp)
	assertValueEquals(t, int64(5), result[2].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 5, len(result))
	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, int64(46), result[0].Value)
	assertValueEquals(t, 135.0, result[1].TimeStamp)
	assertValueEquals(t, int64(36), result[1].Value)
	assertValueEquals(t, 125.0, result[2].TimeStamp)
	assertValueEquals(t, int64(26), result[2].Value)
	assertValueEquals(t, 115.0, result[3].TimeStamp)
	assertValueEquals(t, int64(16), result[3].Value)
	assertValueEquals(t, 105.0, result[4].TimeStamp)
	assertValueEquals(t, int64(6), result[4].Value)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByNameAndEndpoint(
				"/foo/baz", kEndpoint1, 0, 1000.0, a)
		},
	)

	var valueCount int
	aStore.ByPrefixAndEndpoint(
		"/foo/b",
		kEndpoint1,
		0, 1000.0, (*countAppenderType)(&valueCount))
	assertValueEquals(t, 8, valueCount)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByPrefixAndEndpoint(
				"/foo/b", kEndpoint1, 0, 1000.0, a)
		},
	)

	result = nil
	aStore.ByEndpoint(kEndpoint1, 0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 8, len(result))
	var barIdx, bazIdx int
	if result[0].Info.Path() == "/foo/baz" {
		barIdx, bazIdx = 5, 0
	} else {
		barIdx, bazIdx = 0, 3
	}
	assertValueEquals(t, 145.0, result[bazIdx+0].TimeStamp)
	assertValueEquals(t, int64(46), result[bazIdx+0].Value)
	assertValueEquals(t, 135.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, int64(36), result[bazIdx+1].Value)
	assertValueEquals(t, 125.0, result[bazIdx+2].TimeStamp)
	assertValueEquals(t, int64(26), result[bazIdx+2].Value)
	assertValueEquals(t, 115.0, result[bazIdx+3].TimeStamp)
	assertValueEquals(t, int64(16), result[bazIdx+3].Value)
	assertValueEquals(t, 105.0, result[bazIdx+4].TimeStamp)
	assertValueEquals(t, int64(6), result[bazIdx+4].Value)

	assertValueEquals(t, 145.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, int64(45), result[barIdx+0].Value)
	assertValueEquals(t, 125.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, int64(25), result[barIdx+1].Value)
	assertValueEquals(t, 105.0, result[barIdx+2].TimeStamp)
	assertValueEquals(t, int64(5), result[barIdx+2].Value)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByEndpoint(kEndpoint1, 0, 1000.0, a)
		},
	)

	result = nil
	aStore.ByEndpoint(kEndpoint1, 115.0, 135.0, store.AppendTo(&result))
	assertValueEquals(t, 4, len(result))
	if result[0].Info.Path() == "/foo/baz" {
		barIdx, bazIdx = 2, 0
	} else {
		barIdx, bazIdx = 0, 2
	}
	assertValueEquals(t, 125.0, result[bazIdx+0].TimeStamp)
	assertValueEquals(t, int64(26), result[bazIdx+0].Value)
	assertValueEquals(t, 115.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, int64(16), result[bazIdx+1].Value)

	assertValueEquals(t, 125.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, int64(25), result[barIdx+0].Value)
	assertValueEquals(t, 105.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, int64(5), result[barIdx+1].Value)

	// Now add 2 more values and one inactive marker. All the value
	// pages as well as the timestamp
	// pages are full at this point.
	//
	// Verify that the number of values in store does not change.

	result = nil
	aStore.ByEndpoint(
		kEndpoint1, 0.0, 1000.0, store.AppendTo(&result))

	var result1 []store.Record
	aStore.ByEndpoint(
		kEndpoint0, 0.0, 1000.0, store.AppendTo(&result1))

	theSize := len(result) + len(result1)

	aMetric[0].Value = int64(55)
	aMetric[2].Value = int32(57)
	tempMetrics := metrics.SimpleList{aMetric[0], aMetric[2]}
	// Includes inactive marker for 64 bit /foo/baz
	addBatch(t, aStore, kEndpoint1, 155.0, tempMetrics[:].Sorted(), 3)

	result = nil
	aStore.ByEndpoint(
		kEndpoint1, 0.0, 1000.0, store.AppendTo(&result))

	result1 = nil
	aStore.ByEndpoint(
		kEndpoint0, 0.0, 1000.0, store.AppendTo(&result1))

	actual := len(result) + len(result1)
	if actual > theSize+1 || actual < theSize-1 {
		t.Errorf("Expected %d, got %d", theSize, actual)
	}

	// Now test get latest metrics.
	result = nil
	aStore.LatestByEndpoint(kEndpoint0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
	if result[0].Info.Path() == "/foo/bar" {
		barIdx, bazIdx = 0, 1
	} else {
		barIdx, bazIdx = 1, 0
	}
	assertValueEquals(t, 140.0, result[barIdx].TimeStamp)
	assertValueEquals(t, int64(40), result[barIdx].Value)
	assertValueEquals(t, 140.0, result[bazIdx].TimeStamp)
	assertValueEquals(t, int64(41), result[bazIdx].Value)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.LatestByEndpoint(kEndpoint0, a)
		},
	)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 145.0, 160.0, store.AppendTo(&result))

	// Results grouped by metric first then by timestamp
	assertValueEquals(t, 3, len(result))
	var bits32, bits64 int
	if result[0].Info.Bits() == 32 {
		bits32, bits64 = 0, 1
	} else {
		bits32, bits64 = 2, 0
	}
	assertValueEquals(t, 155.0, result[bits32].TimeStamp)
	assertValueEquals(t, int32(57), result[bits32].Value)
	assertValueEquals(t, 155.0, result[bits64+0].TimeStamp)
	// inactive
	assertValueEquals(t, int64(0), result[bits64+0].Value)
	assertValueEquals(t, 145.0, result[bits64+1].TimeStamp)
	assertValueEquals(t, int64(46), result[bits64+1].Value)

	runAppenderClientTest(
		t,
		func(a store.Appender) {
			aStore.ByNameAndEndpoint(
				"/foo/baz",
				kEndpoint1,
				145.0,
				160.0,
				a)
		},
	)
}

func TestHighPriorityEviction(t *testing.T) {
	// Holds 9 values
	aStore := newStore(t, "TestHighPriorithyEviction", 1, 6, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)

	// 2 values to /foo, /bar, /baz
	// 3rd value to /foo, /bar, /baz gets inactive marker
	// 4th value to /foo and /bar only on endpoint 0
	addDataForHighPriorityEvictionTest(aStore)

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/baz",
		kEndpoint0,
		0.0,
		math.Inf(0),
		store.AppendTo(&result))
	// Since the high priority threshold is 100%, we don't expect all the
	// pages of /baz to get reclaimed.
	if len(result) <= 1 {
		t.Error("Expected /baz to have at least 2 values")
	}

	// Holds 9 values
	aStore = newStore(t, "TestHighPriorityEviction2", 1, 6, 0.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)

	// 3 values to /foo, /bar, /baz
	// 4th value to /foo and /bar only on endpoint 0
	addDataForHighPriorityEvictionTest(aStore)

	result = nil
	aStore.ByNameAndEndpoint(
		"/baz",
		kEndpoint0,
		0.0,
		math.Inf(0),
		store.AppendTo(&result))

	// Since the high priority threshold is only 0%, we expect the
	// two pages in /baz to get reclaimed for the 4th value in
	// /foo and /bar
	if len(result) > 1 {
		t.Error("Expected /baz to have only a single value")
	}
}

func addBatch(
	t *testing.T,
	astore *store.Store,
	endpointId *scotty.Endpoint,
	ts float64,
	m metrics.List,
	count uint) {
	out, err := astore.AddBatch(endpointId, ts, m)
	if err != nil {
		t.Errorf("Expected AddBatch to succeed")
	}
	if out != count {
		t.Errorf("Expected %d added, got %d.", count, out)
	}
}

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}

func assertValueDeepEquals(
	t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}

// 3 values to /foo, /bar, /baz
// 4th value to /foo and /bar only on endpoint 0
func addDataForHighPriorityEvictionTest(s *store.Store) {
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo",
			Description: "A description",
		},
		{
			Path:        "/bar",
			Description: "A description",
		},
		{
			Path:        "/baz",
			Description: "A description",
		},
	}
	aMetric[0].Value = int64(2)
	aMetric[1].Value = int64(4)
	aMetric[2].Value = int64(5)
	s.AddBatch(kEndpoint0, 100, aMetric[:].Sorted())
	aMetric[0].Value = int64(12)
	aMetric[1].Value = int64(14)
	aMetric[2].Value = int64(15)
	s.AddBatch(kEndpoint0, 110, aMetric[:].Sorted())
	aMetric[0].Value = int64(22)
	aMetric[1].Value = int64(24)
	s.AddBatch(kEndpoint0, 120, aMetric[:2].Sorted())
	aMetric[0].Value = int64(32)
	aMetric[1].Value = int64(34)
	s.AddBatch(kEndpoint0, 130, aMetric[:2].Sorted())
}
