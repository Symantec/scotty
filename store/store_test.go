package store_test

import (
	"errors"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"math"
	"reflect"
	"testing"
	"time"
)

var (
	kEndpoint0 = scotty.NewEndpoint("host1", 1001)
	kEndpoint1 = scotty.NewEndpoint("host2", 1002)
	kEndpoint2 = scotty.NewEndpoint("host3", 1001)
	kEndpoint3 = scotty.NewEndpoint("host4", 1002)
	kError     = errors.New("An error")
)

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

type sumMetricsType int

func (s *sumMetricsType) Append(r *store.Record) bool {
	*s += sumMetricsType(r.Value.(int))
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

func TestVisitorError(t *testing.T) {
	aStore := store.NewStore(1, 8, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)
	var ev errVisitor
	assertValueEquals(t, kError, aStore.VisitAllEndpoints(&ev))
}

func TestAggregateAppenderAndVisitor(t *testing.T) {
	aStore := store.NewStore(1, 8, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)

	aMetric := [1]*messages.Metric{
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}

	aMetric[0].Value = 1
	aStore.AddBatch(kEndpoint0, 100.0, aMetric[:])
	aMetric[0].Value = 2
	aStore.AddBatch(kEndpoint0, 107.0, aMetric[:])
	aMetric[0].Value = 3
	aStore.AddBatch(kEndpoint0, 114.0, aMetric[:])
	aMetric[0].Value = 4
	aStore.AddBatch(kEndpoint0, 121.0, aMetric[:])

	aMetric[0].Value = 11
	aStore.AddBatch(kEndpoint1, 100.0, aMetric[:])
	aMetric[0].Value = 12
	aStore.AddBatch(kEndpoint1, 107.0, aMetric[:])
	aMetric[0].Value = 13
	aStore.AddBatch(kEndpoint1, 114.0, aMetric[:])
	aMetric[0].Value = 14
	aStore.AddBatch(kEndpoint1, 121.0, aMetric[:])

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
}

type nameAndTsType struct {
	Name string
	Ts   float64
}

type expectedTsValuesType struct {
	lastTsMap map[string]float64
	values    map[nameAndTsType]interface{}
}

func newExpectedTsValues() *expectedTsValuesType {
	return &expectedTsValuesType{
		lastTsMap: make(map[string]float64),
		values:    make(map[nameAndTsType]interface{}),
	}
}

func (e *expectedTsValuesType) copyTo(dest *expectedTsValuesType) {
	dest.lastTsMap = make(map[string]float64, len(e.lastTsMap))
	dest.values = make(map[nameAndTsType]interface{}, len(e.values))
	for k, v := range e.lastTsMap {
		dest.lastTsMap[k] = v
	}
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

func (e *expectedTsValuesType) Add(name string, ts float64, value interface{}) {
	e.values[nameAndTsType{name, ts}] = value
}

func (e *expectedTsValuesType) Iterate(
	t *testing.T, iterator store.Iterator) (count int) {
	var r store.Record
	for iterator.Next(&r) {
		count++
		name := r.Info.Path()
		ts := r.TimeStamp
		nameTs := nameAndTsType{name, ts}
		value := r.Value
		lastTs := e.lastTsMap[name]
		e.lastTsMap[name] = ts
		if ts <= lastTs {
			t.Errorf(
				"(%s %f) does not come after %f",
				name,
				ts,
				lastTs)
		}
		expectedVal, ok := e.values[nameTs]
		if !ok {
			t.Errorf("(%s, %f) not expected", name, ts)
		} else {
			if value != expectedVal {
				t.Errorf(
					"Expected %v for (%s, %f) got %v",
					expectedVal,
					name,
					ts,
					value)
			}
			delete(e.values, nameTs)
		}
	}
	return
}

func (e *expectedTsValuesType) VerifyDone(t *testing.T) {
	for nameTs, val := range e.values {
		t.Errorf("Expected (%s, %f) = %v", nameTs.Name, nameTs.Ts, val)
	}
}

func floatToTime(f float64) time.Time {
	return messages.FloatToTime(f)
}

// TODO: Test iterator with inactive values. This is low priority as right now
// we don't write the inactive values to LMM. We filter them out.
func TestIterator(t *testing.T) {
	aStore := store.NewStore(2, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := [4]*messages.Metric{
		{
			Path:        "Alice",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
			GroupId:     0,
		},
		{
			Path:        "Bob",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
			GroupId:     0,
		},
		{
			Path:        "Charlie",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int32,
			Bits:        32,
			GroupId:     2,
		},
		{
			Path:        "FoxTrot",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
			GroupId:     2,
		},
	}
	aMetric[0].Value = 0
	aMetric[1].Value = 1
	aMetric[2].Value = 2
	aMetric[3].Value = 3
	aMetric[0].TimeStamp = floatToTime(100.0)
	aMetric[1].TimeStamp = floatToTime(100.0)
	aMetric[2].TimeStamp = floatToTime(102.0)
	aMetric[3].TimeStamp = floatToTime(102.0)
	aStore.AddBatch(kEndpoint0, 1000, aMetric[:])

	aMetric[1].Value = 101
	aMetric[0].TimeStamp = floatToTime(200.0)
	aMetric[1].TimeStamp = floatToTime(200.0)
	aMetric[2].TimeStamp = floatToTime(202.0)
	aMetric[3].TimeStamp = floatToTime(202.0)
	aStore.AddBatch(kEndpoint0, 1010, aMetric[:])

	aMetric[0].Value = 200
	aMetric[1].Value = 201
	aMetric[2].Value = 202
	aMetric[3].Value = 203
	aMetric[0].TimeStamp = floatToTime(300.0)
	aMetric[1].TimeStamp = floatToTime(300.0)
	aMetric[2].TimeStamp = floatToTime(302.0)
	aMetric[3].TimeStamp = floatToTime(302.0)
	aStore.AddBatch(kEndpoint0, 1020, aMetric[:])

	aMetric[2].Value = 302
	aMetric[0].TimeStamp = floatToTime(400.0)
	aMetric[1].TimeStamp = floatToTime(400.0)
	aMetric[2].TimeStamp = floatToTime(402.0)
	aMetric[3].TimeStamp = floatToTime(402.0)
	aStore.AddBatch(kEndpoint0, 1030, aMetric[:])

	aMetric[0].Value = 400
	aMetric[1].Value = 401
	aMetric[2].Value = 402
	aMetric[3].Value = 403
	aMetric[0].TimeStamp = floatToTime(500.0)
	aMetric[1].TimeStamp = floatToTime(500.0)
	aMetric[2].TimeStamp = floatToTime(502.0)
	aMetric[3].TimeStamp = floatToTime(502.0)
	aStore.AddBatch(kEndpoint0, 1040, aMetric[:])

	// Timestamps for group 0 don't advance
	aMetric[3].Value = 503
	aMetric[2].TimeStamp = floatToTime(602.0)
	aMetric[3].TimeStamp = floatToTime(602.0)
	aStore.AddBatch(kEndpoint0, 1050, aMetric[:])

	// metric 0 goes missing but timestamps don't advance.
	aMetric[3].Value = 603
	aMetric[2].TimeStamp = floatToTime(702.0)
	aMetric[3].TimeStamp = floatToTime(702.0)
	aStore.AddBatch(kEndpoint0, 1060, aMetric[1:])

	// This time, timestamps advance but metric 1 goes missing
	aMetric[0].Value = 700
	aMetric[3].Value = 703
	aMetric[0].TimeStamp = floatToTime(800.0)
	aMetric[1].TimeStamp = floatToTime(800.0)
	aMetric[2].TimeStamp = floatToTime(802.0)
	aMetric[3].TimeStamp = floatToTime(802.0)
	tempMetric := aMetric
	tempMetric[1], tempMetric[0] = tempMetric[0], tempMetric[1]
	aStore.AddBatch(kEndpoint0, 1070, tempMetric[1:])

	// This time all of group 0 goes missing
	aMetric[3].Value = 803
	aMetric[2].TimeStamp = floatToTime(902.0)
	aMetric[3].TimeStamp = floatToTime(902.0)
	aStore.AddBatch(kEndpoint0, 1080, aMetric[2:])

	// This time all of group 2 goes missing
	aMetric[0].Value = 900
	aMetric[1].Value = 901
	aMetric[0].TimeStamp = floatToTime(1000.0)
	aMetric[1].TimeStamp = floatToTime(1000.0)
	aStore.AddBatch(kEndpoint0, 1090, aMetric[:2])

	// Everything missing
	aStore.AddBatch(kEndpoint0, 1100, aMetric[:0])

	expected := newExpectedTsValues()
	expected.Add("Alice", 100.0, 0)
	expected.Add("Bob", 100.0, 1)
	expected.Add("Charlie", 102.0, 2)
	expected.Add("FoxTrot", 102.0, 3)

	expected.Add("Alice", 200.0, 0)
	expected.Add("Bob", 200.0, 101)
	expected.Add("Charlie", 202.0, 2)
	expected.Add("FoxTrot", 202.0, 3)

	expected.Add("Alice", 300.0, 200)
	expected.Add("Bob", 300.0, 201)
	expected.Add("Charlie", 302.0, 202)
	expected.Add("FoxTrot", 302.0, 203)

	expected.Add("Alice", 400.0, 200)
	expected.Add("Bob", 400.0, 201)
	expected.Add("Charlie", 402.0, 302)
	expected.Add("FoxTrot", 402.0, 203)

	expected.Add("Alice", 500.0, 400)
	expected.Add("Bob", 500.0, 401)
	expected.Add("Charlie", 502.0, 402)
	expected.Add("FoxTrot", 502.0, 403)

	expected.Add("Alice", 500.0, 400)
	expected.Add("Bob", 500.0, 401)
	expected.Add("Charlie", 602.0, 402)
	expected.Add("FoxTrot", 602.0, 503)

	// Even though alice goes missing, we don't log a missing value
	// because the we already have value 400 for alice at the same
	// timestamp
	expected.Add("Alice", 500.0, 400)
	expected.Add("Bob", 500.0, 401)
	expected.Add("Charlie", 702.0, 402)
	expected.Add("FoxTrot", 702.0, 603)

	expected.Add("Alice", 800.0, 700)
	expected.Add("Bob", 800.0, int64(0))
	expected.Add("Charlie", 802.0, 402)
	expected.Add("FoxTrot", 802.0, 703)

	expected.Add("Alice", 800.001, int64(0))
	// The Iterator is simple and does not try to filter consecutive
	// missing flags.
	expected.Add("Bob", 800.001, int64(0))
	expected.Add("Charlie", 902.0, 402)
	expected.Add("FoxTrot", 902.0, 803)

	expected.Add("Alice", 1000.0, 900)
	expected.Add("Bob", 1000.0, 901)
	expected.Add("Charlie", 902.001, int32(0))
	expected.Add("FoxTrot", 902.001, int64(0))

	expected.Add("Alice", 1000.001, int64(0))
	expected.Add("Bob", 1000.001, int64(0))
	expected.Add("Charlie", 902.001, int32(0))
	expected.Add("FoxTrot", 902.001, int64(0))

	beginning := expected.Checkpoint()

	iterator := aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 0)

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
	iterator = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	expected.Iterate(t, iterator)
	expected.Restore(beginning)
	iterator = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	valueCount := expected.Iterate(t, iterator)
	iterator.Commit()
	for valueCount > 0 {
		// 8 = 4 metrics * 2 times per metric
		if valueCount > 8 {
			t.Error("Got too many values")
		}
		iterator = aStore.NamedIteratorForEndpoint(
			"anIterator", kEndpoint0, 2)
		checkpoint := expected.Checkpoint()
		expected.Iterate(t, iterator)
		expected.Restore(checkpoint)
		iterator = aStore.NamedIteratorForEndpoint(
			"anIterator", kEndpoint0, 2)
		valueCount = expected.Iterate(t, iterator)
		iterator.Commit()
	}
	expected.VerifyDone(t)

	iterator = aStore.NamedIteratorForEndpoint(
		"anIterator", kEndpoint0, 2)
	expected.Iterate(t, iterator)
	iterator.Commit()
	expected.VerifyDone(t)

	// Now iterate again but test iterating 5 at a time, committing
	// and creating a new iterator. Since we committed the previous
	// iterator, we have to use a new name to start from the beginning.
	expected.Restore(beginning)

	iterator = aStore.NamedIteratorForEndpoint(
		"anotherIterator", kEndpoint0, 0)
	valueCount = expected.Iterate(t, iteratorLimit(iterator, 5))
	iterator.Commit()
	for valueCount > 0 {
		iterator = aStore.NamedIteratorForEndpoint(
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
	filteredExpected.Add("Alice", 100.0, 0)
	filteredExpected.Add("Alice", 200.0, 0)
	filteredExpected.Add("Alice", 300.0, 200)

	iterator = aStore.NamedIteratorForEndpoint(
		"aThirdIterator", kEndpoint0, 0)
	filteredIterator := store.NamedIteratorFilterFunc(
		iterator,
		func(r *store.Record) bool {
			return r.TimeStamp < 400 && r.Info.Path() == "Alice"
		})
	filteredExpected.Iterate(t, filteredIterator)
	filteredExpected.VerifyDone(t)
	filteredIterator.Commit()

	iterator = aStore.NamedIteratorForEndpoint(
		"aThirdIterator", kEndpoint0, 0)
	filteredExpected.Iterate(t, iterator)
}

func TestIndivMetricGoneInactive(t *testing.T) {
	aStore := store.NewStore(1, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aMetric := [3]*messages.Metric{
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/32bit",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int32,
			Bits:        32,
		},
	}
	aMetric[0].Value = 3
	aMetric[1].Value = 8
	aStore.AddBatch(kEndpoint0, 1000, aMetric[0:2])
	aMetric[0].Value = 13
	aMetric[1].Value = 18
	aStore.AddBatch(kEndpoint0, 1010, aMetric[0:2])
	aMetric[0].Value = 23
	aMetric[1].Value = 28
	aStore.AddBatch(kEndpoint0, 1020, aMetric[0:2])

	// foo/bar metric inactive now
	aMetric[1].Value = 38
	aMetric[2].Value = 39
	aStore.AddBatch(kEndpoint0, 1030, aMetric[1:3])

	// foo/32bit inactive now
	aMetric[0].Value = 43
	aMetric[1].Value = 48
	aStore.AddBatch(kEndpoint0, 1040, aMetric[0:2])

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 1020.0, 1041.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 1040.0, result[0].TimeStamp)
	assertValueEquals(t, 43, result[0].Value)
	assertValueEquals(t, true, result[0].Active)
	assertValueEquals(t, 1030.0, result[1].TimeStamp)
	assertValueEquals(t, int64(0), result[1].Value)
	assertValueEquals(t, false, result[1].Active)
	assertValueEquals(t, 1020.0, result[2].TimeStamp)
	assertValueEquals(t, 23, result[2].Value)
	assertValueEquals(t, true, result[2].Active)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/32bit", kEndpoint0, 1020.0, 1041.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 1040.0, result[0].TimeStamp)
	assertValueEquals(t, int32(0), result[0].Value)
	assertValueEquals(t, false, result[0].Active)
	assertValueEquals(t, 1030.0, result[1].TimeStamp)
	assertValueEquals(t, 39, result[1].Value)
	assertValueEquals(t, true, result[1].Active)
}

func TestMachineGoneInactive(t *testing.T) {
	aStore := store.NewStore(1, 100, 1.0, 10)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)
	aMetric := [2]*messages.Metric{
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}
	aMetric[0].Value = 6
	aMetric[1].Value = 8
	aStore.AddBatch(kEndpoint0, 900, aMetric[:])
	aMetric[0].Value = 16
	aMetric[1].Value = 18
	aStore.AddBatch(kEndpoint0, 910, aMetric[:])

	aMetric[0].Value = 1
	aMetric[1].Value = 2
	aStore.AddBatch(kEndpoint1, 1900, aMetric[:])
	aMetric[0].Value = 11
	aMetric[1].Value = 12
	aStore.AddBatch(kEndpoint1, 1910, aMetric[:])
	aMetric[0].Value = 11
	aMetric[1].Value = 22
	aStore.AddBatch(kEndpoint1, 1920, aMetric[:])

	aStore.MarkEndpointInactive(1915.0, kEndpoint1)

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 900.0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 910.0, result[0].TimeStamp)
	assertValueEquals(t, 16, result[0].Value)
	assertValueEquals(t, true, result[0].Active)
	assertValueEquals(t, 900.0, result[1].TimeStamp)
	assertValueEquals(t, 6, result[1].Value)
	assertValueEquals(t, true, result[1].Active)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 1900.0, 2000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 1915.0, result[0].TimeStamp)
	assertValueEquals(t, int64(0), result[0].Value)
	assertValueEquals(t, false, result[0].Active)
	assertValueEquals(t, 1910.0, result[1].TimeStamp)
	assertValueEquals(t, 11, result[1].Value)
	assertValueEquals(t, true, result[1].Active)
	assertValueEquals(t, 1900.0, result[2].TimeStamp)
	assertValueEquals(t, 1, result[2].Value)
	assertValueEquals(t, true, result[2].Active)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 1900.0, 2000.0, store.AppendTo(&result))

	assertValueEquals(t, 4, len(result))
	assertValueEquals(t, 1920.001, result[0].TimeStamp)
	assertValueEquals(t, int64(0), result[0].Value)
	assertValueEquals(t, false, result[0].Active)
	assertValueEquals(t, 1920.0, result[1].TimeStamp)
	assertValueEquals(t, 22, result[1].Value)
	assertValueEquals(t, true, result[1].Active)
	assertValueEquals(t, 1910.0, result[2].TimeStamp)
	assertValueEquals(t, 12, result[2].Value)
	assertValueEquals(t, true, result[2].Active)
	assertValueEquals(t, 1900.0, result[3].TimeStamp)
	assertValueEquals(t, 2, result[3].Value)
	assertValueEquals(t, true, result[3].Active)

	if _, ok := aStore.AddBatch(kEndpoint1, 2000.0, nil); ok {
		t.Error("Expected AddBatch to fail")
	}
	aStore.MarkEndpointActive(kEndpoint1)
	if _, ok := aStore.AddBatch(kEndpoint1, 2000.0, nil); !ok {
		t.Error("Expected AddBatch to succeed")
	}
}

func TestByNameAndEndpointAndEndpoint(t *testing.T) {
	aStore := store.NewStore(2, 8, 1.0, 10)
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

	aMetric := [3]*messages.Metric{
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        32,
		},
	}

	// On each of two endpoints:
	// 2 time series, 8 unique values, 5 unique timestamps
	// paged values per endpoint = 8 - 2 = 6
	// paged timestamps per endpoint = 5 - 1 = 4
	// total: 6*2 = 12 paged values and 4*2 = 8 paged timestamps
	// Need 12 / 2 = 6 pages for values and 8 / 6 = 2 pages for timestamps
	aMetric[0].Value = 0
	aMetric[1].Value = 1
	addBatch(t, aStore, kEndpoint0, 100.0, aMetric[:2], 2)
	aMetric[1].Value = 11
	addBatch(t, aStore, kEndpoint0, 110.0, aMetric[:2], 1)
	aMetric[0].Value = 20
	aMetric[1].Value = 21
	addBatch(t, aStore, kEndpoint0, 120.0, aMetric[:2], 2)
	aMetric[1].Value = 31
	addBatch(t, aStore, kEndpoint0, 130.0, aMetric[:2], 1)
	aMetric[0].Value = 40
	aMetric[1].Value = 41
	addBatch(t, aStore, kEndpoint0, 140.0, aMetric[:2], 2)

	aMetric[0].Value = 5
	aMetric[1].Value = 6
	addBatch(t, aStore, kEndpoint1, 105.0, aMetric[:2], 2)
	aMetric[1].Value = 16
	addBatch(t, aStore, kEndpoint1, 115.0, aMetric[:2], 1)
	aMetric[0].Value = 25
	aMetric[1].Value = 26
	addBatch(t, aStore, kEndpoint1, 125.0, aMetric[:2], 2)
	aMetric[1].Value = 36
	addBatch(t, aStore, kEndpoint1, 135.0, aMetric[:2], 1)
	aMetric[0].Value = 45
	aMetric[1].Value = 46
	addBatch(t, aStore, kEndpoint1, 145.0, aMetric[:2], 2)

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
	assertValueEquals(t, 40, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 134.0, 130.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 130.0, 131.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 120.0, result[0].TimeStamp)
	assertValueEquals(t, 20, result[0].Value)

	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/bar", kEndpoint0, 120.0, 121.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 120.0, result[0].TimeStamp)
	assertValueEquals(t, 20, result[0].Value)

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
	assertValueEquals(t, 20, result[0].Value)
	assertValueEquals(t, 100.0, result[1].TimeStamp)
	assertValueEquals(t, 0, result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 5, len(result))
	assertValueEquals(t, 140.0, result[0].TimeStamp)
	assertValueEquals(t, 41, result[0].Value)
	assertValueEquals(t, 130.0, result[1].TimeStamp)
	assertValueEquals(t, 31, result[1].Value)
	assertValueEquals(t, 120.0, result[2].TimeStamp)
	assertValueEquals(t, 21, result[2].Value)
	assertValueEquals(t, 110.0, result[3].TimeStamp)
	assertValueEquals(t, 11, result[3].Value)
	assertValueEquals(t, 100.0, result[4].TimeStamp)
	assertValueEquals(t, 1, result[4].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, 45, result[0].Value)
	assertValueEquals(t, 125.0, result[1].TimeStamp)
	assertValueEquals(t, 25, result[1].Value)
	assertValueEquals(t, 105.0, result[2].TimeStamp)
	assertValueEquals(t, 5, result[2].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 5, len(result))
	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, 46, result[0].Value)
	assertValueEquals(t, 135.0, result[1].TimeStamp)
	assertValueEquals(t, 36, result[1].Value)
	assertValueEquals(t, 125.0, result[2].TimeStamp)
	assertValueEquals(t, 26, result[2].Value)
	assertValueEquals(t, 115.0, result[3].TimeStamp)
	assertValueEquals(t, 16, result[3].Value)
	assertValueEquals(t, 105.0, result[4].TimeStamp)
	assertValueEquals(t, 6, result[4].Value)

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
	assertValueEquals(t, 46, result[bazIdx+0].Value)
	assertValueEquals(t, 135.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, 36, result[bazIdx+1].Value)
	assertValueEquals(t, 125.0, result[bazIdx+2].TimeStamp)
	assertValueEquals(t, 26, result[bazIdx+2].Value)
	assertValueEquals(t, 115.0, result[bazIdx+3].TimeStamp)
	assertValueEquals(t, 16, result[bazIdx+3].Value)
	assertValueEquals(t, 105.0, result[bazIdx+4].TimeStamp)
	assertValueEquals(t, 6, result[bazIdx+4].Value)

	assertValueEquals(t, 145.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, 45, result[barIdx+0].Value)
	assertValueEquals(t, 125.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, 25, result[barIdx+1].Value)
	assertValueEquals(t, 105.0, result[barIdx+2].TimeStamp)
	assertValueEquals(t, 5, result[barIdx+2].Value)

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
	assertValueEquals(t, 26, result[bazIdx+0].Value)
	assertValueEquals(t, 115.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, 16, result[bazIdx+1].Value)

	assertValueEquals(t, 125.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, 25, result[barIdx+0].Value)
	assertValueEquals(t, 105.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, 5, result[barIdx+1].Value)

	// Now add 3 more values. Doing this should evict one page
	// from endpoint0 containing two values. The third value
	// being added is for a new time series so it needs no page space.
	aMetric[0].Value = 55
	aMetric[1].Value = 56
	aMetric[2].Value = 57
	addBatch(t, aStore, kEndpoint1, 155.0, aMetric[:], 3)

	result = nil
	aStore.ByEndpoint(
		kEndpoint1, 0.0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 11, len(result))

	result = nil
	aStore.ByEndpoint(
		kEndpoint0, 0.0, 1000.0, store.AppendTo(&result))

	// 6 not 8
	assertValueEquals(t, 6, len(result))

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
	assertValueEquals(t, 40, result[barIdx].Value)
	assertValueEquals(t, 140.0, result[bazIdx].TimeStamp)
	assertValueEquals(t, 41, result[bazIdx].Value)

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
	assertValueEquals(t, 57, result[bits32].Value)
	assertValueEquals(t, 155.0, result[bits64+0].TimeStamp)
	assertValueEquals(t, 56, result[bits64+0].Value)
	assertValueEquals(t, 145.0, result[bits64+1].TimeStamp)
	assertValueEquals(t, 46, result[bits64+1].Value)

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
	aStore := store.NewStore(1, 6, 1.0, 10)
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
	// Since the high priority threshhold is 100%, we don't expect all the
	// pages of /baz to get reclaimed.
	if len(result) <= 1 {
		t.Error("Expected /baz to have at least 2 values")
	}

	// Holds 9 values
	aStore = store.NewStore(1, 6, 0.0, 10)
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

	// Since the high priority threshhold is only 0%, we expect the
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
	m messages.MetricList,
	count int) {
	out, ok := astore.AddBatch(endpointId, ts, m)
	if !ok {
		t.Errorf("Expected AddBatch to succeed")
	}
	if out != count {
		t.Errorf("Expected %d added, got %d.", count, out)
	}
}

func assertValueEquals(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

// 3 values to /foo, /bar, /baz
// 4th value to /foo and /bar only on endpoint 0
func addDataForHighPriorityEvictionTest(s *store.Store) {
	aMetric := [3]*messages.Metric{
		{
			Path:        "/foo",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}
	aMetric[0].Value = 2
	aMetric[1].Value = 4
	aMetric[2].Value = 5
	s.AddBatch(kEndpoint0, 100, aMetric[:])
	aMetric[0].Value = 12
	aMetric[1].Value = 14
	aMetric[2].Value = 15
	s.AddBatch(kEndpoint0, 110, aMetric[:])
	aMetric[0].Value = 22
	aMetric[1].Value = 24
	s.AddBatch(kEndpoint0, 120, aMetric[:2])
	aMetric[0].Value = 32
	aMetric[1].Value = 34
	s.AddBatch(kEndpoint0, 130, aMetric[:2])
}
