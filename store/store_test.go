package store_test

import (
	"errors"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"reflect"
	"testing"
)

var (
	kEndpoint0 = scotty.NewEndpoint("host1", 1001)
	kEndpoint1 = scotty.NewEndpoint("host2", 1002)
	kEndpoint2 = scotty.NewEndpoint("host3", 1001)
	kEndpoint3 = scotty.NewEndpoint("host4", 1002)
	kError     = errors.New("An error")
)

type sumMetricsType int

func (s *sumMetricsType) Append(r *store.Record) {
	*s += sumMetricsType(r.Value.(int))
}

func (s *sumMetricsType) Visit(
	astore *store.Store, e interface{}) error {
	astore.ByEndpoint(e, 0, 1000.0, s)
	return nil
}

type errVisitor int

func (e *errVisitor) Visit(
	astore *store.Store, ee interface{}) error {
	return kError
}

func TestVisitorError(t *testing.T) {
	aStore := store.NewStore(1, 8)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)
	var ev errVisitor
	assertValueEquals(t, kError, aStore.VisitAllEndpoints(&ev))
}

func TestAggregateAppenderAndVisitor(t *testing.T) {
	aStore := store.NewStore(1, 8)
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

func TestIterator(t *testing.T) {
	aStore := store.NewStore(2, 3) // Stores 2*3 + 1 values
	aStore.RegisterEndpoint(kEndpoint0)

	firstMetric := [1]*messages.Metric{
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}
	secondMetric := [1]*messages.Metric{
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}

	// Adding 8 values evicts first 2 values
	firstMetric[0].Value = 1
	addBatch(t, aStore, kEndpoint0, 100.0, firstMetric[:], 1)
	firstMetric[0].Value = 2
	addBatch(t, aStore, kEndpoint0, 110.0, firstMetric[:], 1)
	firstMetric[0].Value = 3
	addBatch(t, aStore, kEndpoint0, 120.0, firstMetric[:], 1)
	firstMetric[0].Value = 4
	addBatch(t, aStore, kEndpoint0, 130.0, firstMetric[:], 1)
	firstMetric[0].Value = 5
	addBatch(t, aStore, kEndpoint0, 140.0, firstMetric[:], 1)
	firstMetric[0].Value = 6
	addBatch(t, aStore, kEndpoint0, 150.0, firstMetric[:], 1)
	firstMetric[0].Value = 7
	addBatch(t, aStore, kEndpoint0, 160.0, firstMetric[:], 1)
	firstMetric[0].Value = 8
	addBatch(t, aStore, kEndpoint0, 170.0, firstMetric[:], 1)

	iterators := aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))
	assertValueEquals(t, "/foo/bar", iterators[0].Info().Path())

	ts, value, skipped := iterators[0].Next()
	assertValueEquals(t, 120.0, ts)
	assertValueEquals(t, 3, value)
	assertValueEquals(t, 2, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 130.0, ts)
	assertValueEquals(t, 4, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Since we didn't commit, iterator starts over
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 120.0, ts)
	assertValueEquals(t, 3, value)
	assertValueEquals(t, 2, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 130.0, ts)
	assertValueEquals(t, 4, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Next iterator will start where this one left off
	iterators[0].Commit()

	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 140.0, ts)
	assertValueEquals(t, 5, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 150.0, ts)
	assertValueEquals(t, 6, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Commit should be idempotent
	iterators[0].Commit()
	iterators[0].Commit()

	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 160.0, ts)
	assertValueEquals(t, 7, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// No commit, so we replay the same values
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 160.0, ts)
	assertValueEquals(t, 7, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	iterators[0].Commit()
	iterators[0].Commit()

	// Now we have past the pages and are at the last value
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 170.0, ts)
	assertValueEquals(t, 8, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// No commit, so we replay the same values
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 170.0, ts)
	assertValueEquals(t, 8, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	iterators[0].Commit()

	// No more values to consume
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Now add a new value
	firstMetric[0].Value = 9
	addBatch(t, aStore, kEndpoint0, 180.0, firstMetric[:], 1)

	// Now there is one more value to consume
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 180.0, ts)
	assertValueEquals(t, 9, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	iterators[0].Commit()

	// No more values to consume
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Now add a new value
	firstMetric[0].Value = 10
	addBatch(t, aStore, kEndpoint0, 190.0, firstMetric[:], 1)

	// Now there is one more value to consume
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, 190.0, ts)
	assertValueEquals(t, 10, value)
	assertValueEquals(t, 0, skipped)

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	iterators[0].Commit()

	// No more values to consume
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 1, len(iterators))

	ts, value, skipped = iterators[0].Next()
	assertValueEquals(t, nil, value)

	// Add a different metric
	secondMetric[0].Value = 1
	addBatch(t, aStore, kEndpoint0, 100.0, secondMetric[:], 1)

	// We should have two iterators now
	iterators = aStore.Iterators(kEndpoint0)
	assertValueEquals(t, 2, len(iterators))
}

func TestIndivMetricGoneInactive(t *testing.T) {
	aStore := store.NewStore(1, 100)
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

	var result []*store.Record
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

func TestByNameAndEndpointAndEndpoint(t *testing.T) {
	aStore := store.NewStore(1, 8)
	aStore.RegisterEndpoint(kEndpoint0)
	aStore.RegisterEndpoint(kEndpoint1)

	var result []*store.Record

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

	// Add 6 unique values to each endpoint.
	aMetric[0].Value = 0
	addBatch(t, aStore, kEndpoint0, 100.0, aMetric[:1], 1)
	aMetric[0].Value = 0
	addBatch(t, aStore, kEndpoint0, 106.0, aMetric[:1], 0)
	aMetric[0].Value = 4
	addBatch(t, aStore, kEndpoint0, 112.0, aMetric[:1], 1)
	aMetric[0].Value = 4
	addBatch(t, aStore, kEndpoint0, 118.0, aMetric[:1], 0)
	aMetric[0].Value = 8
	addBatch(t, aStore, kEndpoint0, 124.0, aMetric[:1], 1)
	aMetric[0].Value = 8
	addBatch(t, aStore, kEndpoint0, 130.0, aMetric[:1], 0)
	aMetric[1].Value = 1

	// We pass both metrics from now on just to keep /foo/bar
	// in endpoint0 active.
	addBatch(t, aStore, kEndpoint0, 103.0, aMetric[:2], 1)
	aMetric[1].Value = 1
	addBatch(t, aStore, kEndpoint0, 109.0, aMetric[:2], 0)
	aMetric[1].Value = 5
	addBatch(t, aStore, kEndpoint0, 115.0, aMetric[:2], 1)
	aMetric[1].Value = 5
	addBatch(t, aStore, kEndpoint0, 121.0, aMetric[:2], 0)
	aMetric[1].Value = 9
	addBatch(t, aStore, kEndpoint0, 127.0, aMetric[:2], 1)
	aMetric[1].Value = 9
	addBatch(t, aStore, kEndpoint0, 133.0, aMetric[:2], 0)

	aMetric[0].Value = 10
	addBatch(t, aStore, kEndpoint1, 200.0, aMetric[:1], 1)
	aMetric[0].Value = 10
	addBatch(t, aStore, kEndpoint1, 206.0, aMetric[:1], 0)
	aMetric[0].Value = 14
	addBatch(t, aStore, kEndpoint1, 212.0, aMetric[:1], 1)
	aMetric[0].Value = 14
	addBatch(t, aStore, kEndpoint1, 218.0, aMetric[:1], 0)
	aMetric[0].Value = 18
	addBatch(t, aStore, kEndpoint1, 224.0, aMetric[:1], 1)
	aMetric[0].Value = 18
	addBatch(t, aStore, kEndpoint1, 230.0, aMetric[:1], 0)
	aMetric[1].Value = 11
	// We pass both metrics from now on just to keep /foo/bar
	// in endpoint1 active.
	addBatch(t, aStore, kEndpoint1, 203.0, aMetric[:2], 1)
	aMetric[1].Value = 11
	addBatch(t, aStore, kEndpoint1, 209.0, aMetric[:2], 0)
	aMetric[1].Value = 15
	addBatch(t, aStore, kEndpoint1, 215.0, aMetric[:2], 1)
	aMetric[1].Value = 15
	addBatch(t, aStore, kEndpoint1, 221.0, aMetric[:2], 0)
	aMetric[1].Value = 19
	addBatch(t, aStore, kEndpoint1, 227.0, aMetric[:2], 1)
	aMetric[1].Value = 19
	addBatch(t, aStore, kEndpoint1, 233.0, aMetric[:2], 0)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 130.0, 130.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 130.0, 131.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, kEndpoint0, result[0].ApplicationId)
	assertValueEquals(t, "/foo/bar", result[0].Info.Path())
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 124.0, 124.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 124.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/bar", kEndpoint0, 124.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	// Now we should get 1 from foo/bar and one from foo/baz.
	result = nil
	aStore.ByPrefixAndEndpoint(
		"/foo/ba", kEndpoint0, 124.0, 125.0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, kEndpoint0, result[0].ApplicationId)
	assertValueEquals(t, kEndpoint0, result[1].ApplicationId)
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
		"/foo/bat", kEndpoint0, 124.0, 125.0, store.AppendTo(&result))
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
		"/foo/bar", kEndpoint0, 0.0, 124.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 112.0, result[0].TimeStamp)
	assertValueEquals(t, 4, result[0].Value)
	assertValueEquals(t, 100.0, result[1].TimeStamp)
	assertValueEquals(t, 0, result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 127.0, result[0].TimeStamp)
	assertValueEquals(t, 9, result[0].Value)
	assertValueEquals(t, 115.0, result[1].TimeStamp)
	assertValueEquals(t, 5, result[1].Value)
	assertValueEquals(t, 103.0, result[2].TimeStamp)
	assertValueEquals(t, 1, result[2].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 224.0, result[0].TimeStamp)
	assertValueEquals(t, 18, result[0].Value)
	assertValueEquals(t, 212.0, result[1].TimeStamp)
	assertValueEquals(t, 14, result[1].Value)
	assertValueEquals(t, 200.0, result[2].TimeStamp)
	assertValueEquals(t, 10, result[2].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 0, 1000.0, store.AppendTo(&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 227.0, result[0].TimeStamp)
	assertValueEquals(t, 19, result[0].Value)
	assertValueEquals(t, 215.0, result[1].TimeStamp)
	assertValueEquals(t, 15, result[1].Value)
	assertValueEquals(t, 203.0, result[2].TimeStamp)
	assertValueEquals(t, 11, result[2].Value)

	result = nil
	aStore.ByEndpoint(kEndpoint1, 0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 6, len(result))
	var barIdx, bazIdx int
	if result[0].Info.Path() == "/foo/baz" {
		barIdx, bazIdx = 3, 0
	} else {
		barIdx, bazIdx = 0, 3
	}
	assertValueEquals(t, 227.0, result[bazIdx+0].TimeStamp)
	assertValueEquals(t, 19, result[bazIdx+0].Value)
	assertValueEquals(t, 215.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, 15, result[bazIdx+1].Value)
	assertValueEquals(t, 203.0, result[bazIdx+2].TimeStamp)
	assertValueEquals(t, 11, result[bazIdx+2].Value)

	assertValueEquals(t, 224.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, 18, result[barIdx+0].Value)
	assertValueEquals(t, 212.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, 14, result[barIdx+1].Value)
	assertValueEquals(t, 200.0, result[barIdx+2].TimeStamp)
	assertValueEquals(t, 10, result[barIdx+2].Value)

	result = nil
	aStore.ByEndpoint(kEndpoint1, 203.0, 224.0, store.AppendTo(&result))
	assertValueEquals(t, 4, len(result))
	if result[0].Info.Path() == "/foo/baz" {
		barIdx, bazIdx = 2, 0
	} else {
		barIdx, bazIdx = 0, 2
	}
	assertValueEquals(t, 215.0, result[bazIdx+0].TimeStamp)
	assertValueEquals(t, 15, result[bazIdx+0].Value)
	assertValueEquals(t, 203.0, result[bazIdx+1].TimeStamp)
	assertValueEquals(t, 11, result[bazIdx+1].Value)

	assertValueEquals(t, 212.0, result[barIdx+0].TimeStamp)
	assertValueEquals(t, 14, result[barIdx+0].Value)
	assertValueEquals(t, 200.0, result[barIdx+1].TimeStamp)
	assertValueEquals(t, 10, result[barIdx+1].Value)

	// Now add 2 more values. Doing this should evict the first 2
	// values on foo/bar on endpoint0 and leave foo/baz alone because
	// there is no longer a 2 page minimum.
	// t=124, value=8 for /foo/bar.
	// t=103, value=1, t=115, value=5 and t=127, value=9 for /foo/baz
	aMetric[1].Value = 13
	// Last value of endpoint0 /foo/bar needed to keep it active
	aMetric[0].Value = 8
	addBatch(t, aStore, kEndpoint0, 139.0, aMetric[:2], 1)
	aMetric[1].Value = 15
	addBatch(t, aStore, kEndpoint0, 145.0, aMetric[:2], 1)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 123.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 100.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 100.0, 124.0, store.AppendTo(&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 5, len(result))

	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, 15, result[0].Value)
	assertValueEquals(t, 139.0, result[1].TimeStamp)
	assertValueEquals(t, 13, result[1].Value)
	assertValueEquals(t, 127.0, result[2].TimeStamp)
	assertValueEquals(t, 9, result[2].Value)
	assertValueEquals(t, 115.0, result[3].TimeStamp)
	assertValueEquals(t, 5, result[3].Value)
	assertValueEquals(t, 103.0, result[4].TimeStamp)
	assertValueEquals(t, 1, result[4].Value)

	// Now test get latest metrics.
	result = nil
	aStore.LatestByEndpoint(kEndpoint0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
	if result[0].Info.Path() == "/foo/bar" {
		barIdx, bazIdx = 0, 1
	} else {
		barIdx, bazIdx = 1, 0
	}
	assertValueEquals(t, 124.0, result[barIdx].TimeStamp)
	assertValueEquals(t, 8, result[barIdx].Value)
	assertValueEquals(t, 145.0, result[bazIdx].TimeStamp)
	assertValueEquals(t, 15, result[bazIdx].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 103.0, 104.0, store.AppendTo(&result))
	assertValueEquals(t, 1, len(result))

	// Now add "foo/baz" values but change the "foo/baz" metric so
	// that it is different. Because this is a new metric with just
	// the latest value, no page eviction takes place.
	// We also have to pass the other values to keep them active.
	aMetric[2].Value = 29
	addBatch(t, aStore, kEndpoint0, 145.0, aMetric[:3], 1)

	// No eviction
	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 103.0, 104.0, store.AppendTo(&result))
	assertValueEquals(t, 1, len(result))

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 144.0, 152.0, store.AppendTo(&result))

	// Results grouped by metric first then by timestamp
	assertValueEquals(t, 3, len(result))
	var bits32, bits64 int
	if result[0].Info.Bits() == 32 {
		bits32, bits64 = 0, 1
	} else {
		bits32, bits64 = 2, 0
	}
	assertValueEquals(t, 145.0, result[bits32].TimeStamp)
	assertValueEquals(t, 29, result[bits32].Value)
	assertValueEquals(t, 145.0, result[bits64+0].TimeStamp)
	assertValueEquals(t, 15, result[bits64+0].Value)
	assertValueEquals(t, 139.0, result[bits64+1].TimeStamp)
	assertValueEquals(t, 13, result[bits64+1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint1, 0.0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 224.0, result[0].TimeStamp)
	assertValueEquals(t, 18, result[0].Value)
	assertValueEquals(t, 212.0, result[1].TimeStamp)
	assertValueEquals(t, 14, result[1].Value)
	assertValueEquals(t, 200.0, result[2].TimeStamp)
	assertValueEquals(t, 10, result[2].Value)
}

func addBatch(
	t *testing.T,
	astore *store.Store,
	endpointId *scotty.Endpoint,
	ts float64,
	m messages.MetricList,
	count int) {
	if out := astore.AddBatch(endpointId, ts, m); out != count {
		t.Errorf("Expected %d added, got %d.", count, out)
	}
}

func assertValueEquals(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
