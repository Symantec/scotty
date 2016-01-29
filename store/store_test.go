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
	astore *store.Store, e *scotty.Endpoint) error {
	astore.ByEndpoint(e, 0, 1000.0, s)
	return nil
}

type errVisitor int

func (e *errVisitor) Visit(
	astore *store.Store, ee *scotty.Endpoint) error {
	return kError
}

func TestVisitorError(t *testing.T) {
	builder := store.NewBuilder(1, 8)
	builder.RegisterEndpoint(kEndpoint0)
	builder.RegisterEndpoint(kEndpoint1)
	aStore := builder.Build()
	var ev errVisitor
	assertValueEquals(t, kError, aStore.VisitAllEndpoints(&ev))
}

func TestAggregateAppenderAndVisitor(t *testing.T) {
	builder := store.NewBuilder(1, 8)
	builder.RegisterEndpoint(kEndpoint0)
	builder.RegisterEndpoint(kEndpoint1)
	aStore := builder.Build()

	aMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	aMetric.Value = 1
	aStore.Add(kEndpoint0, 100.0, &aMetric)
	aMetric.Value = 2
	aStore.Add(kEndpoint0, 107.0, &aMetric)
	aMetric.Value = 3
	aStore.Add(kEndpoint0, 114.0, &aMetric)
	aMetric.Value = 4
	aStore.Add(kEndpoint0, 121.0, &aMetric)

	aMetric.Value = 11
	aStore.Add(kEndpoint1, 100.0, &aMetric)
	aMetric.Value = 12
	aStore.Add(kEndpoint1, 107.0, &aMetric)
	aMetric.Value = 13
	aStore.Add(kEndpoint1, 114.0, &aMetric)
	aMetric.Value = 14
	aStore.Add(kEndpoint1, 121.0, &aMetric)

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

func TestReclaimPages(t *testing.T) {
	builder := store.NewBuilder(1, 8)
	builder.RegisterEndpoint(kEndpoint0)
	builder.RegisterEndpoint(kEndpoint1)
	builder.RegisterEndpoint(kEndpoint2)
	aStore := builder.Build()

	firstMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}
	secondMetric := messages.Metric{
		Path:        "/foo/baz",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	if out := aStore.AvailablePages(); out != 8 {
		t.Errorf("Expected 8 pages, got %d", out)
	}

	// Adding these metrics uses all available pages.
	firstMetric.Value = 1
	add(t, aStore, kEndpoint0, 100.0, &firstMetric, true)
	firstMetric.Value = 2
	add(t, aStore, kEndpoint0, 110.0, &firstMetric, true)
	firstMetric.Value = 3
	add(t, aStore, kEndpoint0, 120.0, &firstMetric, true)
	firstMetric.Value = 4
	add(t, aStore, kEndpoint0, 130.0, &firstMetric, true)
	firstMetric.Value = 1
	add(t, aStore, kEndpoint1, 100.0, &firstMetric, true)
	firstMetric.Value = 2
	add(t, aStore, kEndpoint1, 110.0, &firstMetric, true)
	secondMetric.Value = 1
	add(t, aStore, kEndpoint1, 100.0, &secondMetric, true)
	firstMetric.Value = 1
	add(t, aStore, kEndpoint2, 100.0, &firstMetric, true)

	// All 8 pages used, but the first two of the four pages that
	// kEndpoint0, firstMetric is using can be recycled.
	if out := aStore.AvailablePages(); out != 2 {
		t.Errorf("Expected 2 pages, got %d", out)
	}

	// Now update aStore to have brand new end points.
	builder = aStore.NewBuilder()
	builder.RegisterEndpoint(kEndpoint2)
	builder.RegisterEndpoint(kEndpoint3)
	aStore = builder.Build()

	// We could recliam all pages except the one that
	// kEndpoint2, firstMetric is using.
	if out := aStore.AvailablePages(); out != 7 {
		t.Errorf("Expected 7 of 8 pages available, got %d", out)
	}
}

func TestAddBatch(t *testing.T) {
	builder := store.NewBuilder(1, 12)
	builder.RegisterEndpoint(kEndpoint0)
	aStore := builder.Build()

	aMetric := messages.Metric{
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	// Add 6 unique values to each endpoint.
	aMetric.Path = "/foo/bar"
	aMetric.Value = 1
	add(t, aStore, kEndpoint0, 100.0, &aMetric, true)
	aMetric.Path = "/foo/baz"
	aMetric.Value = 2
	add(t, aStore, kEndpoint0, 100.0, &aMetric, true)
	metrics := []*messages.Metric{
		// Not added a duplicate value
		{
			Path:        "/foo/bar",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int,
			Bits:        64,
			Value:       1,
		},
		// Added
		{
			Path:        "/foo/baz",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int,
			Bits:        64,
			Value:       3,
		},
		// Added
		{
			Path:        "/foo/alice",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int,
			Bits:        64,
			Value:       5,
		},
		// Not added, wrong type
		{
			Path:        "/foo/wrongType",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Uint,
			Bits:        64,
			Value:       5,
		},
	}
	var result []*store.Record
	aStore.AddBatch(
		kEndpoint0,
		105.0,
		metrics,
		func(m *messages.Metric) bool {
			return m.Kind == types.Int
		},
		func(r *store.Record) {
			recordCopy := *r
			result = append(result, &recordCopy)
		})

	assertValueEquals(t, 2, len(result))

	assertValueEquals(t, kEndpoint0, result[0].ApplicationId)
	assertValueEquals(t, "/foo/baz", result[0].Info.Path())
	assertValueEquals(t, 105.0, result[0].TimeStamp)
	assertValueEquals(t, 3, result[0].Value)

	assertValueEquals(t, kEndpoint0, result[1].ApplicationId)
	assertValueEquals(t, "/foo/alice", result[1].Info.Path())
	assertValueEquals(t, 105.0, result[1].TimeStamp)
	assertValueEquals(t, 5, result[1].Value)
}

func TestByNameAndEndpointAndEndpoint(t *testing.T) {
	builder := store.NewBuilder(1, 12)
	builder.RegisterEndpoint(kEndpoint0)
	builder.RegisterEndpoint(kEndpoint1)
	aStore := builder.Build()

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

	aMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	// Add 6 unique values to each endpoint.
	aMetric.Value = 0
	add(t, aStore, kEndpoint0, 100.0, &aMetric, true)
	aMetric.Value = 0
	add(t, aStore, kEndpoint0, 106.0, &aMetric, false)
	aMetric.Value = 4
	add(t, aStore, kEndpoint0, 112.0, &aMetric, true)
	aMetric.Value = 4
	add(t, aStore, kEndpoint0, 118.0, &aMetric, false)
	aMetric.Value = 8
	add(t, aStore, kEndpoint0, 124.0, &aMetric, true)
	aMetric.Value = 8
	add(t, aStore, kEndpoint0, 130.0, &aMetric, false)
	aMetric.Path = "/foo/baz"
	aMetric.Value = 1
	add(t, aStore, kEndpoint0, 103.0, &aMetric, true)
	aMetric.Value = 1
	add(t, aStore, kEndpoint0, 109.0, &aMetric, false)
	aMetric.Value = 5
	add(t, aStore, kEndpoint0, 115.0, &aMetric, true)
	aMetric.Value = 5
	add(t, aStore, kEndpoint0, 121.0, &aMetric, false)
	aMetric.Value = 9
	add(t, aStore, kEndpoint0, 127.0, &aMetric, true)
	aMetric.Value = 9
	add(t, aStore, kEndpoint0, 133.0, &aMetric, false)

	aMetric.Path = "/foo/bar"
	aMetric.Value = 10
	add(t, aStore, kEndpoint1, 200.0, &aMetric, true)
	aMetric.Value = 10
	add(t, aStore, kEndpoint1, 206.0, &aMetric, false)
	aMetric.Value = 14
	add(t, aStore, kEndpoint1, 212.0, &aMetric, true)
	aMetric.Value = 14
	add(t, aStore, kEndpoint1, 218.0, &aMetric, false)
	aMetric.Value = 18
	add(t, aStore, kEndpoint1, 224.0, &aMetric, true)
	aMetric.Value = 18
	add(t, aStore, kEndpoint1, 230.0, &aMetric, false)
	aMetric.Path = "/foo/baz"
	aMetric.Value = 11
	add(t, aStore, kEndpoint1, 203.0, &aMetric, true)
	aMetric.Value = 11
	add(t, aStore, kEndpoint1, 209.0, &aMetric, false)
	aMetric.Value = 15
	add(t, aStore, kEndpoint1, 215.0, &aMetric, true)
	aMetric.Value = 15
	add(t, aStore, kEndpoint1, 221.0, &aMetric, false)
	aMetric.Value = 19
	add(t, aStore, kEndpoint1, 227.0, &aMetric, true)
	aMetric.Value = 19
	add(t, aStore, kEndpoint1, 233.0, &aMetric, false)

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

	// Now add 2 more values. Doing this should evict
	// The earliest /foo/bar and /foo/baz value on endpoint0.
	// leaving only
	// t=112, value=4 and t=124, value=8 for /foo/bar.
	// t=115, value=5 and t=127, value=9 for /foo/baz
	aMetric.Path = "/foo/baz"
	aMetric.Value = 13
	add(t, aStore, kEndpoint0, 139.0, &aMetric, true)
	aMetric.Value = 15
	add(t, aStore, kEndpoint0, 145.0, &aMetric, true)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 124.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 123.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)
	assertValueEquals(t, 112.0, result[1].TimeStamp)
	assertValueEquals(t, 4, result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 100.0, 125.0, store.AppendTo(&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 124.0, result[0].TimeStamp)
	assertValueEquals(t, 8, result[0].Value)
	assertValueEquals(t, 112.0, result[1].TimeStamp)
	assertValueEquals(t, 4, result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/bar", kEndpoint0, 100.0, 124.0, store.AppendTo(&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 112.0, result[0].TimeStamp)
	assertValueEquals(t, 4, result[0].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint0, 0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 4, len(result))

	assertValueEquals(t, 145.0, result[0].TimeStamp)
	assertValueEquals(t, 15, result[0].Value)
	assertValueEquals(t, 139.0, result[1].TimeStamp)
	assertValueEquals(t, 13, result[1].Value)
	assertValueEquals(t, 127.0, result[2].TimeStamp)
	assertValueEquals(t, 9, result[2].Value)
	assertValueEquals(t, 115.0, result[3].TimeStamp)
	assertValueEquals(t, 5, result[3].Value)

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

	// Now add "foo/baz" values but change the "foo/baz" metric so
	// that it is different.
	// This will cause the earliest value for /foo/bar in endpoint1 to
	// get evicted, time=200 value=10
	aMetric.Path = "/foo/baz"
	aMetric.Bits = 32
	aMetric.Value = 29
	add(t, aStore, kEndpoint0, 145.0, &aMetric, true)

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
	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 224.0, result[0].TimeStamp)
	assertValueEquals(t, 18, result[0].Value)
	assertValueEquals(t, 212.0, result[1].TimeStamp)
	assertValueEquals(t, 14, result[1].Value)

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo/baz", kEndpoint1, 0.0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 3, len(result))
}

func add(
	t *testing.T,
	astore *store.Store,
	endpointId *scotty.Endpoint,
	ts float64,
	m *messages.Metric,
	success bool) {
	if astore.Add(endpointId, ts, m) != success {
		if success {
			t.Errorf("Expected %v to be added.", m.Value)
		} else {
			t.Errorf("Expected %v not to be added.", m.Value)
		}
	}
}

func assertValueEquals(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
