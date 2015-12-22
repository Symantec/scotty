package store_test

import (
	"errors"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"testing"
	"time"
)

var (
	kMachine0 = scotty.NewMachine("host1", 1001, nil)
	kMachine1 = scotty.NewMachine("host2", 1002, nil)
	kError    = errors.New("An error")
)

func TestBlockingWrite(t *testing.T) {
	aStore := store.New(8, 3)
	aStore.RegisterMachine(kMachine0)

	aMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	// Add 5 values, the max.
	aMetric.Value = 0
	add(t, aStore, kMachine0, 100.0, &aMetric, true)
	aMetric.Value = 3
	add(t, aStore, kMachine0, 107.0, &aMetric, true)
	aMetric.Value = 6
	add(t, aStore, kMachine0, 114.0, &aMetric, true)
	aMetric.Value = 9
	add(t, aStore, kMachine0, 121.0, &aMetric, true)
	aMetric.Value = 12
	add(t, aStore, kMachine0, 128.0, &aMetric, true)

	var result []*store.Record
	session := aStore.NewSession()
	aStore.ByMachine(session, kMachine0, 0.0, 1000.0, store.AppendTo(&result))

	var wg sync.WaitGroup
	wg.Add(1)

	// Add 5 more values in a separate goroutine. This goroutine
	// should block as we haven't released session
	go func() {
		aMetric.Value = 15
		add(t, aStore, kMachine0, 135.0, &aMetric, true)
		aMetric.Value = 18
		add(t, aStore, kMachine0, 142.0, &aMetric, true)
		aMetric.Value = 21
		add(t, aStore, kMachine0, 149.0, &aMetric, true)
		aMetric.Value = 24
		add(t, aStore, kMachine0, 156.0, &aMetric, true)
		aMetric.Value = 27
		add(t, aStore, kMachine0, 163.0, &aMetric, true)
		wg.Done()
	}()

	// Try to be sure that above goroutine blocked
	time.Sleep(time.Second)

	// Above goroutine should not have corrupted our previous result
	assertValueEquals(t, 5, len(result))
	assertValueEquals(t, 12, result[0].Value())
	assertValueEquals(t, 9, result[1].Value())
	assertValueEquals(t, 6, result[2].Value())
	assertValueEquals(t, 3, result[3].Value())
	assertValueEquals(t, 0, result[4].Value())

	// Now release session and wait on goroutine to finish
	session.Close()
	wg.Wait()

	session = aStore.NewSession()
	result = nil
	aStore.ByMachine(session, kMachine0, 0.0, 1000.0, store.AppendTo(&result))

	// Now we should see the last 5 values the previous goroutine added
	// Plus the earliest value
	assertValueEquals(t, 6, len(result))
	assertValueEquals(t, 27, result[0].Value())
	assertValueEquals(t, 24, result[1].Value())
	assertValueEquals(t, 21, result[2].Value())
	assertValueEquals(t, 18, result[3].Value())
	assertValueEquals(t, 15, result[4].Value())
	assertValueEquals(t, 12, result[5].Value())
}

type sumMetricsType int

func (s *sumMetricsType) Append(r *store.Record) bool {
	*s += sumMetricsType(r.Value().(int))
	return false
}

func (s *sumMetricsType) Visit(
	astore *store.Store, m *scotty.Machine) error {
	astore.ByMachine(nil, m, 0, 1000.0, s)
	return nil
}

type errVisitor int

func (e *errVisitor) Visit(
	astore *store.Store, m *scotty.Machine) error {
	return kError
}

func TestVisitorError(t *testing.T) {
	aStore := store.New(7, 3)
	aStore.RegisterMachine(kMachine0)
	aStore.RegisterMachine(kMachine1)
	var ev errVisitor
	assertValueEquals(t, kError, aStore.VisitAllMachines(&ev))
}

func TestAggregateAppenderAndVisitor(t *testing.T) {
	aStore := store.New(7, 3)
	aStore.RegisterMachine(kMachine0)
	aStore.RegisterMachine(kMachine1)

	aMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	aMetric.Value = 1
	aStore.Add(kMachine0, 100.0, &aMetric)
	aMetric.Value = 2
	aStore.Add(kMachine0, 107.0, &aMetric)
	aMetric.Value = 3
	aStore.Add(kMachine0, 114.0, &aMetric)
	aMetric.Value = 4
	aStore.Add(kMachine0, 121.0, &aMetric)

	aMetric.Value = 11
	aStore.Add(kMachine1, 100.0, &aMetric)
	aMetric.Value = 12
	aStore.Add(kMachine1, 107.0, &aMetric)
	aMetric.Value = 13
	aStore.Add(kMachine1, 114.0, &aMetric)
	aMetric.Value = 14
	aStore.Add(kMachine1, 121.0, &aMetric)

	var total sumMetricsType

	aStore.VisitAllMachines(&total)
	assertValueEquals(t, 60, int(total))

	total = 0
	aStore.ByMachine(nil, kMachine0, 0, 1000.0, &total)
	assertValueEquals(t, 10, int(total))

	total = 0
	aStore.ByNameAndMachine(nil, "/foo/bar", kMachine1, 0, 1000.0, &total)
	assertValueEquals(t, 50, int(total))

	total = 0
	aStore.LatestByMachine(nil, kMachine1, &total)
	assertValueEquals(t, 14, int(total))
}

func TestByNameAndMachineAndMachine(t *testing.T) {
	aStore := store.New(7, 1)
	aStore.RegisterMachine(kMachine0)
	aStore.RegisterMachine(kMachine1)

	var result []*store.Record
	session := aStore.NewSession()

	result = nil
	aStore.ByMachine(session, kMachine1, 0.0, 100.0, store.AppendTo(&result))
	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine1, 0.0, 100.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.LatestByMachine(session, kMachine1, store.AppendTo(&result))
	assertValueEquals(t, 0, len(result))

	assertValueEquals(t, true, session.IsNew())

	session.Close()
	session = aStore.NewSession()

	aMetric := messages.Metric{
		Path:        "/foo/bar",
		Description: "A description",
		Unit:        units.None,
		Kind:        types.Int,
		Bits:        64}

	// Add 6 unique values to each machine, the max.
	aMetric.Value = 0
	add(t, aStore, kMachine0, 100.0, &aMetric, true)
	aMetric.Value = 0
	add(t, aStore, kMachine0, 106.0, &aMetric, false)
	aMetric.Value = 4
	add(t, aStore, kMachine0, 112.0, &aMetric, true)
	aMetric.Value = 4
	add(t, aStore, kMachine0, 118.0, &aMetric, false)
	aMetric.Value = 8
	add(t, aStore, kMachine0, 124.0, &aMetric, true)
	aMetric.Value = 8
	add(t, aStore, kMachine0, 130.0, &aMetric, false)
	aMetric.Path = "/foo/baz"
	aMetric.Value = 1
	add(t, aStore, kMachine0, 103.0, &aMetric, true)
	aMetric.Value = 1
	add(t, aStore, kMachine0, 109.0, &aMetric, false)
	aMetric.Value = 5
	add(t, aStore, kMachine0, 115.0, &aMetric, true)
	aMetric.Value = 5
	add(t, aStore, kMachine0, 121.0, &aMetric, false)
	aMetric.Value = 9
	add(t, aStore, kMachine0, 127.0, &aMetric, true)
	aMetric.Value = 9
	add(t, aStore, kMachine0, 133.0, &aMetric, false)

	aMetric.Path = "/foo/bar"
	aMetric.Value = 10
	add(t, aStore, kMachine1, 200.0, &aMetric, true)
	aMetric.Value = 10
	add(t, aStore, kMachine1, 206.0, &aMetric, false)
	aMetric.Value = 14
	add(t, aStore, kMachine1, 212.0, &aMetric, true)
	aMetric.Value = 14
	add(t, aStore, kMachine1, 218.0, &aMetric, false)
	aMetric.Value = 18
	add(t, aStore, kMachine1, 224.0, &aMetric, true)
	aMetric.Value = 18
	add(t, aStore, kMachine1, 230.0, &aMetric, false)
	aMetric.Path = "/foo/baz"
	aMetric.Value = 11
	add(t, aStore, kMachine1, 203.0, &aMetric, true)
	aMetric.Value = 11
	add(t, aStore, kMachine1, 209.0, &aMetric, false)
	aMetric.Value = 15
	add(t, aStore, kMachine1, 215.0, &aMetric, true)
	aMetric.Value = 15
	add(t, aStore, kMachine1, 221.0, &aMetric, false)
	aMetric.Value = 19
	add(t, aStore, kMachine1, 227.0, &aMetric, true)
	aMetric.Value = 19
	add(t, aStore, kMachine1, 233.0, &aMetric, false)

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 130.0, 130.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	assertValueEquals(t, true, session.IsNew())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 130.0, 131.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	assertValueEquals(t, false, session.IsNew())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 124.0, 124.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	result = nil
	aStore.ByPrefixAndMachine(
		session, "/foo/bar", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	// Now we should get 1 from foo/bar and one from foo/baz.
	result = nil
	aStore.ByPrefixAndMachine(
		session, "/foo/ba", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))
	assertValueEquals(t, 2, len(result))

	// Now we should get nothing
	result = nil
	aStore.ByPrefixAndMachine(
		session, "/foo/bat", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))
	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 90.0, 100.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/notthere", kMachine0, 100.0, 130.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 0.0, 124.0, store.AppendTo(
			&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 112.0, result[0].Timestamp())
	assertValueEquals(t, 4, result[0].Value())
	assertValueEquals(t, 100.0, result[1].Timestamp())
	assertValueEquals(t, 0, result[1].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/baz", kMachine0, 0, 1000.0, store.AppendTo(
			&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 127.0, result[0].Timestamp())
	assertValueEquals(t, 9, result[0].Value())
	assertValueEquals(t, 115.0, result[1].Timestamp())
	assertValueEquals(t, 5, result[1].Value())
	assertValueEquals(t, 103.0, result[2].Timestamp())
	assertValueEquals(t, 1, result[2].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine1, 0, 1000.0, store.AppendTo(
			&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 224.0, result[0].Timestamp())
	assertValueEquals(t, 18, result[0].Value())
	assertValueEquals(t, 212.0, result[1].Timestamp())
	assertValueEquals(t, 14, result[1].Value())
	assertValueEquals(t, 200.0, result[2].Timestamp())
	assertValueEquals(t, 10, result[2].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/baz", kMachine1, 0, 1000.0, store.AppendTo(
			&result))

	assertValueEquals(t, 3, len(result))
	assertValueEquals(t, 227.0, result[0].Timestamp())
	assertValueEquals(t, 19, result[0].Value())
	assertValueEquals(t, 215.0, result[1].Timestamp())
	assertValueEquals(t, 15, result[1].Value())
	assertValueEquals(t, 203.0, result[2].Timestamp())
	assertValueEquals(t, 11, result[2].Value())

	result = nil
	aStore.ByMachine(session, kMachine1, 0, 1000.0, store.AppendTo(&result))
	assertValueEquals(t, 6, len(result))
	var barIdx, bazIdx int
	if result[0].Info().Path() == "/foo/baz" {
		barIdx, bazIdx = 3, 0
	} else {
		barIdx, bazIdx = 0, 3
	}
	assertValueEquals(t, 227.0, result[bazIdx+0].Timestamp())
	assertValueEquals(t, 19, result[bazIdx+0].Value())
	assertValueEquals(t, 215.0, result[bazIdx+1].Timestamp())
	assertValueEquals(t, 15, result[bazIdx+1].Value())
	assertValueEquals(t, 203.0, result[bazIdx+2].Timestamp())
	assertValueEquals(t, 11, result[bazIdx+2].Value())

	assertValueEquals(t, 224.0, result[barIdx+0].Timestamp())
	assertValueEquals(t, 18, result[barIdx+0].Value())
	assertValueEquals(t, 212.0, result[barIdx+1].Timestamp())
	assertValueEquals(t, 14, result[barIdx+1].Value())
	assertValueEquals(t, 200.0, result[barIdx+2].Timestamp())
	assertValueEquals(t, 10, result[barIdx+2].Value())

	result = nil
	aStore.ByMachine(session, kMachine1, 203.0, 224.0, store.AppendTo(&result))
	assertValueEquals(t, 4, len(result))
	if result[0].Info().Path() == "/foo/baz" {
		barIdx, bazIdx = 2, 0
	} else {
		barIdx, bazIdx = 0, 2
	}
	assertValueEquals(t, 215.0, result[bazIdx+0].Timestamp())
	assertValueEquals(t, 15, result[bazIdx+0].Value())
	assertValueEquals(t, 203.0, result[bazIdx+1].Timestamp())
	assertValueEquals(t, 11, result[bazIdx+1].Value())

	assertValueEquals(t, 212.0, result[barIdx+0].Timestamp())
	assertValueEquals(t, 14, result[barIdx+0].Value())
	assertValueEquals(t, 200.0, result[barIdx+1].Timestamp())
	assertValueEquals(t, 10, result[barIdx+1].Value())

	session.Close()
	session = aStore.NewSession()

	// Now add 2 more values. Doing this should evict
	// 2 of the three /foo/bar values on machine0 leaving only
	// t=124, value=8 for /foo/bar.
	// t=112, value=4 should be in the earliest records index.
	aMetric.Path = "/foo/baz"
	aMetric.Value = 13
	add(t, aStore, kMachine0, 139.0, &aMetric, true)
	aMetric.Value = 15
	add(t, aStore, kMachine0, 145.0, &aMetric, true)

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 123.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())
	assertValueEquals(t, 112.0, result[1].Timestamp())
	assertValueEquals(t, 4, result[1].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 100.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 2, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())
	assertValueEquals(t, 112.0, result[1].Timestamp())
	assertValueEquals(t, 4, result[1].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 100.0, 124.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 112.0, result[0].Timestamp())
	assertValueEquals(t, 4, result[0].Value())

	// We need to verify that we don't overwrite earlist value returned
	keep := result

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 100.0, 113.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 112.0, result[0].Timestamp())
	assertValueEquals(t, 4, result[0].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 100.0, 112.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	// Add one more foo/baz value to evict the last /foo/bar value.
	// Now the earliest record for /foo/bar should be
	// timestamp = 124, value = 8 but this shouldn't affect previous
	// result sets as long as we have the opened session.
	aMetric.Path = "/foo/baz"
	aMetric.Value = 17
	add(t, aStore, kMachine0, 151.0, &aMetric, true)

	assertValueEquals(t, 1, len(keep))
	assertValueEquals(t, 112.0, keep[0].Timestamp())
	assertValueEquals(t, 4, keep[0].Value())

	// Now test searching for "/foo/bar" when there are no records for it
	// in circular queue only earliest known record.
	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 125.0, 126.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 124.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 123.0, 125.0, store.AppendTo(
			&result))

	assertValueEquals(t, 1, len(result))
	assertValueEquals(t, 124.0, result[0].Timestamp())
	assertValueEquals(t, 8, result[0].Value())

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/bar", kMachine0, 123.0, 124.0, store.AppendTo(
			&result))

	assertValueEquals(t, 0, len(result))

	session.Close()
	session = aStore.NewSession()

	// Now test get latest metrics. For /foo/bar there are no values in
	// the circular buffer, only the earlist known value.
	result = nil
	aStore.LatestByMachine(session, kMachine0, store.AppendTo(&result))
	assertValueEquals(t, 2, len(result))
	if result[0].Info().Path() == "/foo/bar" {
		barIdx, bazIdx = 0, 1
	} else {
		barIdx, bazIdx = 1, 0
	}
	assertValueEquals(t, 124.0, result[barIdx].Timestamp())
	assertValueEquals(t, 8, result[barIdx].Value())
	assertValueEquals(t, 151.0, result[bazIdx].Timestamp())
	assertValueEquals(t, 17, result[bazIdx].Value())

	// Now add "foo/baz" values but change the "foo/baz" metric so
	// that it is different.
	aMetric.Path = "/foo/baz"
	aMetric.Bits = 32
	aMetric.Value = 29
	add(t, aStore, kMachine0, 145.0, &aMetric, true)

	result = nil
	aStore.ByNameAndMachine(
		session, "/foo/baz", kMachine0, 144.0, 152.0, store.AppendTo(
			&result))

	// Results grouped by metric first then by timestamp
	assertValueEquals(t, 4, len(result))
	var bits32, bits64 int
	if result[0].Info().Bits() == 32 {
		bits32, bits64 = 0, 1
	} else {
		bits32, bits64 = 3, 0
	}
	assertValueEquals(t, 145.0, result[bits32].Timestamp())
	assertValueEquals(t, 29, result[bits32].Value())
	assertValueEquals(t, 151.0, result[bits64+0].Timestamp())
	assertValueEquals(t, 17, result[bits64+0].Value())
	assertValueEquals(t, 145.0, result[bits64+1].Timestamp())
	assertValueEquals(t, 15, result[bits64+1].Value())
	assertValueEquals(t, 139.0, result[bits64+2].Timestamp())
	assertValueEquals(t, 13, result[bits64+2].Value())

	session.Close()

}

func add(
	t *testing.T,
	astore *store.Store,
	machineId *scotty.Machine,
	ts float64,
	m *messages.Metric,
	success bool) {
	if astore.Add(machineId, ts, m) != success {
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
