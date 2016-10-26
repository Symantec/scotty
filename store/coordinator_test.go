package store_test

import (
	"github.com/Symantec/scotty/store"
	"testing"
)

type fakeTsIterator struct {
	Current float64
	End     float64
	Incr    float64
}

func (f *fakeTsIterator) Next(rec *store.Record) bool {
	if f.Current >= f.End {
		return false
	}
	rec.TimeStamp = f.Current
	f.Current += f.Incr
	return true
}

func (f *fakeTsIterator) Name() string {
	return "foo"
}

func (f *fakeTsIterator) Commit() {
	// do nothing
}

type fakeCoordinator struct {
	start     float64
	end       float64
	nextStart float64
	count     int
}

func (f *fakeCoordinator) Lease(
	leaseSpanInSeconds float64, timeToInclude float64) (float64, float64) {
	f.count++
	if timeToInclude >= f.start && timeToInclude < f.end {
		return f.start, f.end
	}
	f.start = f.nextStart
	f.end = timeToInclude + leaseSpanInSeconds
	if f.end < f.start+leaseSpanInSeconds {
		f.end = f.start + leaseSpanInSeconds
	}
	return f.start, f.end
}

func (f *fakeCoordinator) SetNextStart(start float64) {
	f.nextStart = start
}

func (f *fakeCoordinator) Verify(
	t *testing.T, count int, start, end float64) {
	if count != f.count {
		t.Errorf("Expected %d leases, got %d", count, f.count)
	}
	if f.start != start || f.end != end {
		t.Errorf("Expected lease (%v, %v), got (%v, %v)", start, end, f.start, f.end)
	}
}

func runIteration(
	t *testing.T,
	iter store.Iterator,
	expectedValues ...float64) {
	for _, want := range expectedValues {
		var rec store.Record
		if !iter.Next(&rec) {
			t.Fatal("Unexpected end of iterator")
		}
		got := rec.TimeStamp
		if want != got {
			t.Errorf("Want %v, got %v", want, got)
		}
	}
}

func TestCoordinate(t *testing.T) {
	var iter store.NamedIterator = &fakeTsIterator{
		Current: 200.0,
		End:     500.0,
		Incr:    10.0,
	}
	coord := &fakeCoordinator{}
	var skipped uint64
	updateSkipped := func(x uint64) {
		skipped += x
	}
	// Leases go up in 30 second increments
	iter = store.NamedIteratorCoordinate(iter, coord, 30.0, updateSkipped)
	runIteration(t, iter, 200.0, 210.0, 220.0)
	// Verify we are on 1st lease and it goes from 0 to 230
	coord.Verify(t, 1, 0.0, 230.0)
	runIteration(t, iter, 230.0, 240.0, 250.0, 260.0)
	// Verify we are on 3rd lease and it goes from 0 to 290
	coord.Verify(t, 3, 0.0, 290.0)
	iter.Commit()
	if skipped != 0 {
		t.Errorf("Expected 0 skipped, got %d", skipped)
	}
	coord.SetNextStart(340.0)
	// Even though we are on new start, current lease is still valid.
	runIteration(t, iter, 270.0, 280.0)
	coord.Verify(t, 3, 0.0, 290.0)
	// Now we jump to 340
	runIteration(t, iter, 340.0)
	// skip isn't updated until after we commit
	if skipped != 0 {
		t.Errorf("Expected 0 skipped, got %d", skipped)
	}
	iter.Commit()
	if skipped != 5 {
		t.Errorf("Expected 5 skipped, got %d", skipped)
	}
	// 4th lease goes from 340 to 370
	coord.Verify(t, 4, 340.0, 370.0)
	runIteration(t, iter, 350.0, 360.0, 370.0)
	// 5th lease runs to 400
	coord.Verify(t, 5, 340.0, 400.0)
	coord.SetNextStart(600.0)
	runIteration(t, iter, 380.0, 390)
	coord.Verify(t, 5, 340.0, 400.0)
	var rec store.Record
	if iter.Next(&rec) {
		t.Error("Exected no more, but got some")
	}
	coord.Verify(t, 6, 600.0, 630.0)
	// Skip not updated until after we commit
	if skipped != 5 {
		t.Errorf("Expected 5 skipped, got %d", skipped)
	}
	iter.Commit()
	// Our iterator goes to 500 so we skipped last 10 making total skipped
	// be 15.
	if skipped != 15 {
		t.Errorf("Expected 15 skipped, got %d", skipped)
	}
}
