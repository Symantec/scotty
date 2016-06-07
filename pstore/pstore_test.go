package pstore_test

import (
	"errors"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"reflect"
	"testing"
	"time"
)

const (
	kDescription = "A Description"
	kPath        = "/some/path"
)

var (
	kNow = time.Date(2016, 6, 7, 13, 25, 0, 0, time.Local)
)

var (
	kMetricInfo = (&store.MetricInfoBuilder{
		Bits:        64,
		Description: kDescription,
		Kind:        types.Int64,
		Path:        kPath,
		Unit:        units.None,
	}).Build()
	kWriteError = errors.New("An error")
)

func TestConsumer(t *testing.T) {
	baseWriter := newBaseWriterForTesting()
	// Buffer size 7
	consumer := pstore.NewConsumer(baseWriter, 7)
	// Five values
	fiveIterator := newNamedIteratorForTesting(5)
	// Ten values
	tenIterator := newNamedIteratorForTesting(10)
	assertSuccess(t, consumer.Write(
		fiveIterator, "fiveHost", "fiveApp"))
	// Buffer not full yet, no writes and no commits
	fiveIterator.VerifyNoCommits(t)
	baseWriter.VerifyNoMoreWrites(t)
	assertSuccess(t, consumer.Write(
		tenIterator, "tenHost", "tenApp"))
	// This iterator committed
	fiveIterator.VerifyCommitPoints(t, 5)
	// This iterator commited after 2 and 9 writes.
	tenIterator.VerifyCommitPoints(t, 2, 9)
	// First write has all 5 from five iterator and first two from ten
	// iterator
	baseWriter.VerifyWrite(
		t, 0, 5, "fiveHost", "fiveApp", 0, 2, "tenHost", "tenApp")
	// Next write has 7 values from ten iterator
	baseWriter.VerifyWrite(
		t, 2, 9, "tenHost", "tenApp")
	baseWriter.VerifyNoMoreWrites(t)
	oneIterator := newNamedIteratorForTesting(1)
	fourteenIterator := newNamedIteratorForTesting(14)
	// Throw error on second write
	baseWriter.ThrowErrorOnNthWrite(2)
	assertSuccess(t, consumer.Write(
		oneIterator, "oneHost", "oneApp"))
	assertFailure(t, consumer.Write(
		fourteenIterator, "fourteenHost", "fourteenApp"))
	tenIterator.VerifyCommitPoints(t, 2, 9, 10)
	oneIterator.VerifyCommitPoints(t, 1)
	// This iterator committed up to last successful write only
	fourteenIterator.VerifyCommitPoints(t, 5)
	baseWriter.VerifyWrite(
		t, 9, 10, "tenHost", "tenApp",
		0, 1, "oneHost", "oneApp",
		0, 5, "fourteenHost", "fourteenApp",
	)
	baseWriter.VerifyNoMoreWrites(t)
	baseWriter.ThrowErrorOnNthWrite(1)
	threeIterator := newNamedIteratorForTesting(3)
	fourIterator := newNamedIteratorForTesting(4)
	// Succeeds since buffer not yet full
	assertSuccess(t, consumer.Write(
		threeIterator, "threeHost", "threeApp"))
	// Fails because buffer is full and the write fails
	assertFailure(t, consumer.Write(
		fourIterator, "fourHost", "fourApp"))
	// These two iterators not committed because of failure
	threeIterator.VerifyNoCommits(t)
	fourIterator.VerifyNoCommits(t)
	sixIterator := newNamedIteratorForTesting(6)
	assertSuccess(t, consumer.Write(
		sixIterator, "sixHost", "sixApp"))
	sixIterator.VerifyNoCommits(t)
	baseWriter.VerifyNoMoreWrites(t)
	assertSuccess(t, consumer.Flush())
	sixIterator.VerifyCommitPoints(t, 6)
	baseWriter.VerifyWrite(t, 0, 6, "sixHost", "sixApp")
	baseWriter.VerifyNoMoreWrites(t)
}

type baseWriterForTestingType struct {
	written                [][]pstore.Record
	writesLeftToThrowError int
}

func newBaseWriterForTesting() *baseWriterForTestingType {
	return &baseWriterForTestingType{}
}

func (w *baseWriterForTestingType) VerifyNoMoreWrites(t *testing.T) {
	if len(w.written) > 0 {
		t.Error("No more writes expected.")
	}
	w.written = w.written[:0]
}

func (w *baseWriterForTestingType) VerifyWrite(
	t *testing.T, startEndHostPort ...interface{}) {
	length := len(w.written)
	if length == 0 {
		t.Error("Expected another write but no writes.")
		return
	}
	args := startEndHostPort
	nextWrite := w.written[0]
	copy(w.written, w.written[1:])
	w.written = w.written[:length-1]
	for len(args) > 0 {
		start := args[0].(int)
		end := args[1].(int)
		host := args[2].(string)
		app := args[3].(string)
		args = args[4:]
		for i := start; i < end; i++ {
			if len(nextWrite) == 0 {
				t.Error("Still expecting records in current write, but no more.")
				continue
			}
			if actual := nextWrite[0].HostName; host != actual {
				t.Errorf("Expected host %s, got %s", host, actual)
			}
			if actual := nextWrite[0].Path; kPath != actual {
				t.Errorf("Expected path %s, got %s", kPath, actual)
			}
			if actual := nextWrite[0].Tags[pstore.TagAppName]; app != actual {
				t.Errorf("Expected app %s, got %s", app, actual)
			}
			if actual := nextWrite[0].Kind; types.Int64 != actual {
				t.Errorf("Expected type %s, got %s", types.Int64, actual)
			}
			if actual := nextWrite[0].Unit; units.None != actual {
				t.Errorf("Expected unit %s, got %s", units.None, actual)
			}
			if actual := nextWrite[0].Value; int64(i) != actual {
				t.Errorf("Expected value %d, got %d", i, actual)
			}
			if actual := nextWrite[0].Timestamp; kNow != actual {
				t.Errorf("Expected timestamp %v, got %v", kNow, actual)
			}
			nextWrite = nextWrite[1:]
		}
	}
	if len(nextWrite) > 0 {
		t.Error("Expecting no more records in current write, but still some left")
	}
}

func (w *baseWriterForTestingType) ThrowErrorOnNthWrite(n int) {
	w.writesLeftToThrowError = n
}

func (w *baseWriterForTestingType) Write(records []pstore.Record) error {
	if w.writesLeftToThrowError > 0 {
		w.writesLeftToThrowError--
		if w.writesLeftToThrowError == 0 {
			return kWriteError
		}
	}
	nextWrite := make([]pstore.Record, len(records))
	copy(nextWrite, records)
	w.written = append(w.written, nextWrite)
	return nil
}

type namedIteratorForTestingType struct {
	numToWrite   int
	numWritten   int
	commitPoints []int
}

func newNamedIteratorForTesting(count int) *namedIteratorForTestingType {
	return &namedIteratorForTestingType{numToWrite: count}
}

func (n *namedIteratorForTestingType) VerifyCommitPoints(
	t *testing.T, points ...int) {
	if !reflect.DeepEqual(points, n.commitPoints) {
		t.Errorf(
			"Commit points: expected %v, got %v",
			points,
			n.commitPoints)
	}
}

func (n *namedIteratorForTestingType) VerifyNoCommits(t *testing.T) {
	n.VerifyCommitPoints(t)
}

func (n *namedIteratorForTestingType) Next(r *store.Record) bool {
	if n.numWritten == n.numToWrite {
		return false
	}
	r.Info = kMetricInfo
	r.Value = int64(n.numWritten)
	r.TimeStamp = duration.TimeToFloat(kNow)
	r.Active = true
	n.numWritten++
	return true
}

func (n *namedIteratorForTestingType) Name() string {
	return "abc"
}

func (n *namedIteratorForTestingType) Commit() {
	n.commitPoints = append(n.commitPoints, n.numWritten)
}

func assertSuccess(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected success, got %v", err)
	}
}

func assertFailure(t *testing.T, err error) {
	if err == nil {
		t.Error("Expected failure")
	}
}
