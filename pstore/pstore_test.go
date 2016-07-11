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
	kDescription    = "A Description"
	kInt64Path      = "/some/int64/path"
	kStringPath     = "/some/string/path"
	kListInt64Path  = "/some/list/int64/path"
	kListStringPath = "/some/list/string/path"
)

var (
	kNow = time.Date(2016, 6, 7, 13, 25, 0, 0, time.Local)
)

var (
	kInt64MetricInfo = (&store.MetricInfoBuilder{
		Description: kDescription,
		Kind:        types.Int64,
		Path:        kInt64Path,
		Unit:        units.None,
	}).Build()
	kStringMetricInfo = (&store.MetricInfoBuilder{
		Description: kDescription,
		Kind:        types.String,
		Path:        kStringPath,
		Unit:        units.None,
	}).Build()
	kListInt64MetricInfo = (&store.MetricInfoBuilder{
		Description: kDescription,
		Kind:        types.List,
		SubType:     types.Int64,
		Path:        kListInt64Path,
		Unit:        units.None,
	}).Build()
	kListStringMetricInfo = (&store.MetricInfoBuilder{
		Description: kDescription,
		Kind:        types.List,
		SubType:     types.String,
		Path:        kListInt64Path,
		Unit:        units.None,
	}).Build()
	kWriteError = errors.New("An error")
)

func TestSubTypes(t *testing.T) {
	writer := &noListStringWriterType{}
	builder := pstore.NewConsumerWithMetricsBuilder(writer)
	consumer := builder.Build()

	// list int64 values that can be written to pstore
	listInt64Iterator := newNamedIteratorSameValueType(
		"listInt64Iterator",
		3,
		[]int64{},
		kListInt64MetricInfo)
	// list string values that cannot be written to pstore
	listStringIterator := newNamedIteratorSameValueType(
		"listStringIterator",
		2,
		[]string{},
		kListStringMetricInfo)
	// Final iterator yields 3 list int64 values
	// followed by 2 list string values.
	iterator := compoundIteratorType{
		listInt64Iterator, listStringIterator}

	consumer.Write(iterator, "someHost", "someApp")
	consumer.Flush()

	// three values should be written
	if assertValueEquals(t, 3, len(writer.Written)) {
		for i := 0; i < 3; i++ {
			assertValueEquals(
				t, types.List, writer.Written[i].Kind)
			assertValueEquals(
				t, types.Int64, writer.Written[i].SubType)
		}
	}
}

// Test that consumer exhausts iterators even in the corner case where
// the write buffer fills up and all that is left on the iterator are
// types the writer doesn't support.
func TestBug1586(t *testing.T) {
	builder := pstore.NewConsumerWithMetricsBuilder(
		noStringsNilWriterType{})
	// Buffer size and number of int64 values have to match.
	builder.SetBufferSize(20)
	consumer := builder.Build()

	// 20 int64 values that can be written to pstore
	int64Iterator := newNamedIteratorSameValueType(
		"int64Iterator", 20, int64(37), kInt64MetricInfo)
	// 5 string values that cannot be written to pstore
	stringIterator := newNamedIteratorSameValueType(
		"stringIterator", 5, "foo", kStringMetricInfo)
	// Final iterator yields 20 int64 values followed by 5 string values.
	iterator := compoundIteratorType{
		int64Iterator, stringIterator}

	consumer.Write(iterator, "someHost", "someApp")
	consumer.Flush()

	// Writing to the consumer should exhaust all the values in the
	// iterator including the string values left over after the buffer
	// fills.
	int64Iterator.VerifyExhausted(t)
	stringIterator.VerifyExhausted(t)
}

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

	// Iterators involved in failure before remain uncommitted.
	threeIterator.VerifyNoCommits(t)
	fourIterator.VerifyNoCommits(t)

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
			if actual := nextWrite[0].Path; kInt64Path != actual {
				t.Errorf("Expected path %s, got %s", kInt64Path, actual)
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
	r.Info = kInt64MetricInfo
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

type namedIteratorSameValueType struct {
	name            string
	numToWrite      int
	numWritten      int
	value           interface{}
	info            *store.MetricInfo
	lastCommitPoint int
}

func newNamedIteratorSameValueType(
	name string,
	count int,
	value interface{},
	info *store.MetricInfo) *namedIteratorSameValueType {
	return &namedIteratorSameValueType{
		name:       name,
		numToWrite: count,
		value:      value,
		info:       info}
}

func (n *namedIteratorSameValueType) VerifyExhausted(t *testing.T) {
	if n.lastCommitPoint != n.numToWrite {
		t.Errorf("Iterator %s not exhausted", n.Name())
	}
}

func (n *namedIteratorSameValueType) Next(r *store.Record) bool {
	if n.numWritten == n.numToWrite {
		return false
	}
	r.Info = n.info
	r.Value = n.value
	r.TimeStamp = duration.TimeToFloat(kNow)
	r.Active = true
	n.numWritten++
	return true
}

func (n *namedIteratorSameValueType) Name() string {
	return n.name
}

func (n *namedIteratorSameValueType) Commit() {
	n.lastCommitPoint = n.numWritten
}

type compoundIteratorType []store.NamedIterator

func (c compoundIteratorType) Next(r *store.Record) bool {
	for _, iter := range c {
		if iter.Next(r) {
			return true
		}
	}
	return false
}

func (c compoundIteratorType) Name() string {
	return "compound"
}

func (c compoundIteratorType) Commit() {
	for _, iter := range c {
		iter.Commit()
	}
}

type noStringsNilWriterType struct {
}

func (w noStringsNilWriterType) Write(records []pstore.Record) error {
	return nil
}

func (w noStringsNilWriterType) IsTypeSupported(kind types.Type) bool {
	return kind != types.String
}

type noListStringWriterType struct {
	Written []pstore.Record
}

func (w *noListStringWriterType) Write(records []pstore.Record) error {
	w.Written = append(w.Written, records...)
	return nil
}

func (w *noListStringWriterType) IsTypeSupported(kind types.Type) bool {
	return w.IsTypeAndSubTypeSupported(kind, types.Unknown)
}

func (w *noListStringWriterType) IsTypeAndSubTypeSupported(
	kind, subType types.Type) bool {
	return kind != types.List || subType != types.String
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

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}
