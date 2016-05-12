// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

// Commonly used keys in TagGroup instances
const (
	TagAppName = "appname"
)

// TagGroup represents arbitrary key-value pairs describing a metric.
// Clients are to treat TagGroup instances as immutable.
type TagGroup map[string]string

// Record represents one value of one metric in persistent storage.
type Record struct {
	// Originating machine
	HostName string
	// Path of metric
	Path string
	// Arbitrary key-value pairs describing this metric
	Tags TagGroup
	// Kind of metric
	Kind types.Type
	// Unit of metric
	Unit units.Unit
	// Value of metric
	Value interface{}
	// The timestamp of the metric value.
	Timestamp time.Time
}

// RecordWriter is the interface for writing to persistent store.
// Implementations of RecordWriter must be safe to use with multiple goroutines.
type RecordWriter interface {
	// Write writes given collection of records to persistent storage
	Write(records []Record) error
}

// LimitedRecordWriter is a RecordWriter which provides information on what
// types of values it can write.
type LimitedRecordWriter interface {
	RecordWriter
	// IsTypeSupported returns true if this writer supports metrics
	// of a particular kind.
	IsTypeSupported(kind types.Type) bool
}

// RecordWriterMetrics represents writing metrics
type RecordWriterMetrics struct {
	ValuesWritten    uint64
	WriteAttempts    uint64
	SuccessfulWrites uint64
	LastWriteError   string
	TimeSpentWriting time.Duration
}

func (w *RecordWriterMetrics) SuccessfulWriteRatio() float64 {
	return float64(w.SuccessfulWrites) / float64(w.WriteAttempts)
}

// RecordWriterWithMetrics implements RecordWriter and provides metrics
type RecordWriterWithMetrics struct {
	// Client must provide the underlying writer
	W RecordWriter
	// Client populates this to collect write times per metric
	PerMetricWriteTimes *tricorder.CumulativeDistribution
	// Client populates this to collect batch sizes
	BatchSizes *tricorder.CumulativeDistribution
	lock       sync.Mutex
	metrics    RecordWriterMetrics
}

func (w *RecordWriterWithMetrics) Write(records []Record) error {
	return w.write(records)
}

// Metrics stores the current metrics at m
func (w *RecordWriterWithMetrics) Metrics(m *RecordWriterMetrics) {
	w._metrics(m)
}

// AsyncConsumer works like Consumer but does the work asynchronously.
//
// Although AsyncConsumer may be used with multiple goroutines, we do not
// recommend doing so to ensure a clear happens before relationship between
// calls to WriteAsync and Flush.
type AsyncConsumer struct {
	requests     chan consumerRequestType
	flushBarrier *barrier
	concurrency  int
}

// NewAsyncConsumer creates a new AsyncConsumer instance.
// w is the underlying writer.
// concurrency is the count of goroutines doing the consuming.
// bufferSize is the size of each buffer. Each goroutine gets its own buffer.
func NewAsyncConsumer(
	w RecordWriter, bufferSize, concurrency int) *AsyncConsumer {
	return newAsyncConsumer(w, bufferSize, concurrency)
}

// WriteAsync works like the Consumer.Write except that it returns immediately.
// Sometime in the future, a separate goroutine does the work of consuming
// values from n, writing them to the underlying RecordWriter and committing
// progress on n.
//
// Like Consumer.Write, caller must avoid creating and using another
// NamedIterator instance with the same name iterating over the same values
// until it has called Flush on this instance.
func (a *AsyncConsumer) WriteAsync(
	n store.NamedIterator, host, appName string) {
	a.writeAsync(n, host, appName)
}

// Flush works like Consumer.Flush plus Flush waits until every
// call to WriteAsync that happened before resolves before flushing the
// buffers. Flush blocks the caller until it has completed flushing all the
// buffers and committing all necessary NamedIterators.
func (a *AsyncConsumer) Flush() {
	a.flush()
}

// Consumer writes values from NamedIterator instances to persistent storage.
// Consumer buffers values to be written. Whenever the buffer becomes full,
// the Consumer instance clears the buffer, writes the values out to the
// underlying writer, and commits progress on the corresponding
// NamedIterator instances.
// Multiple goroutines cannot share the same Consumer instance.
type Consumer struct {
	w             RecordWriter
	buffer        []Record
	toBeCommitted map[store.NamedIterator]bool
	idx           int
}

// NewConsumer creates a new Consumer instance. w is the underlying writer.
// bufferSize is how many values the buffer holds.
func NewConsumer(w RecordWriter, bufferSize int) *Consumer {
	return newConsumer(w, bufferSize)
}

// Write consumes all the values from n and stores them in this instance's
// buffer writing the values out to the underlying writer whenever the buffer
// becomes full.
//
// When Write returns, either Write has consumed all the values from n or an
// error happened writing values to the underlying writer.
//
// If Write consumes all the values from n, it returns nil. If an error happens
// writing values out to the underlying writer, Write returns that error and
// quits consuming values from n. When this happens, Write clears the buffer
// but does not commit progress on any of the NamedIterator instances
// corresponding to the values that were in the buffer.

// host and appName are the host and application name for the values in n.
//
// When the caller passes a NamedIterator instance to Write, this instance
// holds onto that NamedIterator until either its values are written out to
// the underlying RecordWriter or an error happens. Therefore, the caller should
// avoid creating and using another NamedIterator instance with the same name
// iterating over the same values until it has called Flush on this instance.
func (c *Consumer) Write(
	n store.NamedIterator, host, appName string) error {
	return c.write(n, host, appName)
}

// Flush writes any pending values out to the underlying writer committing
// progress on the corresponding NamedIterator instances.
//
// After calling Flush, the client can safely assume that this instance is
// not holding onto any NamedIterator instances.
//
// If an error happens writing out pending values, Flush returns that error
// and does not commit progress on the corresponding NamedIterator instances.
func (c *Consumer) Flush() error {
	return c.flush()
}
