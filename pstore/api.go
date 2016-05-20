// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"bytes"
	"fmt"
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

func (t TagGroup) String() string {
	buffer := &bytes.Buffer{}
	fmt.Fprintf(buffer, "{")
	firstTime := true
	for k, v := range t {
		if firstTime {
			fmt.Fprintf(buffer, "%s: %s", k, v)
			firstTime = false
		} else {
			fmt.Fprintf(buffer, ", %s: %s", k, v)
		}
	}
	fmt.Fprintf(buffer, "}")
	return buffer.String()
}

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

func (r Record) String() string {
	return fmt.Sprintf("{HostName: %s, Path: %s, Tags: %v, Kind: %v, Unit: %v, Value: %v, Timestamp: %v}", r.HostName, r.Path, r.Tags, r.Kind, r.Unit, r.Value, r.Timestamp)
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

// ConsumerMetrics represents metrics for a consumer.
type ConsumerMetrics struct {
	RecordWriterMetrics
	// The number of values this consumer has yet to write out.
	ValuesNotWritten uint64
}

// ConsumerMetricsStore stores metrics for a consumer.
// ConsumerMetricStore instances are safe to use with multiple goroutines.
type ConsumerMetricsStore struct {
	w           *RecordWriterWithMetrics
	filterer    store.Filterer
	lock        sync.Mutex
	recordCount uint64
}

// Adds count to the total number of records consumer must write out.
func (s *ConsumerMetricsStore) AddToRecordCount(count uint64) {
	s.addToRecordCount(count)
}

// Returns true if consumer is to write out this record or false otherwise.
func (s *ConsumerMetricsStore) Filter(r *store.Record) bool {
	return s.filterer.Filter(r)
}

// Metrics writes the consumer's metrics to m.
func (s *ConsumerMetricsStore) Metrics(m *ConsumerMetrics) {
	s.metrics(m)
}

// ConsumerMetricsStoreList represents an immutable slice of
// ConsumerMetricStore instances.
// ConsumerMetricStoreList instances are safe to use with multiple goroutines.
type ConsumerMetricsStoreList []*ConsumerMetricsStore

// UpdateCounts updates the total record count for all consumers.
// UpdateCount consumes all values from n and commits n.
func (s ConsumerMetricsStoreList) UpdateCounts(n store.NamedIterator) {
	s.updateCounts(n)
}

// ConsumerWithMetrics instances work like Consumer instances but also have
// metrics.
// Like Consumer instances, ConsumerWithMetric instances are NOT safe to use
// with multiple goroutines.
type ConsumerWithMetrics struct {
	metricsStore *ConsumerMetricsStore
	consumer     consumerType
}

// MetricsStore returns the ConsumerMetricsStore for this instance.
func (c *ConsumerWithMetrics) MetricsStore() *ConsumerMetricsStore {
	return c.metricsStore
}

// Write works like Consumer.Write but does not return an error.
func (c *ConsumerWithMetrics) Write(
	n store.NamedIterator, host, appName string) {
	c.consumer.Write(
		store.NamedIteratorFilter(n, c.metricsStore.filterer),
		host,
		appName)
}

// Flush works like Consumer.Flush but does not return an error.
func (c *ConsumerWithMetrics) Flush() {
	c.consumer.Flush()
}

// ConsumerWithMetricsBuilder builds a ConsumerWithMetrics instance.
// Each instance is good for building one and only one ConsumerWithMetrics
// instance.
// These instances are NOT safe to use with multiple goroutines.
type ConsumerWithMetricsBuilder struct {
	c          **ConsumerWithMetrics
	bufferSize int
}

// NewConsumerWithMetricsBuilder creates a new instance that will
// build a consumer that uses w to write values out.
func NewConsumerWithMetricsBuilder(
	w LimitedRecordWriter) *ConsumerWithMetricsBuilder {
	return newConsumerWithMetricsBuilder(w)
}

// SetBufferSize sets how many values the consumer will buffer before
// writing them out. The default is 1000.
func (b *ConsumerWithMetricsBuilder) SetBufferSize(size int) {
	b.bufferSize = size
}

// SetPerMetricWriteTimeDist sets the distribution that the consumer will
// use to record write times. The default is not to record write times.
func (b *ConsumerWithMetricsBuilder) SetPerMetricWriteTimeDist(
	d *tricorder.CumulativeDistribution) {
	(*b.c).metricsStore.w.PerMetricWriteTimes = d
}

// SetPerMetricBatchSizeDist sets the distribution that the consumer will
// use to record the batch size of values written out.
// The default is not to record batch sizes.
func (b *ConsumerWithMetricsBuilder) SetPerMetricBatchSizeDist(
	d *tricorder.CumulativeDistribution) {
	(*b.c).metricsStore.w.BatchSizes = d
}

// Build builds the ConsumerWithMetrics instance and destroys this builder.
func (b *ConsumerWithMetricsBuilder) Build() *ConsumerWithMetrics {
	return b.build()
}

// ConsumerWithMetricsBuilderList represents an immutable slice of builders
type ConsumerWithMetricsBuilderList []*ConsumerWithMetricsBuilder

// SetBufferSize sets the buffer size on all builders in this slice.
func (b ConsumerWithMetricsBuilderList) SetBufferSize(size int) {
	for i := range b {
		b[i].SetBufferSize(size)
	}
}
