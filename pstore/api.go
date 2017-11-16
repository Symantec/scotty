// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/Dominator/lib/log/nulllogger"
	"github.com/Symantec/scotty/lib/gate"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"regexp"
	"sync"
	"time"
)

// Commonly used keys in TagGroup instances
const (
	TagAppName    = "appname"
	TagRegionName = "region"
	TagIpAddress  = "ipaddress"
)

var (
	ErrDisabled = errors.New("pstore: Writer disabled.")
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
	// Subtype of metric
	SubType types.Type
	// Unit of metric
	Unit units.Unit
	// Value of metric
	Value interface{}
	// The timestamp of the metric value.
	Timestamp time.Time
}

func (r Record) String() string {
	return fmt.Sprintf("{HostName: %s, Path: %s, Tags: %v, Kind: %v, SubType: %v, Unit: %v, Value: %v, Timestamp: %v}", r.HostName, r.Path, r.Tags, r.Kind, r.SubType, r.Unit, r.Value, r.Timestamp)
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

// LimitedBySubTypeRecordWriter is a sub interface of LimitedRecordWriter
// that allows filtering by both kind and sub-type. For instance, if a
// RecordWriter can write lists of int64s but not lists of strings, it
// should implement this interface.
type LimitedBySubTypeRecordWriter interface {
	// IsTypeSupported(kind) =
	// IsTypeAndSubTypeSupported(kind, types.Unknown)
	LimitedRecordWriter
	// IsTypeAndSubTypeSupported returns true if this writer supports
	// metrics of a particular kind.
	IsTypeAndSubTypeSupported(kind, subType types.Type) bool
}

// RecordWriterMetrics represents writing metrics
type RecordWriterMetrics struct {
	ValuesWritten         uint64
	WriteAttempts         uint64
	SuccessfulWrites      uint64
	LastWriteError        string
	LastWriteErrorTS      time.Time
	LastSuccessfulWriteTS time.Time
	TimeSpentWriting      time.Duration
	Paused                bool
	Disabled              bool
}

func (w *RecordWriterMetrics) SuccessfulWriteRatio() float64 {
	if w.WriteAttempts == 0 {
		return 0.0
	}
	return float64(w.SuccessfulWrites) / float64(w.WriteAttempts)
}

// RecordWriterWithMetrics implements RecordWriter and provides metrics
type RecordWriterWithMetrics struct {
	// Client must provide the underlying writer
	W RecordWriter
	// Client populates this to collect write times per metric
	PerMetricWriteTimes *tricorder.CumulativeDistribution
	// Client populates this to collect batch sizes
	BatchSizes      *tricorder.CumulativeDistribution
	Logger          log.Logger
	criticalSection *gate.Gate
	lock            sync.Mutex
	metrics         RecordWriterMetrics
}

// NewRecordWriterWithMetrics returns a new RecordWriterWithMetrics.
// Use this method instead of initialising RecordWithMetrics directly.
func NewRecordWriterWithMetrics(writer RecordWriter) *RecordWriterWithMetrics {
	return &RecordWriterWithMetrics{
		W:               writer,
		Logger:          nulllogger.New(),
		criticalSection: gate.New(),
	}
}

// SetMetrics sets the metrics in this instance to m but leaves the Paused
// and Disabled metrics in this instance unchanged.
func (w *RecordWriterWithMetrics) SetMetrics(m *RecordWriterMetrics) {
	w.setMetrics(m)
}

func (w *RecordWriterWithMetrics) Write(records []Record) error {
	return w.write(records)
}

// Pause pauses this writer so that subsequent calls to Write block. Any
// in progress calls wo Write will complete before Pause returns.
func (w *RecordWriterWithMetrics) Pause() {
	w.criticalSection.Pause()
	w.setPauseMetric(true)
}

// Resume resumes this writer. Any blocked calls to Write resume immediatley.
func (w *RecordWriterWithMetrics) Resume() {
	w.setPauseMetric(false)
	w.criticalSection.Resume()
}

// Disable disables this writer so that future calls to Write return
// ErrDisabled. Any in progress calls to Write will complete before Disable
// returns. If this writer is currently paused, Disable resumes it so
// that any blocking calls to Write return ErrDisabled. Calling Write on
// a disabled writer does not update any metrics.
func (w *RecordWriterWithMetrics) Disable() {
	w.criticalSection.End()
	w.setDisabledMetric(true)
}

// Metrics stores the current metrics at m
func (w *RecordWriterWithMetrics) Metrics(m *RecordWriterMetrics) {
	w._metrics(m)
}

// AsyncConsumer works like Consumer but does the work asynchronously.
// For certain persistent stores, AsyncConsumer can offer greater
// throughput than Consumer. However unlike Consumer, AsyncConsumer
// methods do not return any errors encountered to the caller. Like
// Consumer, AsyncConsumer is NOT safe to use with multiple goroutines.
type AsyncConsumer struct {
	requests     chan consumerRequestType
	flushBarrier *barrier
	concurrency  uint
}

// NewAsyncConsumer creates a new AsyncConsumer instance.
// w is the underlying writer.
// concurrency is the count of goroutines doing the consuming.
// bufferSize is the size of each buffer. Each goroutine gets its own buffer.
func NewAsyncConsumer(
	w RecordWriter, bufferSize, concurrency uint) *AsyncConsumer {
	return newAsyncConsumer(w, bufferSize, concurrency)
}

// WriteAsync works like the Consumer.Write except that it returns before
// completing the work. Sometime in the future, a separate goroutine does
// the work of consuming values from n, writing them to the underlying
// RecordWriter and committing progress on n.
//
// Like Consumer.Write, caller must avoid creating and using another
// NamedIterator instance with the same name iterating over the same values
// until it has called Flush on this instance.
func (a *AsyncConsumer) WriteAsync(
	n store.NamedIterator, host string, tagGroup TagGroup) {
	a.writeAsync(n, host, tagGroup)
}

// Flush works like Consumer.Flush waiting until previous calls to WriteAsync
// resolve before flushing the buffers. Flush does not return until it has
// completed flushing all the buffers and committing all necessary
// NamedIterator instances.
//
// After calling Flush, the client can safely assume that this instance is
// not holding onto any NamedIterator instances.
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
	toBeCommitted []store.NamedIterator
	idx           int
}

// NewConsumer creates a new Consumer instance. w is the underlying writer.
// bufferSize is how many values the buffer holds.
func NewConsumer(w RecordWriter, bufferSize uint) *Consumer {
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

// host and tagGroup are the host and tag group for the values in n.
//
// When the caller passes a NamedIterator instance to Write, this instance
// holds onto that NamedIterator until either its values are written out to
// the underlying RecordWriter or an error happens. Therefore, the caller should
// avoid creating and using another NamedIterator instance with the same name
// iterating over the same values until it has called Flush on this instance.
func (c *Consumer) Write(
	n store.NamedIterator, host string, tagGroup TagGroup) error {
	return c.write(n, host, tagGroup)
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
	// Total values to write or skip
	TotalValues uint64
	// Values skipped
	SkippedValues uint64
}

// Zero values not written zeros out the values not written by setting
// TotalValues to ValuesWritten and SkippedValues to 0.
func (c *ConsumerMetrics) ZeroValuesNotWritten() {
	c.SkippedValues = 0
	c.TotalValues = c.ValuesWritten
}

// ValuesNotWritten returns the number of values not written
func (c *ConsumerMetrics) ValuesNotWritten() uint64 {
	if c.TotalValues <= c.ValuesWritten+c.SkippedValues {
		return 0
	}
	return c.TotalValues - c.SkippedValues - c.ValuesWritten
}

// ConsumerMetricsStore stores metrics for a consumer.
// ConsumerMetricStore instances are safe to use with multiple goroutines.
type ConsumerMetricsStore struct {
	w                  *RecordWriterWithMetrics
	filterer           store.TypeFilterer
	lock               sync.Mutex
	recordCount        uint64
	removedRecordCount uint64
}

// Pause pauses the writer in the corresponding consumer waiting for any
// in progress writes to complete
func (s *ConsumerMetricsStore) Pause() {
	s.w.Pause()
}

// Resume resumes the writer in the corresponding consumer
func (s *ConsumerMetricsStore) Resume() {
	s.w.Resume()
}

// DisableWrites causes all future writes to underlying persistent store of
// corresponding consumer to return an error. Any in progress writes
// will complete before DisableWrites returns.
func (s *ConsumerMetricsStore) DisableWrites() {
	s.w.Disable()
}

// Adds count to the total number of records consumer must write out.
func (s *ConsumerMetricsStore) AddToRecordCount(count uint64) {
	s.addToRecordCount(count)
}

// Removed count from the total number of records consumer must write out.
func (s *ConsumerMetricsStore) RemoveFromRecordCount(count uint64) {
	s.removeFromRecordCount(count)
}

// Returns true if consumer is to write out this record or false otherwise.
func (s *ConsumerMetricsStore) Filter(r *store.Record) bool {
	return s.filterer.Filter(r)
}

// Metrics writes the metrics of corresponding consumer to m.
func (s *ConsumerMetricsStore) Metrics(m *ConsumerMetrics) {
	s.metrics(m)
}

// SetMetrics sets the metrics for corresponding consumer to m.
func (s *ConsumerMetricsStore) SetMetrics(m *ConsumerMetrics) {
	s.setMetrics(m)
}

// ConsumerAttributes represent the unchanging attributes of a particular
// ConsumerWithMetrics instance.
type ConsumerAttributes struct {
	// The number of writing goroutines
	Concurrency uint
	// The number of records written each time
	BatchSize uint
	// The maximum records per second per goroutine. 0 means unlimited.
	RecordsPerSecond uint
	// The time period length for rolled up values.
	// A value bigger than 0 means that client should feed this consumer
	// store.NamedIterator instances that report summarised values for
	// the same time period length. 0 means client should feed this
	// consumer store.NamedIterator instances that report all metric
	// values.
	RollUpSpan time.Duration
	// Compiled regex forms of metrics to exclude
	MetricsToExclude    []*regexp.Regexp
	BatchSizes          *tricorder.CumulativeDistribution
	PerMetricWriteTimes *tricorder.CumulativeDistribution
}

// WritesSameAs returns true if these consumer attributes would cause the
// same values to be written as other.
func (c *ConsumerAttributes) WritesSameAs(other *ConsumerAttributes) bool {
	if c.RollUpSpan != other.RollUpSpan {
		return false
	}
	if len(c.MetricsToExclude) != len(other.MetricsToExclude) {
		return false
	}
	for i := range c.MetricsToExclude {
		if c.MetricsToExclude[i].String() != other.MetricsToExclude[i].String() {
			return false
		}
	}
	return true
}

// TotalRecordsPerSecond returns RecordsPerSecond * Concurrency
func (c *ConsumerAttributes) TotalRecordsPerSecond() uint {
	return c.RecordsPerSecond * c.Concurrency
}

// ConsumerWithMetrics instances work like Consumer instances but also have
// metrics.
// Like Consumer instances, ConsumerWithMetric instances are NOT safe to use
// with multiple goroutines.
type ConsumerWithMetrics struct {
	name         string
	attributes   ConsumerAttributes
	metricsStore *ConsumerMetricsStore
	consumer     consumerType
}

func (c *ConsumerWithMetrics) Name() string {
	return c.name
}

// Attributes writes this instance's attirbutes to a
func (c *ConsumerWithMetrics) Attributes(a *ConsumerAttributes) {
	*a = c.attributes
}

// MetricsStore returns the ConsumerMetricsStore for this instance.
func (c *ConsumerWithMetrics) MetricsStore() *ConsumerMetricsStore {
	return c.metricsStore
}

// Write works like Consumer.Write but does not return an error.
func (c *ConsumerWithMetrics) Write(
	n store.NamedIterator, host string, tagGroup TagGroup) {
	c.consumer.Write(
		store.NamedIteratorFilter(n, c.metricsStore.filterer),
		host,
		tagGroup)
}

// Flush works like Consumer.Flush but does not return an error.
func (c *ConsumerWithMetrics) Flush() {
	c.consumer.Flush()
}

// Instances that want to know when a batch of records are written to
// persistent store implement this interface.
type RecordWriteHooker interface {
	// WriteHook is called just after a write or attempted write to
	// pstore. records are the records written; err is the resulting
	// error if any. Implementations must not modify the records array.
	WriteHook(records []Record, result error)
}

// ConsumerWithMetricsBuilder builds a ConsumerWithMetrics instance.
// Each instance is good for building one and only one ConsumerWithMetrics
// instance.
// These instances are NOT safe to use with multiple goroutines.
type ConsumerWithMetricsBuilder struct {
	c       *ConsumerWithMetrics
	hooks   []RecordWriteHooker
	metrics ConsumerMetrics
	filter  func(*store.MetricInfo) bool
	paused  bool
	logger  log.Logger
}

// NewConsumerWithMetricsBuilder creates a new instance that will
// build a consumer that uses w to write values out.
func NewConsumerWithMetricsBuilder(
	w LimitedRecordWriter) *ConsumerWithMetricsBuilder {
	return newConsumerWithMetricsBuilder(w)
}

// SetLogger sets the logger that built instance is to use
func (b *ConsumerWithMetricsBuilder) SetLogger(logger log.Logger) {
	b.logger = logger
}

// SetConsumerMetrics ensures that built instance with have metrics equal
// to m.
func (b *ConsumerWithMetricsBuilder) SetConsumerMetrics(m *ConsumerMetrics) {
	b.metrics = *m
}

// AddHook adds a hook for writes. hook must be non-nil.
func (b *ConsumerWithMetricsBuilder) AddHook(hook RecordWriteHooker) {
	if hook == nil {
		panic("nil RecordWriteHooker")
	}
	b.hooks = append(b.hooks, hook)
}

// SetRecordsPerSecond throttles writes. Default is 0 which means no
// throttling.
func (b *ConsumerWithMetricsBuilder) SetRecordsPerSecond(
	recordsPerSecond uint) {
	b.c.attributes.RecordsPerSecond = recordsPerSecond
}

// SetBufferSize sets how many values the consumer will buffer before
// writing them out. The default is 1000. SetBufferSize panics if size < 1.
func (b *ConsumerWithMetricsBuilder) SetBufferSize(size uint) {
	if size < 1 {
		panic("non-zero size required.")
	}
	b.c.attributes.BatchSize = size
}

// SetConcurrency sets how many goroutines will write to the underlying
// writer. Default is 1. SetConcurrency panics if concurrency < 1.
func (b *ConsumerWithMetricsBuilder) SetConcurrency(concurrency uint) {
	if concurrency < 1 {
		panic("non-zero concurrency required.")
	}
	b.c.attributes.Concurrency = concurrency
}

// SetRollUpSpan sets the length of time periods for rolled up values
// Other than setting RollUpSpan consumer attribute, this method has
// no effect on built consumer.
func (b *ConsumerWithMetricsBuilder) SetRollUpSpan(dur time.Duration) {
	if dur < 0 {
		panic("Non-negative duration required.")
	}
	b.c.attributes.RollUpSpan = dur
}

// SetRegexesOfMetricsToExclude sets what metric names to exclude.
// SetRegexesOfMetricsToExclude returns an error if one of regexes is an
// invalid regular expression.
func (b *ConsumerWithMetricsBuilder) SetRegexesOfMetricsToExclude(
	regexes []string) error {
	if len(regexes) == 0 {
		b.c.attributes.MetricsToExclude = nil
		return nil
	}
	metricsToExclude := make([]*regexp.Regexp, len(regexes))
	for i := range regexes {
		var err error
		metricsToExclude[i], err = regexp.Compile(regexes[i])
		if err != nil {
			return err
		}
	}
	b.c.attributes.MetricsToExclude = metricsToExclude
	return nil
}

func (b *ConsumerWithMetricsBuilder) Name() string {
	return b.c.name
}

// SetName sets the name of the consumer. Default is the empty string.
func (b *ConsumerWithMetricsBuilder) SetName(name string) {
	b.c.name = name
}

// SetPerMetricWriteTimeDist sets the distribution that the consumer will
// use to record write times. The default is not to record write times.
func (b *ConsumerWithMetricsBuilder) SetPerMetricWriteTimeDist(
	d *tricorder.CumulativeDistribution) {
	b.c.attributes.PerMetricWriteTimes = d
}

// SetBatchSizeDist sets the distribution that the consumer will
// use to record the batch size of values written out.
// The default is not to record batch sizes.
func (b *ConsumerWithMetricsBuilder) SetBatchSizeDist(
	d *tricorder.CumulativeDistribution) {
	b.c.attributes.BatchSizes = d
}

// Attributes gets the attributes in this builder object
func (b *ConsumerWithMetricsBuilder) Attributes(attrs *ConsumerAttributes) {
	*attrs = b.c.attributes
}

// Pause causes the built consumer's writer to be paused.
func (b *ConsumerWithMetricsBuilder) SetPaused(paused bool) {
	b.paused = paused
}

// Paused returns whether or not built consumer will be paused.
func (b *ConsumerWithMetricsBuilder) Paused() bool {
	return b.paused
}

// Build builds the ConsumerWithMetrics instance and destroys this builder.
func (b *ConsumerWithMetricsBuilder) Build() *ConsumerWithMetrics {
	return b.build()
}
