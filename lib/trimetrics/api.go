// Package trimetrics contains routines to facilitate exposing metrics in
// tricorder.
package trimetrics

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

type SlidingSuccessCounter struct {
	now time.Time
}

func NewSlidingSuccessCounter() *SlidingSuccessCounter {
	return &SlidingSuccessCounter{now: time.Now()}
}

func (s *SlidingSuccessCounter) Add(total, success uint64) {
}

func (s *SlidingSuccessCounter) Elapsed() int64 {
	return int64(time.Since(s.now) / time.Second)
}

func (s *SlidingSuccessCounter) Register(path string) error {
	return tricorder.RegisterMetric(
		path,
		s.Elapsed,
		units.Second,
		"time elapsed in seconds")
}

// Duration stores a time.Duration behind a mutex
type Duration struct {
	mu    sync.Mutex
	value time.Duration
}

// Set sets the duration
func (d *Duration) Set(x time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.value = x
}

// Get retrieves the duration.
func (d *Duration) Get() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.value
}

// Counter represents a single counter metric.
type Counter struct {
	mu    sync.Mutex
	value uint64
}

// NewCounter creates a new count metric. path is the path of the new metric;
// desc is the description of the metric.
func NewCounter(path, desc string) (*Counter, error) {
	return newCounter(path, desc)
}

// Inc increments the value of this metric  by x.
func (c *Counter) Inc(x uint64) {
	c.inc(x)
}

// WriterMetrics instances allow modules writing data to easily expose
// performance metrics about the writes.
type WriterMetrics struct {
	timeToWriteDist *tricorder.CumulativeDistribution
	batchSizeDist   *tricorder.CumulativeDistribution
	mu              sync.Mutex
	singles         writerSingleMetricsType
}

// NewWriterMetrics creates a new WriterMetric instance. parentPath is
// the parent path of the metrics. NewWriterMetrics returns an error
// if the path of the metrics it publishes collide with the path of some
// existing metric.
func NewWriterMetrics(parentPath string) (*WriterMetrics, error) {
	return newWriterMetrics(parentPath)
}

// LogError logs a write failure.
// elapsed is the time elapsed during the write.
// numToWrite is the number of records to be written.
// err is the resulting write error.
func (w *WriterMetrics) LogError(
	elapsed time.Duration, numToWrite uint64, err error) {
	w.logError(elapsed, numToWrite, err)
}

// LogSuccess logs a write success.
// elapsed is the time elapsed during the write.
// numToWrite is the number of records to be written.
func (w *WriterMetrics) LogSuccess(elapsed time.Duration, numWritten uint64) {
	w.logSuccess(elapsed, numWritten)
}
