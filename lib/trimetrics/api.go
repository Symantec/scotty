// Package trimetrics contains routines to facilitate exposing metrics in
// tricorder.
package trimetrics

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"sync"
	"time"
)

// SlidingSuccessCounter tracks the total and success count of an event
// over a period of one hour and 24 hours.
type SlidingSuccessCounter struct {
	mu          sync.Mutex
	totalHour   ringCounterType
	totalDay    ringCounterType
	successHour ringCounterType
	successDay  ringCounterType
}

// NewSlidingSuccessCounter returns a new instance.
func NewSlidingSuccessCounter() *SlidingSuccessCounter {
	return newSlidingSuccessCounter()
}

// Inc increments the total and success count of the event being tracked.
// The events are assumed to have happened at time.Now()
func (s *SlidingSuccessCounter) Inc(total, success int64) {
	s.inc(total, success)
}

// Register ensures that the metrics this instance is tracking are being
// published to the tricorder ecosystem. This method only needs to be called
// once unless caller later uses the tricorder API to remove the published
// metrics. path is the path for the metrics; desc is the description for
// the metrics. This method publishes 6 metrics each with the following suffix
// added to path.
//
// 	_ratio_1d - success/total over last 24 hours
// 	_ratio_1h - success/total over last hour
// 	_success_1d - success over last 24 hours
// 	_success_1h - success over last 1 hour
// 	_total_1d - total over last 24 hours
// 	_total_1h - total over last 1 hour
func (s *SlidingSuccessCounter) Register(path, desc string) error {
	return s.register(path, desc)
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
