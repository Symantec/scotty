package trimetrics

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

func newCounter(path, desc string) (*Counter, error) {
	result := &Counter{}

	if err := tricorder.RegisterMetric(
		path, result.get, units.None, desc); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Counter) inc(x uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += x
}

func (c *Counter) get() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

type writerSingleMetricsType struct {
	Total     uint64
	Success   uint64
	Failure   uint64
	LastError string
}

func createDistributions(
	parentDir *tricorder.DirectorySpec, result *WriterMetrics) error {
	result.timeToWriteDist = tricorder.NewGeometricBucketer(0.001, 1000000).NewCumulativeDistribution()
	result.batchSizeDist = tricorder.NewGeometricBucketer(1, 1000000).NewCumulativeDistribution()
	if err := parentDir.RegisterMetric(
		"timeToWrite",
		result.timeToWriteDist,
		units.Millisecond,
		"Time per write"); err != nil {
		return err
	}
	if err := parentDir.RegisterMetric(
		"batchSize",
		result.batchSizeDist,
		units.None,
		"Batch size"); err != nil {
		return err
	}
	return nil
}

func createCounter(
	parentDir *tricorder.DirectorySpec, result *WriterMetrics) error {
	result.counter = newSlidingSuccessCounter()
	return result.counter.registerUnder(parentDir, "write", "writes")
}

func newWriterMetrics(parentPath string) (*WriterMetrics, error) {
	parentDir, err := tricorder.RegisterDirectory(parentPath)
	if err != nil {
		return nil, err
	}

	result := &WriterMetrics{}
	if err := createDistributions(parentDir, result); err != nil {
		return nil, err
	}
	if err := createCounter(parentDir, result); err != nil {
		return nil, err
	}

	var singles writerSingleMetricsType
	grp := tricorder.NewGroup()
	grp.RegisterUpdateFunc(func() time.Time {
		result.get(&singles)
		return time.Now()
	})
	dg := tricorder.DirectoryGroup{Group: grp, Directory: parentDir}

	if err := dg.RegisterMetric(
		"total", &singles.Total, units.None, "Total write calls"); err != nil {
		return nil, err
	}
	if err := dg.RegisterMetric(
		"success", &singles.Success, units.None, "Successful write calls"); err != nil {
		return nil, err
	}
	if err := dg.RegisterMetric(
		"failure", &singles.Failure, units.None, "Failed write calls"); err != nil {
		return nil, err
	}
	if err := dg.RegisterMetric(
		"lastError", &singles.LastError, units.None, "Last write error"); err != nil {
		return nil, err
	}
	return result, nil
}

func (w *WriterMetrics) get(dest *writerSingleMetricsType) {
	w.mu.Lock()
	defer w.mu.Unlock()
	*dest = w.singles
}

func (w *WriterMetrics) logError(
	elapsed time.Duration, numToWrite uint64, err error) {
	w.timeToWriteDist.Add(elapsed)
	w.batchSizeDist.Add(float64(numToWrite))
	w.counter.Inc(int64(numToWrite), 0)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.singles.Total++
	w.singles.Failure++
	w.singles.LastError = err.Error()
}

func (w *WriterMetrics) logSuccess(elapsed time.Duration, numWritten uint64) {
	w.timeToWriteDist.Add(elapsed)
	w.batchSizeDist.Add(float64(numWritten))
	w.counter.Inc(int64(numWritten), int64(numWritten))
	w.mu.Lock()
	defer w.mu.Unlock()
	w.singles.Total++
	w.singles.Success++
}
