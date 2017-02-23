package main

import (
	"github.com/Symantec/scotty/cis"
	"github.com/Symantec/scotty/lib/keyedqueue"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

type splitTimerByKeyType struct {
	lock               sync.Mutex
	lastTimeStampByKey map[interface{}]time.Time
}

func newSplitTimerByKey() *splitTimerByKeyType {
	return &splitTimerByKeyType{
		lastTimeStampByKey: make(map[interface{}]time.Time),
	}
}

// Split returns the difference between timestamp and last reported
// timestamp for given key. Split returns false if Split being called for the
// very first time with given key.
func (t *splitTimerByKeyType) Split(key interface{}, timeStamp time.Time) (
	result time.Duration, ok bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if lastTimeStamp, exists := t.lastTimeStampByKey[key]; exists {
		result = timeStamp.Sub(lastTimeStamp)
		ok = true
	}
	t.lastTimeStampByKey[key] = timeStamp
	return
}

type cisWriterMetricsType struct {
	LastWriteError   string
	SuccessfulWrites uint64
	TotalWrites      uint64
}

func (c *cisWriterMetricsType) FailedWrites() uint64 {
	return c.TotalWrites - c.SuccessfulWrites
}

type cisWriterMetricsManagerType struct {
	lock    sync.Mutex
	metrics cisWriterMetricsType
}

func newCISWriterMetricsManager() *cisWriterMetricsManagerType {
	return &cisWriterMetricsManagerType{}
}

func (c *cisWriterMetricsManagerType) LogWrite(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.metrics.TotalWrites++
	if err != nil {
		c.metrics.LastWriteError = err.Error()
	} else {
		c.metrics.SuccessfulWrites++
	}
}

func (c *cisWriterMetricsManagerType) Get(metrics *cisWriterMetricsType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	*metrics = c.metrics
}

func createCISWriters(
	cisQueue *keyedqueue.Queue,
	cisClient *cis.Client,
	goroutineCount uint) (err error) {
	if err = tricorder.RegisterMetric(
		"cis/queueSize",
		cisQueue.Len,
		units.None,
		"Length of queue"); err != nil {
		return
	}
	timeBetweenWritesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err = tricorder.RegisterMetric(
		"cis/timeBetweenWrites",
		timeBetweenWritesDist,
		units.Second,
		"elapsed time between CIS updates"); err != nil {
		return
	}
	writeTimesDist := tricorder.NewGeometricBucketer(1, 100000.0).NewCumulativeDistribution()
	if err = tricorder.RegisterMetric(
		"cis/writeTimes",
		writeTimesDist,
		units.Millisecond,
		"elapsed time between CIS updates"); err != nil {
		return
	}
	payloadSizeDist := tricorder.NewGeometricBucketer(1, 1000000000.0).NewCumulativeDistribution()
	if err = tricorder.RegisterMetric(
		"cis/payloadSizes",
		payloadSizeDist,
		units.None,
		"Payload sizes"); err != nil {
		return
	}

	metricsManager := newCISWriterMetricsManager()
	var cisWriterMetrics cisWriterMetricsType
	cisWriterMetricsGroup := tricorder.NewGroup()
	cisWriterMetricsGroup.RegisterUpdateFunc(func() time.Time {
		metricsManager.Get(&cisWriterMetrics)
		return time.Now()
	})

	if err = tricorder.RegisterMetricInGroup(
		"cis/lastWriteError",
		&cisWriterMetrics.LastWriteError,
		cisWriterMetricsGroup,
		units.None,
		"Last CIS write error"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/successfulWrites",
		&cisWriterMetrics.SuccessfulWrites,
		cisWriterMetricsGroup,
		units.None,
		"Successful write count"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/totalWrites",
		&cisWriterMetrics.TotalWrites,
		cisWriterMetricsGroup,
		units.None,
		"total write count"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/failedWrites",
		cisWriterMetrics.FailedWrites,
		cisWriterMetricsGroup,
		units.None,
		"failed write count"); err != nil {
		return
	}

	splitTimerByKey := newSplitTimerByKey()

	for i := uint(0); i < goroutineCount; i++ {
		// CIS loop
		go func() {
			for {
				stat := cisQueue.Remove().(*cis.Stats)
				key := stat.Key()
				if elapsed, ok := splitTimerByKey.Split(key, stat.TimeStamp); ok {
					timeBetweenWritesDist.Add(elapsed)
				}
				writeStartTime := time.Now()
				writeInfo, err := cisClient.Write(stat)
				if err == nil {
					writeTimesDist.Add(time.Since(writeStartTime))
					payloadSizeDist.Add(float64(writeInfo.PayloadSize))
				}
				metricsManager.LogWrite(err)
			}
		}()
	}
	return
}
