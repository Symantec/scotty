package main

import (
	"errors"
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
	LastWriteError  string
	CompletedWrites uint64
	ProcessedWrites uint64
	SentWrites      uint64
	AttemptedWrites uint64
}

func (c *cisWriterMetricsType) ServerFailures() uint64 {
	return c.ProcessedWrites - c.CompletedWrites
}

func (c *cisWriterMetricsType) SendFailures() uint64 {
	return c.AttemptedWrites - c.SentWrites
}

type cisWriterMetricsManagerType struct {
	lock    sync.Mutex
	metrics cisWriterMetricsType
}

func newCISWriterMetricsManager() *cisWriterMetricsManagerType {
	return &cisWriterMetricsManagerType{}
}

func (c *cisWriterMetricsManagerType) LogRequest(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.metrics.AttemptedWrites++
	if err != nil {
		c.metrics.LastWriteError = err.Error()
	} else {
		c.metrics.SentWrites++
	}
}

func (c *cisWriterMetricsManagerType) LogResponse(r *cis.Response) {
	var err error
	if r.Err != nil {
		err = errors.New(r.Err.Error() + ": " + r.Url + ": " + r.Payload)
	} else if r.Code/100 != 2 {
		err = errors.New(r.Status + ": " + r.Url + ": " + r.Payload)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.metrics.ProcessedWrites++
	if err != nil {
		c.metrics.LastWriteError = err.Error()
	} else {
		c.metrics.CompletedWrites++
	}
}

func (c *cisWriterMetricsManagerType) Get(metrics *cisWriterMetricsType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	*metrics = c.metrics
}

func createCISWriters(
	cisQueue *keyedqueue.Queue,
	cisClient *cis.Client) (err error) {
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
		"cis/completedWrites",
		&cisWriterMetrics.CompletedWrites,
		cisWriterMetricsGroup,
		units.None,
		"Number of successful, completed writes"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/serverFailures",
		cisWriterMetrics.ServerFailures,
		cisWriterMetricsGroup,
		units.None,
		"Number of server side failutes"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/processedWrites",
		&cisWriterMetrics.ProcessedWrites,
		cisWriterMetricsGroup,
		units.None,
		"Number of writes server received"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/sentWrites",
		&cisWriterMetrics.SentWrites,
		cisWriterMetricsGroup,
		units.None,
		"Number of writes client sent."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/sendFailures",
		cisWriterMetrics.SendFailures,
		cisWriterMetricsGroup,
		units.None,
		"Number of send failures"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"cis/attemptedWrites",
		&cisWriterMetrics.AttemptedWrites,
		cisWriterMetricsGroup,
		units.None,
		"Number of client attempted writes"); err != nil {
		return
	}

	splitTimerByKey := newSplitTimerByKey()
	var writer *cis.Writer
	writer, err = cisClient.NewAsyncWriter(func(r *cis.Response) {
		metricsManager.LogResponse(r)
		payloadSizeDist.Add(float64(len(r.Payload)))
	})
	if err != nil {
		return
	}
	// CIS loop
	go func() {
		for {
			stat := cisQueue.Remove().(*cis.Stats)
			key := stat.Key()
			if elapsed, ok := splitTimerByKey.Split(key, stat.TimeStamp); ok {
				timeBetweenWritesDist.Add(elapsed)
			}
			writeStartTime := time.Now()
			err := writer.Write(stat)
			writeTimesDist.Add(time.Since(writeStartTime))
			metricsManager.LogRequest(err)
		}
	}()
	return
}
