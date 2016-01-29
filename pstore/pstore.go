package pstore

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

var (
	gWriteTimeBucketer = tricorder.NewGeometricBucketer(1e-4, 1000.0)
	gBatchSizeBucketer = tricorder.NewGeometricBucketer(1.0, 1e9)
)

type noBlockPStoreDataType struct {
	RequestCount           uint64
	SkippedCount           uint64
	TotalBatchCount        uint64
	UnsuccessfulBatchCount uint64
	LastBatchWriteError    string
}

func newNoBlockPStore(
	writer Writer,
	batchSize, channelSize int,
	flushDelay time.Duration) *NoBlockPStore {
	result := &NoBlockPStore{
		inChannel:          make(chan Record, channelSize),
		batchSize:          batchSize,
		channelSize:        channelSize,
		writer:             writer,
		flushDelay:         flushDelay,
		perMetricWriteDist: gWriteTimeBucketer.NewCumulativeDistribution(),
		batchSizeDist:      gBatchSizeBucketer.NewCumulativeDistribution(),
	}
	go result.loop()
	return result
}

func (n *NoBlockPStore) write(record *Record) {
	if !n.writer.IsTypeSupported(record.Kind) {
		return
	}
	select {
	case n.inChannel <- *record:
		n.logAcceptedRequest()
	default:
		n.logSkippedRequest()
	}
}

func (n *NoBlockPStore) collectData(data *noBlockPStoreDataType) {
	n.lock.Lock()
	defer n.lock.Unlock()
	*data = n.data
}

func (n *NoBlockPStore) logSkippedRequest() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.data.RequestCount++
	n.data.SkippedCount++
}

func (n *NoBlockPStore) logAcceptedRequest() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.data.RequestCount++
}

func (n *NoBlockPStore) logBatchError(err error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.data.TotalBatchCount++
	n.data.UnsuccessfulBatchCount++
	n.data.LastBatchWriteError = err.Error()
}

func (n *NoBlockPStore) logBatchSuccess() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.data.TotalBatchCount++
}

func (n *NoBlockPStore) flush(records []Record) {
	now := time.Now()
	err := n.writer.Write(records)
	rlen := len(records)
	n.perMetricWriteDist.Add(time.Now().Sub(now) / time.Duration(rlen))
	n.batchSizeDist.Add(float64(rlen))
	if err != nil {
		n.logBatchError(err)
	} else {
		n.logBatchSuccess()
	}
}

func (n *NoBlockPStore) loop() {
	batch := make([]Record, n.batchSize)
	idx := 0
	for {
		if idx == n.batchSize {
			n.flush(batch)
			idx = 0
		}
		if idx == 0 {
			batch[idx] = <-n.inChannel
			idx++
		} else {
			select {
			case batch[idx] = <-n.inChannel:
				idx++
			case <-time.After(n.flushDelay):
				n.flush(batch[:idx])
				idx = 0
			}
		}
	}
}

func (n *NoBlockPStore) registerMetrics() (err error) {
	if err = tricorder.RegisterMetric(
		"writer/writeTimePerMetric",
		n.perMetricWriteDist,
		units.Millisecond,
		"Time spent writing each metric"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/batchSizeDist",
		n.batchSizeDist,
		units.None,
		"Metrics per batch"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/maxBatchSize",
		&n.batchSize,
		units.None,
		"Max number of metrics per batch."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/channelCapacity",
		&n.channelSize,
		units.None,
		"Channel capacity."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/channelLoad",
		func() int { return len(n.inChannel) },
		units.None,
		"Channel load."); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"writer/flushDelay",
		&n.flushDelay,
		units.Second,
		"Flush delay."); err != nil {
		return
	}
	var data noBlockPStoreDataType
	region := tricorder.RegisterRegion(func() { n.collectData(&data) })
	if err = tricorder.RegisterMetricInRegion(
		"writer/requestCount",
		&data.RequestCount,
		region,
		units.None,
		"Total number of requests to write an individual metric. Does not include requests to write metrics of an unsupported kind."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/skippedCount",
		&data.SkippedCount,
		region,
		units.None,
		"Number of requests skipped because the writer was too busy to field all the requests."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/totalBatchCount",
		&data.TotalBatchCount,
		region,
		units.None,
		"Number of metric batches sent to back end"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/unsuccessfulBatchCount",
		&data.UnsuccessfulBatchCount,
		region,
		units.None,
		"Number of batches that were not written successfully"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInRegion(
		"writer/lastBatchWriteError",
		&data.LastBatchWriteError,
		region,
		units.None,
		"Last batch write error"); err != nil {
		return
	}
	return
}
