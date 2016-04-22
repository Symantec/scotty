package pstore

import (
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"strings"
	"time"
)

func (w *RecordWriterWithMetrics) write(records []Record) error {
	ctime := time.Now()
	result := w.W.Write(records)
	timeTaken := time.Now().Sub(ctime)
	if result == nil {
		w.logWrite(uint(len(records)), timeTaken)
	} else {
		w.logWriteError(
			uint(len(records)), result.Error(), timeTaken)
	}
	return result
}

func (w *RecordWriterWithMetrics) _metrics(m *RecordWriterMetrics) {
	w.lock.Lock()
	defer w.lock.Unlock()
	*m = w.metrics
}

func (w *RecordWriterWithMetrics) logWrite(
	batchSize uint, timeTaken time.Duration) {
	w.logDistributions(batchSize, timeTaken)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.metrics.logWrite(batchSize, timeTaken)
}

func (w *RecordWriterWithMetrics) logWriteError(
	batchSize uint, err string, timeTaken time.Duration) {
	w.logDistributions(batchSize, timeTaken)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.metrics.logWriteError(err, timeTaken)
}

func (w *RecordWriterWithMetrics) logDistributions(
	batchSize uint, timeTaken time.Duration) {
	if w.PerMetricWriteTimes != nil {
		w.PerMetricWriteTimes.Add(
			timeTaken / time.Duration(batchSize))
	}
	if w.BatchSizes != nil {
		w.BatchSizes.Add(float64(batchSize))
	}
}

func (w *RecordWriterMetrics) logWrite(
	batchSize uint, timeTaken time.Duration) {
	w.ValuesWritten += uint64(batchSize)
	w.WriteAttempts += 1
	w.SuccessfulWrites += 1
	w.TimeSpentWriting += timeTaken
}

func (w *RecordWriterMetrics) logWriteError(err string, timeTaken time.Duration) {
	w.WriteAttempts += 1
	w.LastWriteError = err
	w.TimeSpentWriting += timeTaken
}

func newConsumer(w RecordWriter, bufferSize int) *Consumer {
	return &Consumer{
		w:             w,
		buffer:        make([]Record, bufferSize),
		toBeCommitted: make(map[store.NamedIterator]bool),
	}
}

func (c *Consumer) write(
	n store.NamedIterator, hostName, appName string) error {
	var r store.Record
	for n.Next(&r) {
		c.toBeCommitted[n] = true
		c.buffer[c.idx] = Record{
			HostName:  hostName,
			Tags:      TagGroup{TagAppName: appName},
			Path:      strings.Replace(r.Info.Path(), "/", "_", -1),
			Kind:      r.Info.Kind(),
			Unit:      r.Info.Unit(),
			Value:     r.Value,
			Timestamp: messages.FloatToTime(r.TimeStamp),
		}
		c.idx++
		if c.idx == len(c.buffer) {
			err := c.flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consumer) flush() error {
	if c.idx == 0 {
		// fast track: No pending records to write out.
		return nil
	}
	err := c.w.Write(c.buffer[:c.idx])
	c.idx = 0
	if err == nil {
		for k := range c.toBeCommitted {
			k.Commit()
		}
	}
	for k := range c.toBeCommitted {
		delete(c.toBeCommitted, k)
	}
	return err
}
