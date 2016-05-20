package pstore

import (
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"strings"
	"time"
)

const (
	kDefaultBufferSize = 1000
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

type consumerType interface {
	Write(n store.NamedIterator, hostName, appName string)
	Flush()
}

func toConsumerType(c *Consumer) consumerType {
	return consumerTypeAdapter{c}
}

type consumerTypeAdapter struct {
	c *Consumer
}

func (c consumerTypeAdapter) Write(
	n store.NamedIterator, hostName, appName string) {
	c.c.Write(n, hostName, appName)
}

func (c consumerTypeAdapter) Flush() {
	c.c.Flush()
}

func (s *ConsumerMetricsStore) metrics(m *ConsumerMetrics) {
	s.w.Metrics(&m.RecordWriterMetrics)
	recordCount := s.getRecordCount()
	if m.ValuesWritten < recordCount {
		m.ValuesNotWritten = recordCount - m.ValuesWritten
	} else {
		m.ValuesNotWritten = 0
	}
}

func (s *ConsumerMetricsStore) getRecordCount() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.recordCount
}

func (s *ConsumerMetricsStore) addToRecordCount(count uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.recordCount += count
}

func (s ConsumerMetricsStoreList) updateCounts(
	iterator store.NamedIterator) {
	var r store.Record
	tempCounts := make([]uint64, len(s))
	for iterator.Next(&r) {
		for i := range s {
			if s[i].Filter(&r) {
				tempCounts[i]++
			}
		}
	}
	iterator.Commit()
	for i := range s {
		s[i].AddToRecordCount(tempCounts[i])
	}
}

func toFilterer(typeFilter func(types.Type) bool) store.Filterer {
	f := func(r *store.Record) bool {
		return typeFilter(r.Info.Kind()) && r.Active
	}
	return store.FiltererFunc(f)
}

func newConsumerWithMetricsBuilder(
	w LimitedRecordWriter) *ConsumerWithMetricsBuilder {
	writerWithMetrics := &RecordWriterWithMetrics{W: w}
	ptr := &ConsumerWithMetrics{
		metricsStore: &ConsumerMetricsStore{
			w:        writerWithMetrics,
			filterer: toFilterer(w.IsTypeSupported),
		},
	}
	return &ConsumerWithMetricsBuilder{
		c: &ptr, bufferSize: kDefaultBufferSize}
}

func (b *ConsumerWithMetricsBuilder) build() *ConsumerWithMetrics {
	(*b.c).consumer = toConsumerType(
		NewConsumer((*b.c).metricsStore.w, b.bufferSize))
	result := *b.c
	*b.c = nil
	return result
}
