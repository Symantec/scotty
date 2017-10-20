package pstore

import (
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"regexp"
	"time"
)

const (
	kDefaultBufferSize = 1000
)

type hookWriter struct {
	wrapped RecordWriter
	hooks   []RecordWriteHooker
}

func (h *hookWriter) Write(records []Record) error {
	result := h.wrapped.Write(records)
	for _, hook := range h.hooks {
		hook.WriteHook(records, result)
	}
	return result
}

type throttleWriter struct {
	wrapped          RecordWriter
	recordsPerSecond uint
}

func (t *throttleWriter) Write(records []Record) error {
	if t.recordsPerSecond == 0 {
		return t.wrapped.Write(records)
	}
	now := time.Now()
	result := t.wrapped.Write(records)
	throttleDuration := time.Second * time.Duration(len(records)) / time.Duration(t.recordsPerSecond)
	timeToBeDone := now.Add(throttleDuration)
	now = time.Now()
	if now.Before(timeToBeDone) {
		time.Sleep(timeToBeDone.Sub(now))
	}
	return result
}

func (w *RecordWriterWithMetrics) write(records []Record) error {
	if !w.criticalSection.Enter() {
		return ErrDisabled
	}
	defer w.criticalSection.Exit()
	ctime := time.Now()
	result := w.W.Write(records)
	timeTaken := time.Now().Sub(ctime)
	if result == nil {
		w.logWrite(uint(len(records)), timeTaken)
	} else {
		w.logWriteError(
			uint(len(records)), result, timeTaken)
	}
	return result
}

func (w *RecordWriterWithMetrics) setMetrics(m *RecordWriterMetrics) {
	temp := *m
	w.lock.Lock()
	defer w.lock.Unlock()
	temp.Paused = w.metrics.Paused
	temp.Disabled = w.metrics.Disabled
	w.metrics = temp
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
	batchSize uint, err error, timeTaken time.Duration) {
	w.logDistributions(batchSize, timeTaken)
	w.Logger.Printf("Error writing to persistent store: %v", err)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.metrics.logWriteError(err.Error(), timeTaken)
}

func (w *RecordWriterWithMetrics) setPauseMetric(paused bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.metrics.Paused = paused
}

func (w *RecordWriterWithMetrics) setDisabledMetric(b bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.metrics.Disabled = b
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
	w.LastSuccessfulWriteTS = time.Now()
	w.TimeSpentWriting += timeTaken
}

func (w *RecordWriterMetrics) logWriteError(err string, timeTaken time.Duration) {
	w.WriteAttempts += 1
	w.LastWriteError = err
	w.LastWriteErrorTS = time.Now()
	w.TimeSpentWriting += timeTaken
}

type consumerRequestType struct {
	Iterator store.NamedIterator
	HostName string
	TagGroup TagGroup
}

// barrier releases callers in groups of N and makes callers wait until
// there are N callers to release.
//
// If a caller happens to come in while the barrier is already releasing a
// group N callers, that caller waits until the barrier releases the next
// group of N callers.
type barrier struct {
	inCh  chan bool
	outCh chan bool
}

// newBarrier creates a new barrier. count is N.
func newBarrier(count uint) *barrier {
	result := &barrier{inCh: make(chan bool), outCh: make(chan bool)}
	go result.loop(count)
	return result
}

// Await blocks the caller until there are N callers to release.
func (b *barrier) Await() {
	b.inCh <- true
	<-b.outCh
}

func (b *barrier) loop(count uint) {
	for {
		for i := uint(0); i < count; i++ {
			<-b.inCh
		}
		for i := uint(0); i < count; i++ {
			b.outCh <- true
		}
	}
}

func newAsyncConsumer(
	w RecordWriter, bufferSize, concurrency uint) *AsyncConsumer {
	result := &AsyncConsumer{
		requests: make(chan consumerRequestType, concurrency),
		// Flush barrier to accomodate each goroutine plus the
		// one caller to Flush.
		flushBarrier: newBarrier(concurrency + 1),
		concurrency:  concurrency,
	}
	for i := uint(0); i < concurrency; i++ {
		go result.loop(w, bufferSize)
	}
	return result
}

func (a *AsyncConsumer) loop(w RecordWriter, bufferSize uint) {
	consumer := newConsumer(w, bufferSize)
	for {
		request := <-a.requests
		if request.Iterator != nil {
			consumer.Write(
				request.Iterator,
				request.HostName,
				request.TagGroup)
		} else {
			consumer.Flush()

			// Wait here untl all goroutines have processed their
			// flush.
			a.flushBarrier.Await()
		}
	}
}

func (a *AsyncConsumer) writeAsync(
	n store.NamedIterator, hostName string, tagGroup TagGroup) {
	// nil signals a flush request, so we don't allow it here.
	if n == nil {
		panic("Got nil NamedIterator")
	}
	a.requests <- consumerRequestType{
		Iterator: n,
		HostName: hostName,
		TagGroup: tagGroup}
}

func (a *AsyncConsumer) flush() {
	// Send a flush request for each goroutine. Since each goroutine
	// waits on the others to process their flush, we are guaranteed that
	// each goroutine will get one and only one flush request.
	//
	// If two goroutines were to call flush at the same time, all of this
	// instance's goroutines would all block on the flush barrier before
	// either calling goroutine finished sending all of its flush requests
	// resulting in deadlock. This is why AsyncConsumer is NOT safe to use
	// with multiple goroutines.
	for i := uint(0); i < a.concurrency; i++ {
		a.requests <- consumerRequestType{}
	}
	// Block caller until all goroutines have processed their flush. Then
	// release the goroutines and the caller.
	a.flushBarrier.Await()
}

func newConsumer(w RecordWriter, bufferSize uint) *Consumer {
	return &Consumer{
		w:      w,
		buffer: make([]Record, bufferSize),
	}
}

func (c *Consumer) write(
	n store.NamedIterator, hostName string, tagGroup TagGroup) error {
	var r store.Record
	c.toBeCommitted = append(c.toBeCommitted, n)
	for n.Next(&r) {
		c.buffer[c.idx] = Record{
			HostName:  hostName,
			Tags:      tagGroup,
			Path:      r.Info.Path(),
			Kind:      r.Info.Kind(),
			SubType:   r.Info.SubType(),
			Unit:      r.Info.Unit(),
			Value:     r.Value,
			Timestamp: duration.FloatToTime(r.TimeStamp),
		}
		c.idx++
		if c.idx == len(c.buffer) {
			err := c.flush()
			if err != nil {
				return err
			}
			c.toBeCommitted = append(c.toBeCommitted, n)
		}
	}
	return nil
}

func (c *Consumer) flush() (err error) {
	// Write pending records out if there are any.
	if c.idx != 0 {
		err = c.w.Write(c.buffer[:c.idx])
		c.idx = 0
	}
	// Commit pending iterators if no error
	if err == nil {
		for _, iter := range c.toBeCommitted {
			iter.Commit()
		}
	}
	// Clear out pending iterators
	c.toBeCommitted = c.toBeCommitted[:0]
	return
}

type consumerType interface {
	Write(n store.NamedIterator, hostName string, tagGroup TagGroup)
	Flush()
}

func toConsumerType(c *Consumer) consumerType {
	return consumerTypeAdapter{c}
}

func toAsyncConsumerType(c *AsyncConsumer) consumerType {
	return asyncConsumerTypeAdapter{c}
}

type consumerTypeAdapter struct {
	c *Consumer
}

func (c consumerTypeAdapter) Write(
	n store.NamedIterator, hostName string, tagGroup TagGroup) {
	c.c.Write(n, hostName, tagGroup)
}

func (c consumerTypeAdapter) Flush() {
	c.c.Flush()
}

type asyncConsumerTypeAdapter struct {
	*AsyncConsumer
}

func (c asyncConsumerTypeAdapter) Write(
	n store.NamedIterator, hostName string, tagGroup TagGroup) {
	c.WriteAsync(n, hostName, tagGroup)
}

func (s *ConsumerMetricsStore) metrics(m *ConsumerMetrics) {
	s.w.Metrics(&m.RecordWriterMetrics)
	s.lock.Lock()
	defer s.lock.Unlock()
	m.TotalValues = s.recordCount
	m.SkippedValues = s.removedRecordCount
}

func (s *ConsumerMetricsStore) setMetrics(m *ConsumerMetrics) {
	s.w.SetMetrics(&m.RecordWriterMetrics)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.recordCount = m.TotalValues
	s.removedRecordCount = m.SkippedValues
}

func (s *ConsumerMetricsStore) addToRecordCount(count uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.recordCount += count
}

func (s *ConsumerMetricsStore) removeFromRecordCount(count uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.removedRecordCount += count
}

func toFilterer(w LimitedRecordWriter) func(*store.MetricInfo) bool {
	withSubType, ok := w.(LimitedBySubTypeRecordWriter)
	if ok {
		return func(m *store.MetricInfo) bool {
			return withSubType.IsTypeAndSubTypeSupported(m.Kind(), m.SubType())
		}
	}
	return func(m *store.MetricInfo) bool {
		return w.IsTypeSupported(m.Kind())
	}
}

func newConsumerWithMetricsBuilder(
	w LimitedRecordWriter) *ConsumerWithMetricsBuilder {
	writerWithMetrics := NewRecordWriterWithMetrics(w)
	ptr := &ConsumerWithMetrics{
		attributes: ConsumerAttributes{
			BatchSize:   kDefaultBufferSize,
			Concurrency: 1},
		metricsStore: &ConsumerMetricsStore{
			w: writerWithMetrics,
		},
	}
	return &ConsumerWithMetricsBuilder{c: ptr, filter: toFilterer(w)}
}

func excludeMetrics(
	filter func(*store.MetricInfo) bool,
	metricsToExclude []*regexp.Regexp) func(*store.MetricInfo) bool {
	return func(m *store.MetricInfo) bool {
		if !filter(m) {
			return false
		}
		path := m.Path()
		for _, re := range metricsToExclude {
			if re.MatchString(path) {
				return false
			}
		}
		return true
	}
}

func (b *ConsumerWithMetricsBuilder) build() *ConsumerWithMetrics {
	result := b.c
	// fixup writer
	writer := result.metricsStore.w.W
	if result.attributes.RecordsPerSecond > 0 {
		writer = &throttleWriter{
			wrapped:          writer,
			recordsPerSecond: result.attributes.RecordsPerSecond,
		}
	}
	if len(b.hooks) > 0 {
		writer = &hookWriter{
			wrapped: writer,
			hooks:   b.hooks}
	}
	result.metricsStore.filterer = store.TypeFiltererFuncActiveOnly(
		excludeMetrics(b.filter, result.attributes.MetricsToExclude))
	result.metricsStore.w.W = writer

	// fix up writer with metrics
	if b.paused {
		result.metricsStore.w.Pause()
	}
	// fix up metrics
	result.metricsStore.w.BatchSizes = result.attributes.BatchSizes
	result.metricsStore.w.PerMetricWriteTimes = result.attributes.PerMetricWriteTimes
	result.metricsStore.w.Logger = b.logger

	result.metricsStore.SetMetrics(&b.metrics)

	// Set up consumer
	if result.attributes.Concurrency == 1 {
		result.consumer = toConsumerType(
			NewConsumer(
				result.metricsStore.w,
				result.attributes.BatchSize))
	} else if result.attributes.Concurrency > 1 {
		result.consumer = toAsyncConsumerType(NewAsyncConsumer(
			result.metricsStore.w,
			result.attributes.BatchSize,
			result.attributes.Concurrency))
	} else {
		panic("pstore: Oops, bad state in build method.")
	}
	b.c = nil
	b.hooks = nil
	b.filter = nil
	b.logger = nil
	return result
}
