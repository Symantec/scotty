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

type consumerRequestType struct {
	Iterator store.NamedIterator
	HostName string
	AppName  string
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
func newBarrier(count int) *barrier {
	result := &barrier{inCh: make(chan bool), outCh: make(chan bool)}
	go result.loop(count)
	return result
}

// Await blocks the caller until there are N callers to release.
func (b *barrier) Await() {
	b.inCh <- true
	<-b.outCh
}

func (b *barrier) loop(count int) {
	for {
		for i := 0; i < count; i++ {
			<-b.inCh
		}
		for i := 0; i < count; i++ {
			b.outCh <- true
		}
	}
}

func newAsyncConsumer(
	w RecordWriter, bufferSize, concurrency int) *AsyncConsumer {
	result := &AsyncConsumer{
		requests: make(chan consumerRequestType, concurrency),
		// Flush barrier to accomodate each goroutine plus the
		// one caller to Flush.
		flushBarrier: newBarrier(concurrency + 1),
		concurrency:  concurrency,
	}
	for i := 0; i < concurrency; i++ {
		go result.loop(w, bufferSize)
	}
	return result
}

func (a *AsyncConsumer) loop(w RecordWriter, bufferSize int) {
	consumer := newConsumer(w, bufferSize)
	for {
		request := <-a.requests
		if request.Iterator != nil {
			consumer.Write(
				request.Iterator,
				request.HostName,
				request.AppName)
		} else {
			consumer.Flush()

			// We have to wait here untl all goroutines have
			// processed their flush. Otherwise, we may end up
			// grabbing a flush request intended for a different
			// goroutine.
			a.flushBarrier.Await()
		}
	}
}

func (a *AsyncConsumer) writeAsync(
	n store.NamedIterator, hostName, appName string) {
	// nil signals a flush request, so we don't allow it here.
	if n == nil {
		panic("Got nil NamedIterator")
	}
	a.requests <- consumerRequestType{
		Iterator: n,
		HostName: hostName,
		AppName:  appName}
}

func (a *AsyncConsumer) flush() {
	// Send a flush request for each goroutine. Since each goroutine
	// waits on the others to process their flush, we are guaranteed that
	// each goroutine will get one and only one flush request.
	for i := 0; i < a.concurrency; i++ {
		a.requests <- consumerRequestType{}
	}
	// Block caller until all goroutines have processed their flush.
	a.flushBarrier.Await()
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
