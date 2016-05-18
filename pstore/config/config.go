package config

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"os"
	"regexp"
	"sync"
	"time"
)

const (
	kDefaultBatchSize = 1000
)

var (
	kFakeWriter = &fakeWriter{}
)

type fakeWriter struct {
	lock   sync.Mutex
	file   *os.File
	buffer *bufio.Writer
}

func newFakeWriterToPath(path string) (pstore.RecordWriter, error) {
	if path == "" {
		return kFakeWriter, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &fakeWriter{
		file:   file,
		buffer: bufio.NewWriter(file),
	}, nil
}

func (f *fakeWriter) Write(records []pstore.Record) (err error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for i := range records {
		f.printLine(records[i])
		f.printLine()
	}
	f.printLine()
	return
}

func (f *fakeWriter) printLine(args ...interface{}) {
	if f.buffer == nil {
		fmt.Println(args...)
	} else {
		fmt.Fprintln(f.buffer, args...)
		f.buffer.Flush()
		f.file.Sync()
	}
}

type filterWriter struct {
	Wrapped pstore.RecordWriter
	Filter  func(r *pstore.Record) bool
}

func (f *filterWriter) Write(records []pstore.Record) error {
	var filtered []pstore.Record
	for i := range records {
		if f.Filter(&records[i]) {
			filtered = append(filtered, records[i])
		}
	}
	// Don't write empty arrays.
	if filtered == nil {
		return nil
	}
	return f.Wrapped.Write(filtered)
}

type dualWriter struct {
	pstore.LimitedRecordWriter
	Extra pstore.RecordWriter
}

func (d *dualWriter) Write(records []pstore.Record) error {
	result := d.LimitedRecordWriter.Write(records)
	if result == nil {
		d.Extra.Write(records)
	}
	return result
}

type throttleWriter struct {
	pstore.LimitedRecordWriter
	RecordsPerMinute int
}

func (t *throttleWriter) Write(records []pstore.Record) error {
	now := time.Now()
	result := t.LimitedRecordWriter.Write(records)
	throttleDuration := time.Minute * time.Duration(len(records)) / time.Duration(t.RecordsPerMinute)
	timeToBeDone := now.Add(throttleDuration)
	now = time.Now()
	if now.Before(timeToBeDone) {
		time.Sleep(timeToBeDone.Sub(now))
	}
	return result
}

func (d Decorator) newWriter(wf WriterFactory) (
	result pstore.LimitedRecordWriter, err error) {
	if d.RecordsPerMinute < 0 {
		d.RecordsPerMinute = 0
	}
	writer, err := wf.NewWriter()
	if err != nil {
		return
	}
	if d.RecordsPerMinute > 0 {
		writer = &throttleWriter{
			LimitedRecordWriter: writer,
			RecordsPerMinute:    d.RecordsPerMinute}
	}
	if d.DebugMetricRegex != "" || d.DebugHostRegex != "" {
		var fakeWriter pstore.RecordWriter
		fakeWriter, err = newFakeWriterToPath(d.DebugFilePath)
		if err != nil {
			return
		}
		var metricRegex, hostRegex *regexp.Regexp
		if d.DebugMetricRegex != "" {
			metricRegex = regexp.MustCompile(d.DebugMetricRegex)
		}
		if d.DebugHostRegex != "" {
			hostRegex = regexp.MustCompile(d.DebugHostRegex)
		}
		afilter := func(r *pstore.Record) bool {
			if metricRegex != nil && !metricRegex.MatchString(r.Path) {
				return false
			}
			if hostRegex != nil && !hostRegex.MatchString(r.HostName) {
				return false
			}
			return true
		}
		filterWriter := &filterWriter{
			Wrapped: fakeWriter,
			Filter:  afilter,
		}
		result = &dualWriter{writer, filterWriter}
	} else {
		result = writer
	}
	return
}

func (c ConsumerConfig) newConsumerBuilder(
	wf WriterFactory) (
	result *pstore.ConsumerWithMetricsBuilder, err error) {
	if c.Name == "" {
		return nil, errors.New("Name field is required.")
	}
	if c.BatchSize < 1 {
		c.BatchSize = kDefaultBatchSize
	}
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	writer, err := wf.NewWriter()
	if err != nil {
		return
	}
	result = pstore.NewConsumerWithMetricsBuilder(writer)
	result.SetConcurrency(c.Concurrency)
	result.SetBufferSize(c.BatchSize)
	result.SetName(c.Name)
	return
}

func createConsumerBuilders(c ConsumerBuilderFactoryList) (
	list []*pstore.ConsumerWithMetricsBuilder, err error) {
	result := make([]*pstore.ConsumerWithMetricsBuilder, c.Len())
	nameSet := make(map[string]bool, c.Len())
	for i := range result {
		name := c.NameAt(i)
		if nameSet[name] {
			err = errors.New(fmt.Sprintf("config: Duplicate consumer name found: %s", name))
			return
		}
		nameSet[name] = true
		if result[i], err = c.NewConsumerBuilderByIndex(i); err != nil {
			return
		}
	}
	list = result
	return
}
