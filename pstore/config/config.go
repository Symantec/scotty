package config

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"os"
	"regexp"
	"sync"
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

type hookWriter struct {
	wrapped pstore.RecordWriter
}

func (h *hookWriter) WriteHook(records []pstore.Record, err error) {
	if err == nil {
		h.wrapped.Write(records)
	}
}

func newWriteHooker(debugMetricRegex, debugHostRegex, debugFilePath string) (
	result pstore.RecordWriteHooker, err error) {
	var fakeWriter pstore.RecordWriter
	fakeWriter, err = newFakeWriterToPath(debugFilePath)
	if err != nil {
		return
	}
	var metricRegex, hostRegex *regexp.Regexp
	if debugMetricRegex != "" {
		metricRegex = regexp.MustCompile(debugMetricRegex)
	}
	if debugHostRegex != "" {
		hostRegex = regexp.MustCompile(debugHostRegex)
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
	return &hookWriter{wrapped: filterWriter}, nil
}

func (c ConsumerConfig) newConsumerBuilder(wf WriterFactory) (
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
	if c.RollUpSpan < 0 {
		c.RollUpSpan = 0
	}
	writer, err := wf.NewWriter()
	if err != nil {
		return
	}
	builder := pstore.NewConsumerWithMetricsBuilder(writer)
	if c.RecordsPerSecond > 0 {
		builder.SetRecordsPerSecond(c.RecordsPerSecond)
	}
	if c.DebugMetricRegex != "" || c.DebugHostRegex != "" {
		var hook pstore.RecordWriteHooker
		hook, err = newWriteHooker(
			c.DebugMetricRegex, c.DebugHostRegex, c.DebugFilePath)
		if err != nil {
			return
		}
		builder.AddHook(hook)
	}
	builder.SetConcurrency(c.Concurrency)
	builder.SetBufferSize(c.BatchSize)
	builder.SetRollUpSpan(c.RollUpSpan)
	builder.SetName(c.Name)
	builder.SetPaused(c.Paused)
	if err = builder.SetRegexesOfMetricsToExclude(
		c.RegexesOfMetricsToExclude); err != nil {
		return
	}
	return builder, nil
}

func (c ConfigList) createConsumerBuilders() (
	list []*pstore.ConsumerWithMetricsBuilder, err error) {
	result := make([]*pstore.ConsumerWithMetricsBuilder, len(c))
	nameSet := make(map[string]bool, len(c))
	for i := range result {
		name := c[i].Consumer.Name
		if nameSet[name] {
			err = errors.New(fmt.Sprintf("config: Duplicate consumer name found: %s", name))
			return
		}
		nameSet[name] = true
		if result[i], err = c[i].NewConsumerBuilder(); err != nil {
			return
		}
	}
	list = result
	return
}
