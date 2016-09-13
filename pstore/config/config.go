package config

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

const (
	kDefaultBatchSize = 1000
)

var (
	kFakeWriter = &fakeWriter{}
)

func extractYAMLName(tag string) string {
	if !strings.HasPrefix(tag, "yaml:\"") {
		return ""
	}
	return strings.SplitN(tag[6:len(tag)-1], ",", 2)[0]

}

func extractYAMLFields(ptr interface{}) (result []string) {
	t := reflect.TypeOf(ptr).Elem()
	for i := 0; i < t.NumField(); i++ {
		fieldInfo := t.Field(i)
		if name := extractYAMLName(string(fieldInfo.Tag)); name != "" {
			result = append(result, name)
		} else if fieldInfo.PkgPath == "" {
			result = append(result, strings.ToLower(fieldInfo.Name))
		}
	}
	return
}

func fieldError(nameValues map[string]interface{}) error {
	var unrecognized []string
	for key := range nameValues {
		unrecognized = append(unrecognized, key)
	}
	return errors.New(
		fmt.Sprintf(
			"Unrecognized fields: %s",
			strings.Join(unrecognized, ", ")))
}

func strictUnmarshalYAML(
	unmarshal func(part interface{}) error, structPtr interface{}) error {
	// first unmarshal structPtr in the default way
	if err := unmarshal(structPtr); err != nil {
		return err
	}
	// now unmarshal the same chunk into a map
	var nameValues map[string]interface{}
	if err := unmarshal(&nameValues); err != nil {
		return err
	}
	// For each field in ptr, remove corresponding field from nameValues
	for _, fieldName := range extractYAMLFields(structPtr) {
		delete(nameValues, fieldName)
	}
	// anything left in the nameValues map is an unrecognised field.
	if len(nameValues) != 0 {
		return fieldError(nameValues)
	}
	return nil
}

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
	return builder, nil
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
