// Package config includes utilities for handling configuration files.
package config

import (
	"bytes"
	"github.com/Symantec/scotty/pstore"
	"gopkg.in/yaml.v2"
	"io"
	"os"
)

// Config implementations may be read from a yaml configuration file.
type Config interface {
	// Reset resets this instance in place
	Reset()
}

// Read uses contents from r to initialize c
func Read(r io.Reader, c Config) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	c.Reset()
	if err := yaml.Unmarshal(content.Bytes(), c); err != nil {
		return err
	}
	return nil
}

// ReadFromFile uses the configuration file stored at filename to
// initialize c.
func ReadFromFile(filename string, c Config) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = Read(f, c); err != nil {
		return err
	}
	return nil
}

type WriterFactory interface {
	NewWriter() (pstore.LimitedRecordWriter, error)
}

// List represents a list of items read from a configuration file
type List interface {
	// Len returns the number of items in the list.
	Len() int
}

// ConsumerBuilderFactoryList instances contain a list of items for creating
// consumer builders.
type ConsumerBuilderFactoryList interface {
	List
	// Create a Consumer builder from item at given index.
	NewConsumerBuilderByIndex(idx int) (
		*pstore.ConsumerWithMetricsBuilder, error)
	// Returns the name of the item at the given index.
	NameAt(idx int) string
}

// CreateConsumerBuilders creates consumer builders from c.
func CreateConsumerBuilders(c ConsumerBuilderFactoryList) (
	list []*pstore.ConsumerWithMetricsBuilder, err error) {
	return createConsumerBuilders(c)
}

// Reset resets all the configs.
func Reset(configs ...Config) {
	for i := range configs {
		configs[i].Reset()
	}
}

// Decorator creates a decorated writer.
type Decorator struct {
	// Maximum reocrds to write per minute. Optional.
	// 0 or negative means no limit.
	RecordsPerMinute int `yaml:"recordsPerMinute"`
	// Metrics whose name matches DebugMetricRegex AND whose host matches
	// DebugHostRegex are written to the debug file. Empty values in
	// both of these fields means no debugging. An empty value in
	// one of these fields means ignore it when deciding if a metric
	// matches.
	DebugMetricRegex string `yaml:"debugMetricRegex"`
	DebugHostRegex   string `yaml:"debugHostRegex"`
	// The full path of the debug file. Optional.
	// If empty, debug goes to stdout.
	DebugFilePath string `yaml:"debugFilePath"`
}

// NewWriter creates a new, decorated writer using the given WriterFactory
func (d *Decorator) NewWriter(wf WriterFactory) (
	pstore.LimitedRecordWriter, error) {
	return d.newWriter(wf)
}

func (d *Decorator) Reset() {
	*d = Decorator{}
}

// ConsumerConfig creates a consumer builder
type ConsumerConfig struct {
	// The name of the consumer. Required.
	Name string `yaml:"name"`
	// The number of goroutines doing writing. Optional.
	// A zero or negative value means 1.
	Concurrency int `yaml:"concurrency"`
	// The number of values written each time. Optional.
	// A zero or negative value means 1000.
	BatchSize int `yaml:"batchSize"`
}

// NewConsumerBuilder creates a new consumer builder using the given
// WriterFactory.
func (c *ConsumerConfig) NewConsumerBuilder(
	wf WriterFactory) (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c.newConsumerBuilder(wf)
}

func (c *ConsumerConfig) Reset() {
	*c = ConsumerConfig{}
}
