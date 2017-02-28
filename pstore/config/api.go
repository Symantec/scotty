// Package config includes utilities for handling configuration files.
package config

import (
	"errors"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/influx"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/scotty/pstore/config/mock"
	"github.com/Symantec/scotty/pstore/config/tsdb"
	"io"
	"time"
)

// NewConsumerBuilders creates consumer builders from a reader.
func NewConsumerBuilders(reader io.Reader) (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	var c ConfigList
	if err = yamlutil.Read(reader, &c); err != nil {
		return
	}
	return c.CreateConsumerBuilders()
}

type WriterFactory interface {
	NewWriter() (pstore.LimitedRecordWriter, error)
}

// ConsumerConfig creates a consumer builder
type ConsumerConfig struct {
	// The name of the consumer. Required.
	Name string `yaml:"name"`
	// The number of goroutines doing writing. Optional.
	// A zero value means 1.
	Concurrency uint `yaml:"concurrency"`
	// The number of values written each time. Optional.
	// A zero value means 1000.
	BatchSize uint `yaml:"batchSize"`
	// The length of time for rolling up values when writing. Optional.
	// Zero means write every value and do no rollup.
	RollUpSpan time.Duration `yaml:"rollUpSpan"`
	// Maximum reocrds to write per second. Optional.
	// 0 means no limit.
	RecordsPerSecond uint `yaml:"recordsPerSecond"`
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
	// If true, this consumer is paused
	Paused                    bool     `yaml:"paused"`
	RegexesOfMetricsToExclude []string `yaml:"regexesOfMetricsToExclude"`
}

func (c *ConsumerConfig) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type consumerConfigFields ConsumerConfig
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*consumerConfigFields)(c))
}

// NewConsumerBuilder creates a new consumer builder using the given
// WriterFactory.
func (c *ConsumerConfig) NewConsumerBuilder(wf WriterFactory) (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c.newConsumerBuilder(wf)
}

func (c *ConsumerConfig) Reset() {
	*c = ConsumerConfig{}
}

// ConfigPlus represents one persistent store.
type ConfigPlus struct {
	// One of these pointer fields must be non-nil
	Kafka    *kafka.Config  `yaml:"kafka"`
	Influx   *influx.Config `yaml:"influx"`
	OpenTSDB *tsdb.Config   `yaml:"openTSDB"`
	Mock     *mock.Config   `yaml:"mock"`
	Consumer ConsumerConfig `yaml:"consumer"`
}

func (c *ConfigPlus) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configPlusFields ConfigPlus
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*configPlusFields)(c))
}

func (c *ConfigPlus) NewConsumerBuilder() (
	*pstore.ConsumerWithMetricsBuilder, error) {
	switch {
	case c.Kafka != nil:
		return c.Consumer.NewConsumerBuilder(c.Kafka)
	case c.Influx != nil:
		return c.Consumer.NewConsumerBuilder(c.Influx)
	case c.OpenTSDB != nil:
		return c.Consumer.NewConsumerBuilder(c.OpenTSDB)
	case c.Mock != nil:
		return c.Consumer.NewConsumerBuilder(c.Mock)
	default:
		return nil, errors.New("One writer field must be defined.")
	}
}

func (c *ConfigPlus) Reset() {
	*c = ConfigPlus{}
}

// ConfigList represents a list of persistent stores.
type ConfigList []ConfigPlus

func (c ConfigList) CreateConsumerBuilders() (
	list []*pstore.ConsumerWithMetricsBuilder, err error) {
	return c.createConsumerBuilders()
}

func (c *ConfigList) Reset() {
	*c = nil
}
