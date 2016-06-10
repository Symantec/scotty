package config_test

import (
	"bytes"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"testing"
	"time"
)

type nilWriter struct {
}

func (n nilWriter) Write(records []pstore.Record) error {
	return nil
}

func (n nilWriter) IsTypeSupported(kind types.Type) bool {
	return true
}

type configType struct {
	SomeField string `yaml:"someField"`
}

func (c *configType) NewWriter() (pstore.LimitedRecordWriter, error) {
	return nilWriter{}, nil
}

func (c *configType) Reset() {
	*c = configType{}
}

type configPlus struct {
	Writer   configType            `yaml:"writer"`
	Options  config.Decorator      `yaml:"options"`
	Consumer config.ConsumerConfig `yaml:"consumer"`
}

func (c *configPlus) NewConsumerBuilder() (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c.Consumer.NewConsumerBuilder(&c.Writer, &c.Options)
}

func (c *configPlus) Reset() {
	config.Reset(&c.Writer, &c.Options, &c.Consumer)
}

type configList []configPlus

func (c configList) Len() int {
	return len(c)
}

func (c configList) NewConsumerBuilderByIndex(i int) (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c[i].NewConsumerBuilder()
}

func (c configList) NameAt(i int) string {
	return c[i].Consumer.Name
}

func (c *configList) Reset() {
	*c = nil
}

func consumerBuildersFromString(s string) (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	buffer := bytes.NewBuffer(([]byte)(s))
	var c configList
	if err = config.Read(buffer, &c); err != nil {
		return
	}
	return config.CreateConsumerBuilders(c)
}

func buildAll(list []*pstore.ConsumerWithMetricsBuilder) (
	result []*pstore.ConsumerWithMetrics) {
	result = make([]*pstore.ConsumerWithMetrics, len(list))
	for i := range result {
		result[i] = list[i].Build()
	}
	return
}

func TestReadConfig(t *testing.T) {
	configFile := `
# a comment
- writer:
    someField: "some value"
  options:
    recordsPerSecond: 124
  consumer:
    name: "some name"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- writer:
    someField: "hello"
  consumer:
    name: "minimal"
- writer:
    someField: "another"
  options:
    recordsPerSecond: -235
  consumer:
    concurrency: -2
    name: "negative"
    batchSize: -17
    rollUpSpan: -2m
`
	consumerBuilders, err := consumerBuildersFromString(configFile)
	if err != nil {
		t.Fatal(err)
	}
	consumers := buildAll(consumerBuilders)
	assertValueEquals(t, 3, len(consumers))
	assertValueEquals(t, "some name", consumers[0].Name())
	var attributes pstore.ConsumerAttributes
	consumers[0].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency:      7,
			BatchSize:        730,
			RecordsPerSecond: 124,
			RollUpSpan:       135 * time.Second,
		},
		attributes)
	assertValueEquals(t, "minimal", consumers[1].Name())
	consumers[1].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency: 1,
			BatchSize:   1000,
		},
		attributes)
	assertValueEquals(t, "negative", consumers[2].Name())
	consumers[2].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency: 1,
			BatchSize:   1000,
		},
		attributes)
}

func TestReadConfigDupName(t *testing.T) {
	configFile := `
# a comment
- writer:
    someField: "some value"
  options:
    recordsPerSecond: 124
  consumer:
    name: "some name"
    concurrency: 7
    batchSize: 730
- writer:
    someField: "hello"
  consumer:
    name: "minimal"
- writer:
    someField: "another"
  options:
    recordsPerSecond: -235
  consumer:
    concurrency: -2
    # dup name
    name: "minimal"
    batchSize: -17
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected an error, got nil.")
	}
}

func TestConfigMissingName(t *testing.T) {
	configFile := `
# a comment
- writer:
    someField: "some value"
  options:
    recordsPerSecond: 124
  consumer:
    name: "some name"
    concurrency: 7
    batchSize: 730
- writer:
    someField: "hello"
  consumer:
    name: "minimal"
- writer:
    someField: "another"
  options:
    recordsPerSecond: -235
  consumer:
    concurrency: -2
    # missing name
    batchSize: -17
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected an error, got nil.")
	}
}

func assertValueEquals(
	t *testing.T,
	expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
