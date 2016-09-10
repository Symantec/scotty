// Package kafka enables writing metric values to kafka.
package kafka

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"github.com/Symantec/tricorder/go/tricorder/types"
)

// IsTypeSupported returns true if kafka supports the given metric type
func IsTypeSupported(t types.Type) bool {
	return isTypeSupported(t)
}

// ToFloat64 converts the metric value in r to a floating point value for kafka.
func ToFloat64(r *pstore.Record) float64 {
	return asFloat64(r)
}

// FromFile creates a new writer from a configuration file.
func FromFile(filename string) (result pstore.LimitedRecordWriter, err error) {
	var c Config
	if err = config.ReadFromFile(filename, &c); err != nil {
		return
	}
	return c.NewWriter()
}

// ConsumerBuildersFromFile creates consumer builders from a configuration file.
func ConsumerBuildersFromFile(filename string) (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	var c ConfigList
	if err = config.ReadFromFile(filename, &c); err != nil {
		return
	}
	return config.CreateConsumerBuilders(c)
}

// NewFakeWriter creates a new writer that dumps the JSON to stdout.
// The tenantId and apiKey are fake for security.
func NewFakeWriter() pstore.LimitedRecordWriter {
	return newFakeWriter()
}

// NewFakeWriterToPath creates a new writer that dumps the JSON to a file
// with given path. The tenantId and apiKey are fake for security.
func NewFakeWriterToPath(path string) (pstore.LimitedRecordWriter, error) {
	return newFakeWriterToPath(path)
}

// Config represents the configuration of kafka.
// Config implements both config.Config and config.WriterFactory
type Config struct {
	// The KAFKA endpoints in "hostname:port" format.
	// At least one is required.
	Endpoints []string `yaml:"endpoints"`
	// The KAFKA topic. Required.
	Topic string `yaml:"topic"`
	// The KAFKA clientId. Required.
	ClientId string `yaml:"clientId"`
	// User credential. Required.
	TenantId string `yaml:"tenantId"`
	// User credential. Required.
	ApiKey string `yaml:"apiKey"`
}

func (c *Config) NewWriter() (pstore.LimitedRecordWriter, error) {
	return newWriter(*c)
}

func (c *Config) Reset() {
	*c = Config{}
}

// ConfigPlus represents an entire kafka configuration.
// ConfigPlus implements config.Config and config.WriterFactory
type ConfigPlus struct {
	Writer   Config                `yaml:"writer"`
	Consumer config.ConsumerConfig `yaml:"consumer"`
}

func (c *ConfigPlus) NewConsumerBuilder() (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c.Consumer.NewConsumerBuilder(&c.Writer)
}

func (c *ConfigPlus) Reset() {
	config.Reset(&c.Writer, &c.Consumer)
}

// ConfigList represents a list of entire kafka configurations.
// ConfigList implements config.Config and config.ConsumerBuilderFactoryList
type ConfigList []ConfigPlus

func (c ConfigList) Len() int {
	return len(c)
}

func (c ConfigList) NewConsumerBuilderByIndex(i int) (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c[i].NewConsumerBuilder()
}

func (c ConfigList) NameAt(i int) string {
	return c[i].Consumer.Name
}

func (c *ConfigList) Reset() {
	*c = nil
}
