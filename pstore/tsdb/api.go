// Package tsdb enables writing metric values to tsdb.
package tsdb

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"time"
)

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

// Config represents the configuration of tsdb.
// Config implements both config.Config and config.WriterFactory
type Config struct {
	// The tsdb endpoint. Required.
	HostAndPort string `yaml:"hostAndPort"`
	// The timeout. Optionsl. Default 30s.
	Timeout time.Duration `yaml:"timeout"`
}

func (c *Config) NewWriter() (pstore.LimitedRecordWriter, error) {
	return newWriter(*c)
}

func (c *Config) Reset() {
	*c = Config{}
}

// ConfigPlus represents an entire tsdb configuration.
// ConfigPlus implements config.Config and config.WriterFactory
type ConfigPlus struct {
	Writer   Config                `yaml:"writer"`
	Options  config.Decorator      `yaml:"options"`
	Consumer config.ConsumerConfig `yaml:"consumer"`
}

func (c *ConfigPlus) NewConsumerBuilder() (
	*pstore.ConsumerWithMetricsBuilder, error) {
	return c.Consumer.NewConsumerBuilder(&c.Writer, &c.Options)
}

func (c *ConfigPlus) Reset() {
	config.Reset(&c.Writer, &c.Options, &c.Consumer)
}

// ConfigList represents a list of entire tsdb configurations.
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
