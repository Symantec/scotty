// Package kafka enables writing metric values to kafka.
package kafka

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/utils"
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
	if err = utils.ReadFromFile(filename, &c); err != nil {
		return
	}
	return c.NewWriter()
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
// Config implements utils.Config
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

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configFields Config
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*configFields)(c))
}

func (c *Config) NewWriter() (pstore.LimitedRecordWriter, error) {
	return newWriter(*c)
}

func (c *Config) Reset() {
	*c = Config{}
}
