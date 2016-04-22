// Package kafka enables writing metric values to kafka.
package kafka

import (
	"github.com/Symantec/scotty/pstore"
	"io"
)

// NewWriter creates a new writer that writes to kafka endpoints.
func NewWriter(config *Config) (pstore.LimitedRecordWriter, error) {
	return newWriter(config)
}

// NewFakeWriter creates a new writer that dumps the JSON to stdout.
// The tenantId and apiKey are fake for security.
func NewFakeWriter() pstore.RecordWriter {
	return newFakeWriter()
}

// Config represents the configuration of kafka.
type Config struct {
	// The KAFKA endpoints in "hostname:port" format
	Endpoints []string `yaml:"endpoints"`
	// The KAFKA topic
	Topic string `yaml:"topic"`
	// The KAFKA clientId
	ClientId string `yaml:"clientId"`
	// User credential
	TenantId string `yaml:"tenantId"`
	// User credential
	ApiKey string `yaml:"apiKey"`
}

// Read initializes this instance from r, which represents a YAML file.
func (c *Config) Read(r io.Reader) error {
	return c.read(r)
}
