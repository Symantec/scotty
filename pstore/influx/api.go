// Package influx enables writing metric values to influxdb.
package influx

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"io"
)

// NewWriter creates a new writer that writes to influx database(s).
func NewWriter(config Config) (pstore.LimitedRecordWriter, error) {
	return newWriter(&config)
}

// FromFile creates a new writer from a configuration file.
func FromFile(filename string) (result pstore.LimitedRecordWriter, err error) {
	var c Config
	if err = config.Read(filename, &c); err != nil {
		return
	}
	return NewWriter(c)
}

type Endpoint struct {
	HostAndPort string `yaml:"hostAndPort"`
	UserName    string `yaml:"username"`
	Password    string `yaml:"password"`
}

// Config represents the configuration of kafka.
type Config struct {
	Endpoints []Endpoint `yaml:"endpoints"`
	Database  string     `yaml:"database"`
}

// Read initializes this instance from r, which represents a YAML file.
func (c *Config) Read(r io.Reader) error {
	return c.read(r)
}
