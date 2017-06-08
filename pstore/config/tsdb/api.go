// Package tsdb enables writing metric values to tsdb.
package tsdb

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
)

// FromFile creates a new writer from a configuration file.
func FromFile(filename string) (result pstore.LimitedRecordWriter, err error) {
	var c Config
	if err = yamlutil.ReadFromFile(filename, &c); err != nil {
		return
	}
	return c.NewWriter()
}

// Config represents the configuration of tsdb.
// Config implements both config.Config and config.WriterFactory
type Config struct {
	// The tsdb endpoint. Required. Looks like "somehost:4242"
	HostAndPort string `yaml:"hostAndPort"`
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
