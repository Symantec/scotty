// Package mock enables writing metric values to a mockdb for testing.
package mock

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/utils"
)

// FromFile creates a new writer from a configuration file.
func FromFile(filename string) (result pstore.LimitedRecordWriter, err error) {
	var c Config
	if err = utils.ReadFromFile(filename, &c); err != nil {
		return
	}
	return c.NewWriter()
}

// Config represents the configuration of mock db.
type Config struct {
	// Accepted is a list of types that this mock db can record.
	// e.g int64, uint8, float64, etc. If left empty, the mock db will accept
	// all types.
	Accepted []string `yaml:"accepted"`
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
