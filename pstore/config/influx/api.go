// Package influx enables writing metric values to influxdb.
package influx

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

// Config represents the configuration of influx db.
// Config implements utils.Config
type Config struct {
	// The influxdb endpoint. Required.
	HostAndPort string `yaml:"hostAndPort"`
	// The user name. Optional.
	UserName string `yaml:"username"`
	// The password. Optional.
	Password string `yaml:"password"`
	// The database name. Required.
	Database string `yaml:"database"`
	// The precision. Optional. Defaults to "ns"
	Precision string `yaml:"precision"`
	// The retention policy to use when writing. Optional.
	RetentionPolicy string `yaml:"retentionPolicy"`
	// The write consistency. Optional. "any", "one", "quorum", or "all"
	WriteConsistency string `yaml:"writeConsistency"`
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
