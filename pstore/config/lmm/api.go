// Package lmm enables writing metric values to lmm.
package lmm

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
)

// Config represents the configuration of lmm.
// Config implements yamlutil.Config
type Config struct {
	// LMM rest API endpont. Required
	Endpoint string `yaml:"endpoint"`
	// User credential. Required.
	TenantId string `yaml:"tenantId"`
	// User credential. Required.
	ApiKey string `yaml:"apiKey"`
	// Name for metrics
	Name string `yaml:"name"`
	//  AWS region
	Region string `yaml:"region"`
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
