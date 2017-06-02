// Package loadsource provides bogus metrics for load testing.
package loadsource

import (
	"github.com/Symantec/scotty/sources"
)

type Config struct {
	Count int // default is 100
}

func NewConnector(config Config) sources.Connector {
	if config.Count == 0 {
		config.Count = 100
	}
	return &connectorType{config: &config}
}
