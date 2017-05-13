// Package dynconfig provides routines for implementing dynamic configuration
// files.
package dynconfig

import (
	"github.com/Symantec/Dominator/lib/log"
	"io"
	"sync"
)

// DynConfig represents a dynamic configuration file.
// Caller creates a DynConfig instance by caling New or NewInitialized at
// startup. Then caller keeps the DynConfig instance around throughout the
// duration of execution. When caller needs the end product of the
// configuration file, caller calls Get() on the DynConfig instance.
type DynConfig struct {
	path    string
	builder func(io.Reader) (interface{}, error)
	name    string
	logger  log.Logger
	mu      sync.Mutex
	value   interface{}
}

// New creates a new DynConfig instance.
// path is the absolute path to the configuration file.

// builder builds the end product from the configuration file contents. builder
// takes the contents of the configuration file as input and outputs either
// the final end product or an error. builder is called each time the
// configuration file changes.
//
// name is the the name of the configuration file and is used as a log prefix
// logger is where any errors reading the configuration get logged.
func New(
	path string,
	builder func(io.Reader) (interface{}, error),
	name string,
	logger log.Logger) *DynConfig {
	return newDynConfig(path, builder, name, logger, nil)
}

// NewInitialized works like New except that if the configuration file doesn't
// exist or the end product can't be built, it returns an error
// instead of a new DynConfig instance. If NewInitialized returns a DynConfig
// instance, its Get method is guaranteed to return a non-nil value.
func NewInitialized(
	path string,
	builder func(io.Reader) (interface{}, error),
	name string,
	logger log.Logger) (*DynConfig, error) {
	return newInitialized(path, builder, name, logger)
}

// Get returns the end product of the configuration file. If the configuration
// file changes during runtime, Get will return a different end product that
// reflects the updated configuration file. If the configuration file is
// updated so that it contains an error such that no end product can be
// created from it, Get continues to return the previous end product. If the
// DynConfig instance was created with New and the config file doesn't exist
// or couldn't be parsed initially, then Get returns nil until such time that
// the configuration file errors get fixed.
func (d *DynConfig) Get() interface{} {
	return d.get()
}
