// package config includes utilities for handling configuration files
package config

import (
	"io"
	"os"
)

// Implementations can read their state from a configuration file
type Config interface {
	Read(f io.Reader) error
}

// Read reads the content of filename into config
func Read(filename string, c Config) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = c.Read(f); err != nil {
		return err
	}
	return nil
}
