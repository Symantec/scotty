package utils

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"io"
	"os"
)

// Config implementations may be read from a yaml configuration file.
type Config interface {
	// Reset resets this instance in place
	Reset()
}

// Read uses contents from r to initialize c
func Read(r io.Reader, c Config) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	c.Reset()
	if err := yaml.Unmarshal(content.Bytes(), c); err != nil {
		return err
	}
	return nil
}

// ReadFromFile uses the configuration file stored at filename to
// initialize c.
func ReadFromFile(filename string, c Config) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = Read(f, c); err != nil {
		return err
	}
	return nil
}
