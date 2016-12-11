package yamlutil

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"io"
	"os"
)

// StrictUnmarshalYAML unmarshals YAML storing at structPtr, but returns an
// error if the YAML contains unrecognized top level fields.
//
// unmarshal is what is passed to the standard UnmarshalYAML method.
// structPtr must be a pointer to a struct, not a slice or map.
func StrictUnmarshalYAML(
	unmarshal func(interface{}) error, structPtr interface{}) error {
	return strictUnmarshalYAML(unmarshal, structPtr)
}

// Config implementations may be read from a yaml configuration file.
type Config interface {
	// Reset resets this instance in place making it be the zero value.
	// Read and ReadFromFile call Reset on the passed config instance
	// before reading YAML into it.
	Reset()
}

// Read uses contents from r to initialize c. c is generally a pointer to a
// struct.
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
// initialize c. c is generally a pointer to a struct.
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
