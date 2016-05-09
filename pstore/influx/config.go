package influx

import (
	"bytes"
	"errors"
	"gopkg.in/yaml.v2"
	"io"
)

func (e *Endpoint) checkRequiredFields() (err error) {
	if e.HostAndPort == "" {
		return errors.New("HostAndPort required field in endpoint.")
	}
	return
}

func (c *Config) read(r io.Reader) (err error) {
	var content bytes.Buffer
	if _, err = content.ReadFrom(r); err != nil {
		return
	}
	*c = Config{}
	if err = yaml.Unmarshal(content.Bytes(), c); err != nil {
		return
	}
	if err = c.checkRequiredFields(); err != nil {
		return
	}
	return
}

func (c *Config) checkRequiredFields() (err error) {
	for i := range c.Endpoints {
		if err = c.Endpoints[i].checkRequiredFields(); err != nil {
			return
		}
	}
	if len(c.Endpoints) == 0 || c.Database == "" {
		return errors.New(
			"Endpoints and Database fields required.")
	}
	return
}
