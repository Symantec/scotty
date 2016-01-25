package lmm

import (
	"bytes"
	"errors"
	"gopkg.in/yaml.v2"
	"io"
)

type Config struct {
	Endpoints []string `yaml:"endpoints"`
	Topic     string   `yaml:"topic"`
	TenantId  string   `yaml:"tenantId"`
	ApiKey    string   `yaml:"apiKey"`
}

func (c *Config) Read(r io.Reader) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	*c = Config{}
	if err := yaml.Unmarshal(content.Bytes(), c); err != nil {
		return err
	}
	if !c.hasRequiredFields() {
		return errors.New(
			"endpoint, topic, tenantId, and apiKey keys required")
	}
	return nil
}

func (c *Config) hasRequiredFields() bool {
	return len(c.Endpoints) > 0 && c.Topic != "" && c.TenantId != "" && c.ApiKey != ""
}
