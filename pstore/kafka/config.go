package kafka

import (
	"bytes"
	"errors"
	"gopkg.in/yaml.v2"
	"io"
)

func (c *Config) read(r io.Reader) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	*c = Config{}
	if err := yaml.Unmarshal(content.Bytes(), c); err != nil {
		return err
	}
	if err := c.checkRequiredFields(); err != nil {
		return err
	}
	return nil
}

func (c *Config) checkRequiredFields() error {
	if len(c.Endpoints) == 0 || c.Topic == "" || c.ClientId == "" || c.TenantId == "" || c.ApiKey == "" {
		return errors.New(
			"endpoint, topic, clientId, tenantId, and apiKey keys required")
	}
	return nil
}
