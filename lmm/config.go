package lmm

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Config struct {
	Endpoints []string
	Topic     string
	TenantId  string
	ApiKey    string
}

func (c *Config) Read(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	requiredKeys := map[string]bool{
		"endpoint": true,
		"topic":    true,
		"tenantId": true,
		"apiKey":   true,
	}
	c.Endpoints = nil
	for scanner.Scan() {
		keyAndValue := strings.SplitN(
			strings.TrimSpace(scanner.Text()), "\t", 2)
		if len(keyAndValue) < 2 {
			continue
		}
		delete(requiredKeys, keyAndValue[0])
		switch keyAndValue[0] {
		case "endpoint":
			c.Endpoints = append(c.Endpoints, keyAndValue[1])
		case "topic":
			c.Topic = keyAndValue[1]
		case "tenantId":
			c.TenantId = keyAndValue[1]
		case "apiKey":
			c.ApiKey = keyAndValue[1]
		default:
			return errors.New(
				fmt.Sprintf("Key %s is invalid", keyAndValue[0]))
		}
	}
	err := scanner.Err()
	if err != nil {
		return err
	}
	if len(requiredKeys) > 0 {
		return errors.New(
			"endpoint, topic, tenantId, and apiKey keys required")
	}
	return nil
}
