// The cis package handles sending data to CIS.
package cis

import (
	"github.com/Symantec/scotty/metrics"
	"time"
)

// PackageEntry represents a single package on a machine
type PackageEntry struct {
	// Package name e.g python3.4
	Name string
	// Package version e.g 3.4
	Version string
	// Package size
	Size uint64
}

// PackageInfo represents all the packages on a machine
type PackageInfo struct {
	// The packaging type e.g debian.
	ManagementType string
	// The individual packages
	Packages []PackageEntry
}

// Stats represents fetching package data for a machine
type Stats struct {
	// Time the data were fetched
	TimeStamp time.Time
	// The AWS instance ID of the machine
	InstanceId string
	// The packags on the machine
	Packages PackageInfo
}

// Key keys by the instance ID
func (s *Stats) Key() interface{} {
	return s.InstanceId
}

// GetStats extracts the CIS data from the metrics pulled for a machine
func GetStats(list metrics.List) *Stats {
	return getStats(list)

}

// Config represents the configuration for connecting to CIS
type Config struct {
	// CIS endpoint. Example. "http://a.cis.endpoint.net:8080"
	Endpoint string
}

// Client represents a client connection to CIS
type Client struct {
	endpoint string
}

// NewClient returns a new CIS client instance.
func NewClient(config *Config) (*Client, error) {
	return &Client{endpoint: config.Endpoint}, nil
}

// Write writes data to CIS
func (c *Client) Write(stats *Stats) error {
	return c.write(stats)
}
