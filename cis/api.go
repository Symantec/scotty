// The cis package handles sending data to CIS.
package cis

import (
	"github.com/Symantec/scotty/lib/queuesender"
	"github.com/Symantec/scotty/lib/synchttp"
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
// GetStats uses the optInstanceId if supplied. Otherwise, it tries to
// extract instanceId from the health agent.
func GetStats(list metrics.List, optInstanceId string) *Stats {
	return getStats(list, optInstanceId)

}

// Config represents the configuration for connecting to CIS
type Config struct {
	// CIS endpoint. Example. "http://a.cis.endpoint.net:8080"
	Endpoint string
	// Name for writing tricorder metrics
	Name string
}

// Client represents a client connection to CIS
type Client struct {
	endpoint string
	sync     synchttp.JSONWriter
	async    *queuesender.Sender
}

// NewClient returns a new CIS client instance.
func NewClient(config *Config) (*Client, error) {
	if config.Name != "" {
		sender, err := queuesender.New(config.Endpoint, 2000, "", nil)
		if err != nil {
			return nil, err
		}
		sender.Register(config.Name)
		return &Client{
			endpoint: config.Endpoint,
			async:    sender,
		}, nil
	} else {
		writer, err := synchttp.NewSyncJSONWriter()
		if err != nil {
			return nil, err
		}
		return &Client{
			endpoint: config.Endpoint,
			sync:     writer,
		}, nil
	}
}

// Write writes data to CIS
func (c *Client) Write(stats *Stats) error {
	return c.write(stats)
}
