// The cis package handles sending data to CIS.
package cis

import (
	"github.com/Symantec/scotty/lib/queuesender"
	"github.com/Symantec/scotty/lib/synchttp"
	"github.com/Symantec/scotty/metrics"
	"sync"
	"time"
)

// PackageEntry represents a single package on a machine
// This type should support ==
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

// Equals returns true if p is equivalent to rhs.
func (p *PackageInfo) Equals(rhs *PackageInfo) bool {
	return p.equals(rhs)
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
// extract instanceId from the health agent. GetStats returns nil if it
// cannot find any CIS data.
func GetStats(list metrics.List, optInstanceId string) *Stats {
	return getStats(list, optInstanceId)
}

// Config represents the configuration for connecting to CIS
type Config struct {
	// CIS endpoint. Example. "http://a.cis.endpoint.net:8080"
	Endpoint string
	// Required: data center e.g us-east-1.
	DataCenter string
	// Optional: Name for writing tricorder metrics for async http.
	// If ommitted, writing is synchronous.
	Name string
}

// Client represents a client connection to CIS. Client instances are safe
// to use with multiple goroutines.
type Client struct {
	endpoint   string
	dataCenter string
	sync       synchttp.JSONWriter
	async      *queuesender.Sender
}

// NewClient returns a new CIS client instance.
func NewClient(config Config) (*Client, error) {
	if config.Name != "" {
		sender, err := queuesender.New(config.Endpoint, 2000, "", nil)
		if err != nil {
			return nil, err
		}
		sender.Register(config.Name)
		return &Client{
			endpoint:   config.Endpoint,
			dataCenter: config.DataCenter,
			async:      sender,
		}, nil
	} else {
		writer, err := synchttp.NewSyncJSONWriter()
		if err != nil {
			return nil, err
		}
		return &Client{
			endpoint:   config.Endpoint,
			dataCenter: config.DataCenter,
			sync:       writer,
		}, nil
	}
}

// Write writes data to CIS
func (c *Client) Write(stats *Stats) error {
	return c.write(stats)
}

// WriteAll batch writes data to CIS.
func (c *Client) WriteAll(stats []Stats) error {
	return c.writeAll(stats)
}

// BulkWriter is the interface for batch writing data to CIS.
type BulkWriter interface {
	WriteAll(stats []Stats) error
}

// Buffered buffers CIS data for bulk writing.
type Buffered struct {
	mu        sync.Mutex
	statsList []Stats
	writer    BulkWriter
}

// NewBuffered creates a new Buffered instance.
// size is the size of the buffer; writer is the instrument that writes
// the data to CIS. Buffered instances are safe to use with multiple
// goroutines.
func NewBuffered(size int, writer BulkWriter) *Buffered {
	return &Buffered{
		statsList: make([]Stats, 0, size),
		writer:    writer,
	}
}

// Write buffers data. When size pieces of data are buffered, Write bulk
// clears the buffer, and returns the pieces written or the pieces that would
// have been written if there was any error.
// When simply adding to the buffer instead of bulk writing, Write always
// returns nil, nil.
func (b *Buffered) Write(stats Stats) ([]Stats, error) {
	return b.write(stats)
}

// Flush writes out the contents of the buffer, clears the buffer, and
// returns the pieces written or the pieces that would have been written
// if there was any error.
func (b *Buffered) Flush() ([]Stats, error) {
	return b.flush()
}
