// The cis package handles sending data to CIS.
package cis

import (
	"container/list"
	"github.com/Symantec/scotty/metrics"
	"net"
	"sync"
	"time"
)

// WriteInfo contains info of a single CIS write
type WriteInfo struct {
	// The size of the payload written
	PayloadSize uint64
}

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
	// CIS endpoint. Example. "a.cis.endpoint.net:8080"
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

// Request represents a request sent to CIS
type Request struct {
	// Url to which request was sent
	Url string
	// Payload of request
	Payload string
}

// Response represents a response from CIS
type Response struct {
	// Original request
	Request
	// Http status of response
	Status string
	// Http status code of response
	Code int
	// Non-nil if problem reading response
	Err error
}

// NewAsyncWriter returns a new writer using this instance to write
// to CIS. A separate goroutine calls handler asynchronously whenever a
// response comes in.
func (c *Client) NewAsyncWriter(handler func(r *Response)) (
	*Writer, error) {
	return c.newAsyncWriter(handler)
}

// Writer writes to CIS.
type Writer struct {
	endpoint string
	conn     net.Conn
	handler  func(res *Response)
	// Only one goroutine at a time can write to socket and enqueue.
	// Otherwise, races could cause queued requests to be out of order.
	writeAndEnqueueSemaphore sync.Mutex
	// the queue lock
	lock sync.Mutex
	// Signaled when items are on the queue.
	queueReady sync.Cond
	queue      list.List // list of *Response
}

// Write writes data to CIS asynchronously. If error happens sending,
// Write returns the error. After sending, Write returns nil
// without waiting for response.
func (w *Writer) Write(stats *Stats) error {
	return w.write(stats)
}
