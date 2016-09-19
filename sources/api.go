// Package sources provides the interfaces for metric sources.
package sources

import (
	"github.com/Symantec/scotty/metrics"
)

// Connector connects to a particular type of source.
// Connector instances must be safe to use with multiple goroutines.
type Connector interface {
	Connect(host string, port uint) (Poller, error)
	Name() string
}

// Resource represents a resource to connect to.
//
// Resource instances are long lived and therefore can amortize the cost of
// opening up a connection again and again from scratch using a host and port.
// A particular Resource instance is valid only for the ResourceConnector
// that created it.
type Resource interface{}

// ResourceConnector is implemented by Connector instances that support
// amortizing the cost of connecting. Clients should check at runtime if a
// Connector instance implements this interface. If it does they should use
// use the instance with this interface instead of the Connector interface.
// Like Connector, ResourceConnector instances must be safe to use with
// multiple goroutines.
type ResourceConnector interface {
	NewResource(host string, port uint) Resource
	ResourceConnect(resource Resource) (Poller, error)
	Name() string
}

// Poller polls metrics from a particular source. Generally, a Poller
// instance may be polled once and then must be closed.
type Poller interface {
	Poll() (metrics.List, error)
	Close() error
}
