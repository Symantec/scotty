// Package sources provides the interfaces for metric sources.
package sources

import (
	"github.com/Symantec/scotty/metrics"
)

// Config controls how a Connector connects.
type Config struct {
	IsTls bool // If true, Connector connects using TLS
}

// Connector connects to a particular type of source.
// Connector instances must be safe to use with multiple goroutines.
type Connector interface {
	Connect(host string, port uint, config Config) (Poller, error)
	Name() string
}

// ConnectorList is a list of connectors.
// First element of list is most prefered; last element is least preferred.
// Instances of this type must be treated as immutable.
type ConnectorList []Connector

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
// the NewResource and ResourceConnect methods instead of the Connect method.
type ResourceConnector interface {
	Connector

	// Creates a long lived resource from host and port. Returned resource
	// can be passed to ResourceConnect again and again amortizing the cost
	// of connecting.
	NewResource(host string, port uint, config Config) Resource

	ResourceConnect(resource Resource) (Poller, error)
}

// MultiResourceConnector makes a ResourceConnector out of a ConnectorList.
// consecutiveCallsForReset is how many times an alternate Connector must
// be used through a Resource created from this instance before the first
// Connector is tried again. See preference.New in
// github.com/Symantec/scotty/lib/preference.
func MultiResourceConnector(
	conns ConnectorList, consecutiveCallsForReset int) ResourceConnector {
	return multiResourceConnector(conns, consecutiveCallsForReset)
}

// AsResourceConnector upgrades conn to a ResourceConnector.
func AsResourceConnector(conn Connector) ResourceConnector {
	return simpleResourceConnector(conn)
}

// Poller polls metrics from a particular source. Generally, a Poller
// instance may be polled once and then must be closed.
type Poller interface {
	Poll() (metrics.List, error)
	Close() error
}
