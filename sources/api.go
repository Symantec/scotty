// Package sources provides the interfaces for metric sources.
package sources

import (
	"github.com/Symantec/scotty/metrics"
)

// Connector connects to a particular type of source.
// Connector implementations must be safe to use with multiple goroutines.
type Connector interface {
	Connect(host string, port uint) (Poller, error)
	Name() string
}

// Poller polls metrics from a particular source
type Poller interface {
	Poll() (metrics.List, error)
	Close() error
}
