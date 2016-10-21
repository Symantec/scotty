// Package consul integrates scotty with Consul.
package consul

import (
	"github.com/Symantec/scotty/store"
	"log"
)

type Coordinator struct {
	coord    *coordinator
	listener func(blocked bool)
}

// NewCoordinator returns a new coordinator for consul that implements
// store.Coordinator.
// The returned coordinator writes any errors encountered to logger as the
// Lease method on returned instance must block until success.
// The Consul agent runs on the local machine at port 8500 so no other
// configuration is needed.
func NewCoordinator(logger *log.Logger) (*Coordinator, error) {
	result, err := newCoordinator(logger)
	if err != nil {
		return nil, err
	}
	return &Coordinator{coord: result}, nil
}

// Lease implements Lease from store.Coordinator
func (c *Coordinator) Lease(leaseSpanInSeconds, timeToInclude float64) (
	startTimeInclusive, endTimeExclusive float64) {
	return c.coord.Lease(leaseSpanInSeconds, timeToInclude, c.listener)
}

// WithStateListener returns a new view to this same Coordinator that
// monitors state. The Lease method on the returned view calls listener(true)
// if it must block to acquire or extend the lease. Once it has the lease,
// it calls listener(false) before returning. The Lease method on the
// returned view makes no calls to listener if it determines that the
// current lease is viable and can be returned as is.
func (c *Coordinator) WithStateListener(
	listener func(blocked bool)) store.Coordinator {
	result := *c
	result.listener = listener
	return &result
}
