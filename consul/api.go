// Package consul integrates scotty with Consul.
package consul

import (
	"github.com/Symantec/scotty/store"
	"log"
)

// NewCoordinator returns a new coordinator for consul. The returned
// coordinator writes any errors encountered to logger as the Lease method
// on returned instance must block until success. The Consul agent runs
// on the local machine at port 8500 so no other configuration is needed.
func NewCoordinator(logger *log.Logger) (store.Coordinator, error) {
	return newCoordinator(logger)
}
