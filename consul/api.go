// Package consul integrates scotty with Consul.
package consul

import (
	"errors"
	"github.com/Symantec/scotty/lib/retry"
	"github.com/Symantec/scotty/store"
	"log"
)

// Coordinator represents scotty's connection with consul
type Coordinator struct {
	coord    *coordinator
	listener func(blocked bool)
}

var (
	// ErrMissing indicates that the resource is missing.
	ErrMissing = errors.New("consul: Missing.")
)

var (
	kRetry       retry.Retry
	kCoordinator *Coordinator
)

// The first call to GetCoordinator returns a new coordinator for consul
// that implements store.Coordinator and uses passed logger. Successive
// calls return the same, already created coordinator and ignore the
// logger parameter.
// Blocking methods of the returned coordinator such as the Lease method
// write any errors encountered to logger. If logger is nil, blocking
// methods write errors to stderr. Non blocking methods that return an
// error do not log error messages.
// The Consul agent runs on the local machine at port 8500 so no other
// configuration is needed.
func GetCoordinator(logger *log.Logger) (*Coordinator, error) {
	err := kRetry.Do(func() error {
		result, err := newCoordinator(logger)
		if err != nil {
			return err
		}
		kCoordinator = &Coordinator{coord: result}
		return nil
	})
	return kCoordinator, err
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

// PutPStoreConfig stores a new scotty config file
func (c *Coordinator) PutPStoreConfig(value string) error {
	return c.coord.kernel.Put(kConfigFileKey, value)
}

// GetPStoreConfig gets the current scotty config file. If none exists,
// returns "", ErrMissing
func (c *Coordinator) GetPStoreConfig() (result string, err error) {
	result, ok, _, err := c.coord.kernel.get(kConfigFileKey, 0)
	if err != nil {
		return
	}
	if !ok {
		err = ErrMissing
	}
	return
}

// WatchPStoreConfig returns the contents of the scotty config file in
// returned channel. Each time the config file changes, the returned
// channel emits the entire contents of the file. If the caller calls
// WatchPStoreConfig before the config file is created, the returned
// channel blocks until the config file is created. If the config file
// already exists when the caller calls WatchPStoreConfig, the returned
// channel emits the initial contents of the file immediately.
//
// If done is non-nil, the caller can close done to signal that it wants
// the watch terminated. Termination of the watch closes the returned
// channel. Termination may not happen until several minutes after the
// call closes the done channel.
func (c *Coordinator) WatchPStoreConfig(done <-chan struct{}) <-chan string {
	return c.coord.kernel.Watch(kConfigFileKey, done)
}
