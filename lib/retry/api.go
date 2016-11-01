package retry

import (
	"sync"
	"sync/atomic"
)

// Retry is an object that will perform an action until it succeeds.
type Retry struct {
	m    sync.Mutex
	done uint32
}

// Do calls f once and returns the result of f until f returns nil. Once f
// return nil, calling Do on this instance no longer calls f even if f changes
// but simply returns nil. Calling Do on this instance with multiple
// goroutines gates calls to f so that only only one goroutine at a time
// calls f while the others block.
func (r *Retry) Do(f func() error) error {
	if atomic.LoadUint32(&r.done) == 1 {
		return nil
	}
	// slow-path
	r.m.Lock()
	defer r.m.Unlock()
	if r.done == 0 {
		err := f()
		if err != nil {
			return err
		}
		atomic.StoreUint32(&r.done, 1)
	}
	return nil
}
