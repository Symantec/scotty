// Package pool provides pools for io.Closer instances.
//
// The pools automatically close instances no longer in use. Clients should
// never call Close() directly on a pool managed closer.
//
// Code works like this:
// 	p := pool.New...(...)
//	...
// 	func someFunc() {
// 		id, closer := p.Get()
// 		defer p.Put(id)
// 		doSomethingWithCloserWithoutClosingIt(closer)
// 	}
package pool

import (
	"io"
	"sync"
)

// SingleResource is a pool of a single closer.
// Client can change the underlying closer at any time.
type SingleResource struct {
	mu        sync.Mutex
	closers   map[uint64]*singleResourceType
	currentId uint64
}

// NewSingleResource creates a new instance containing just closer.
func NewSingleResource(closer io.Closer) *SingleResource {
	return &SingleResource{
		closers: map[uint64]*singleResourceType{
			0: &singleResourceType{closer: closer},
		},
	}
}

// Set sets the closer in this instance
func (r *SingleResource) Set(closer io.Closer) {
	r.set(closer)
}

// Get returns the id and closer of the current closer in this instance.
// Client should follow each Get call with a deferred Put() call as in the
// package example.
func (r *SingleResource) Get() (uint64, io.Closer) {
	return r.get()
}

// Put signals to this instance that caller is done using the associated
// closer. id is the same id that Get() returned.
func (r *SingleResource) Put(id uint64) {
	r.put(id)
}
