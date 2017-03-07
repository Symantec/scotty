package pool

import (
	"io"
)

type singleResourceType struct {
	closer io.Closer
	count  int
	done   bool
}

func (s *singleResourceType) Open() io.Closer {
	s.count++
	return s.closer
}

func (s *singleResourceType) MarkDone() {
	s.done = true
	if s.count == 0 {
		go s.closer.Close()
	}
}

func (s *singleResourceType) Close() bool {
	s.count--
	if s.count < 0 {
		panic("Reference count can't be negative")
	}
	if s.count == 0 && s.done == true {
		go s.closer.Close()
		return true
	}
	return false
}

func (r *SingleResource) set(closer io.Closer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closers[r.currentId].MarkDone()
	r.currentId++
	r.closers[r.currentId] = &singleResourceType{closer: closer}
}

func (r *SingleResource) get() (uint64, io.Closer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentId, r.closers[r.currentId].Open()
}

func (r *SingleResource) put(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closers[id].Close() {
		delete(r.closers, id)
	}
}
