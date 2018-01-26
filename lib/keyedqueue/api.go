// Package keyedqueue provides a queue with keyed elements allowing elements
// already on the queue to be easily replaced.
package keyedqueue

import (
	"container/list"
	"sync"
)

// Element represents a queue element.
type Element interface {
	// Key returns the key of this element. Returned key must support
	// equality.
	Key() interface{}
}

// Queue represents a queue of elements. Queue instances are safe to use
// with multiple goroutines.
type Queue struct {
	lock       sync.Mutex
	elementsOn sync.Cond
	queue      list.List
	byKey      map[interface{}]*list.Element
}

// New returns a brand new, empty queue.
func New() *Queue {
	result := &Queue{}
	result.elementsOn.L = &result.lock
	result.queue.Init()
	result.byKey = make(map[interface{}]*list.Element)
	return result
}

// Len returns the length of this queue
func (q *Queue) Len() int {
	return q.length()
}

// Add replaces the element in the queue with key matching elementToAdd with
// elementToAdd. elementToAdd gets the same position in the queue as the
// element it replaced. If no element with a matching key exists in the queue,
// Add places elementToAdd at the end of the queue.
func (q *Queue) Add(elementToAdd Element) {
	q.add(elementToAdd)
}

// Remove removes the element at the front of the queue. If queue is empty,
// Remove blocks.
func (q *Queue) Remove() Element {
	return q.remove()
}

// Peek returns the element at the front of the queue without removing it.
// If queue is empty, Peek blocks.
func (q *Queue) Peek() Element {
	return q.peek()
}
