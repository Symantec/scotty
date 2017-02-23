package queuesender

import (
	"sync"
)

// queueType is a circular queue of fixed length for sending JSON requests
// asynchronously. These queues have 3 pointers:
//
// the front of the queue where the next sent request to receive a response
// resides.
//
// the back of the queue where new requests to be sent go.
//
// the next not sent pointer which falls somewhere between the front and back
// of the queue.
//
// Requests get added to the back of the queue. At some point the next not
// sent pointer reaches the added request, and the request is sent. Later
// the response for that request comes, and the request is popped off the
// front of the queue.
type queueType struct {
	length      int
	lock        sync.Mutex
	cond        sync.Cond
	data        []requestType
	start       int
	end         int
	nextNotSent int
	connId      int
}

// newQueue returns a new queue capable of holding length requests.
func newQueue(length int) *queueType {
	result := &queueType{length: length + 1}
	result.cond.L = &result.lock
	result.data = make([]requestType, length+1)
	return result
}

func (q *queueType) Cap() int {
	return q.length - 1
}

func (q *queueType) Len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	result := q.end - q.start
	if result < 0 {
		result += q.length
	}
	return result
}

func (q *queueType) Sent() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	result := q.nextNotSent - q.start
	if result < 0 {
		result += q.length
	}
	return result
}

// Add adds r to the back of the queue. Add blocks if queue is full
func (q *queueType) Add(r requestType) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for q.isFull() {
		q.cond.Wait()
	}
	q.data[q.end] = r
	q.incEnd()
	q.cond.Broadcast()
}

// DiscardNextSent pops the next item off the front of the queue.
// Called when a response comes in
// DiscardNextSent blocks if the queue is empty
func (q *queueType) DiscardNextSent() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.isEmpty() {
		q.cond.Wait()
	}
	q.incStart()
	q.cond.Broadcast()
}

// MoveToNotSent is for when an error comes back in the response.
// MoveToNotSent moves the item at the front of the queue to the back of
// the queue atomially. MoveToNotSent blocks if queue is empty.
func (q *queueType) MoveToNotSent() requestType {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.isEmpty() {
		q.cond.Wait()
	}
	q.data[q.end] = q.data[q.start]
	q.incEnd()
	q.incStart()
	q.cond.Broadcast()
	return q.data[q.end]
}

// NextNotSent returns the next item in the queue not yet sent and moves
// the next not sent pointer forward. NextNotSent blocks if nothing remains
// in the queue that hasn't been sent.
//
// NextNotSent depends on the current connection. Whenever the connection is
// refreshed, NextNotSent moves to the front of the queue. The connId
// argument helps protect against data races. The passed connId must match
// the ID of the connection this queue is tracking. A connId that is too big
// means the caller is asking for the next not sent pointer for a future
// connection. In this case, NextNotSent blocks until the connection Ids match.
// A connId that is to small means that the caller is asking for the next not
// sent pointer of a connection that has been closed. In this case,
// NextNotSent immediately returns false.
func (q *queueType) NextNotSent(connId int) (result requestType, ok bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for connId > q.connId {
		q.cond.Wait()
	}
	for connId == q.connId && q.noNotSent() {
		q.cond.Wait()
	}
	if connId < q.connId {
		return
	}
	result = q.data[q.nextNotSent]
	ok = true
	q.incNextNotSent()
	q.cond.Broadcast()
	return
}

// Calling ResetNextNotSent indicates the most current connection has changed.
// ResetNextNotSent moves the next not sent pointer to the front of the
// queue and updates the connection id being tracked while unblocking any
// pending calls for the older connection.
func (q *queueType) ResetNextNotSent(newConnId int) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if newConnId <= q.connId {
		panic("Connection Ids must be monotone increasing")
	}
	q.nextNotSent = q.start
	q.connId = newConnId
	q.cond.Broadcast()
}

func (q *queueType) incEnd() {
	q.end = (q.end + 1) % q.length
}

func (q *queueType) incStart() {
	if q.start == q.nextNotSent {
		q.incNextNotSent()
	}
	q.start = (q.start + 1) % q.length
}

func (q *queueType) incNextNotSent() {
	q.nextNotSent = (q.nextNotSent + 1) % q.length
}

func (q *queueType) isEmpty() bool {
	return q.start == q.end
}

func (q *queueType) isFull() bool {
	return (q.end+1)%q.length == q.start
}

func (q *queueType) noNotSent() bool {
	return q.nextNotSent == q.end
}
