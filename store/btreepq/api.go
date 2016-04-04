// Package btreepq implements a queue of pages for scotty based on btrees.
package btreepq

import (
	"github.com/Symantec/btree"
)

// Page represents a single page in scotty
// Each page has a sequence number which indicates when the page was last
// used. Pages with higher sequence numbers were used more recently than pages
// with lower sequence numbers.
type Page interface {
	// btree.Item Less method must return true if this instance's
	// sequence number is less than than's.
	btree.Item
	// Sets the sequence number of this page. Only PageQueue uses this.
	// Clients must not call this directly.
	SetSeqNo(i uint64)
}

// PageQueue represents the page queue for scotty.
// Instances have two internal queues: A low priority and a high priority queue.
// Generally, the high priority queue contain the pages that are less valuable
// to scotty such as pages belonging to inactive metrics.
// If the size of the high priority queue equals or exceeds a given threshhold,
// the NextPage method always pulls from the high priority queue. Otherwise,
// NextPage pulls from whatever queue has the least recently used page.
// Initially, all pages are on the high priority queue as new pages have no
// data valuable to scotty.
// NextPage always adds the page it returns back to the end of the low
// priority queue. In fact len(lowPriorityQueue) + len(highPriorityQueue) is
// always constant, and the two queues are mutually exclusive.
type PageQueue struct {
	high       *btree.BTree
	low        *btree.BTree
	threshhold int
	nextSeqNo  uint64
}

// New returns a new PageQueue.
// pageCount is the total number of pages.
// threshhold is the size that the high priority queue must be to always pull
// from it.
// degree is the degree of the Btrees. See github.com/Symantec/btree
// creater creates each page and is called exactly pageCount times.
func New(
	pageCount,
	threshhold,
	degree int,
	creater func() Page) *PageQueue {
	return _new(pageCount, threshhold, degree, creater)
}

// NextPage returns the next page for scotty to use.
func (p *PageQueue) NextPage() Page {
	return p.nextPage()
}

// ReclaimHigh moves pg from the low priority queue to the high priority queue
// If pg is already in the high priority queue, ReclaimHigh is a no-op.
func (p *PageQueue) ReclaimHigh(pg Page) {
	p.moveFromTo(pg, p.low, p.high)
}

// ReclaimLow moves pg from the high priority queue to the low priority queue
// If pg is already in the low priority queue, ReclaimLow is a no-op.
func (p *PageQueue) ReclaimLow(pg Page) {
	p.moveFromTo(pg, p.high, p.low)
}
