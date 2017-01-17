// Package btreepq implements a queue of pages for scotty based on btrees.
package btreepq

import (
	"github.com/google/btree"
)

// IsPageLessThanThat returns true if page should be evicted / reused before
// that.
func IsPageLessThanThat(page, that Page) bool {
	if page.TS() < that.TS() {
		return true
	}
	if page.TS() > that.TS() {
		return false
	}
	return page.SeqNo() < that.SeqNo()
}

// Page represents a single page in scotty
// Each page has a sequence number which indicates when the page was last
// used. Pages with higher sequence numbers were used more recently than pages
// with lower sequence numbers.
type Page interface {
	// btree.Item Less method must return true if and only if
	// IsPageLessThanThat(instance, argument) returns true.
	// Implementations of Less must downcast their argument to this type, Page.
	// Implementations must never downcast the argument directly to the
	// implementation's type as there is no guarantee that the argument's
	// type will match the implementation's type.
	btree.Item
	// Sets the sequence number of this page. Only PageQueue uses this.
	// Clients must not call this directly.
	SetSeqNo(i uint64)
	// Returns the sequence number of this page.
	SeqNo() uint64
	// Sets the timestamp. Only PageQueue uses this.
	// Clients must not call this directly.
	SetTS(ts float64)
	// Returns the timestamp.
	TS() float64
}

// PageQueueStats represents statistics for a PageQueue
type PageQueueStats struct {
	LowPriorityCount    uint
	HighPriorityCount   uint
	Threshold           uint
	FirstLowPriorityTS  float64
	FirstHighPriorityTS float64
}

func (s *PageQueueStats) TotalCount() uint {
	return s.LowPriorityCount + s.HighPriorityCount
}

func (s *PageQueueStats) HighPriorityRatio() float64 {
	return float64(s.HighPriorityCount) / float64(s.TotalCount())
}

// PageQueue represents the page queue for scotty.
// Instances have two internal queues: A low priority and a high priority queue.
// Generally, the high priority queue contain the pages that are less valuable
// to scotty such as pages belonging to inactive metrics.
// If the size of the high priority queue equals or exceeds a given threshold,
// the NextPage method always pulls from the high priority queue. Otherwise,
// NextPage pulls from whatever queue has the least recently used page.
// Initially, all pages are on the high priority queue as new pages have no
// data valuable to scotty.
// NextPage always adds the page it returns back to the end of the low
// priority queue. The low and high priority queues are mutually exclusive.
type PageQueue struct {
	high *btree.BTree
	low  *btree.BTree

	// Number of pages that must be in high before the page queue ignores low
	// when finding the next page.
	threshold uint

	// The next available sequence number. This value increases monotonically.
	nextSeqNo uint64
	creater   func() Page
}

// New returns a new PageQueue.
// pageCount is the total number of pages which must be at least 1.
// threshold is the size that the high priority queue must be to always pull
// from it.
// degree is the degree of the Btrees. See github.com/Symantec/btree
// creater creates each page and is called exactly pageCount times.
func New(
	pageCount,
	threshold,
	degree uint,
	creater func() Page) *PageQueue {
	if pageCount < 1 {
		panic("pageCount must be at least 1")
	}
	return _new(pageCount, threshold, degree, creater)
}

// Len returns the total number of pages in this instance.
func (p *PageQueue) Len() uint {
	return uint(p.high.Len()) + uint(p.low.Len())
}

// SetThreshold sets the size that the high priority queue must be in order
// to always pull from it. If threshold < 1, its effective value is 1.
func (p *PageQueue) SetThreshold(threshold uint) {
	if threshold < 1 {
		threshold = 1
	}
	p.threshold = threshold
}

// NextPage returns the next page for scotty to use leaving it in the queue
// by adding it to the end of the low priority queue.
func (p *PageQueue) NextPage() Page {
	return p.nextPage()
}

// NewPage works like NextPage except that it always returns a brand new
// page and it increases the total page count by 1 in this instance.
func (p *PageQueue) NewPage() Page {
	return p.newPage()
}

// RemovePage works like NextPage except that it removes returned page from
// this queue entirely. If this instance already has the minimum number
// of pages, RemovePages returns nil, false
func (p *PageQueue) RemovePage() (removed Page, ok bool) {
	return p.removePage()
}

// ReclaimHigh moves pg from the low priority queue to the high priority queue
// If pg is already in the high priority queue, ReclaimHigh is a no-op.
func (p *PageQueue) ReclaimHigh(pg Page) {
	moveFromTo(pg, p.low, p.high)
}

// ReclaimLow moves pg from the high priority queue to the low priority queue
// If pg is already in the low priority queue, ReclaimLow is a no-op.
func (p *PageQueue) ReclaimLow(pg Page) {
	moveFromTo(pg, p.high, p.low)
}

// Prioritise prioritises pg according to ts. Pages with lower ts values come
// off the queue first. By default, the ts for a page is +Inf.
func (p *PageQueue) Prioritise(pg Page, ts float64) {
	p.prioritise(pg, ts)
}

// Stats gets the statistics for this instance
func (p *PageQueue) Stats(stats *PageQueueStats) {
	p._stats(stats)
}
