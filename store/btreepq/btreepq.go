package btreepq

import (
	"github.com/google/btree"
	"math"
)

var (
	kInf    = math.Inf(0)
	kNegInf = math.Inf(-1)
)

func _new(
	pageCount,
	threshold,
	degree uint,
	creater func() Page) *PageQueue {
	fl := btree.NewFreeList(btree.DefaultFreeListSize)
	high := btree.NewWithFreeList(int(degree), fl)
	low := btree.NewWithFreeList(int(degree), fl)
	for i := uint(0); i < pageCount; i++ {
		page := creater()
		page.SetSeqNo(uint64(i))
		// We want these first pages to always be of utmost priority.
		page.SetTS(kNegInf)
		insert(high, page)
	}
	// A threshold < 1 may result in pulling from an empty high priority
	// queue.
	if threshold < 1 {
		threshold = 1
	}
	return &PageQueue{
		high:      high,
		low:       low,
		threshold: threshold,
		nextSeqNo: uint64(pageCount),
		creater:   creater}
}

func (p *PageQueue) popPage() Page {
	if uint(p.high.Len()) >= p.threshold {
		return p.high.DeleteMin().(Page)
	}
	lowNext := first(p.low)
	highNext := first(p.high)
	if lessNilHigh(lowNext, highNext) {
		return p.low.DeleteMin().(Page)
	} else if lessNilHigh(highNext, lowNext) {
		return p.high.DeleteMin().(Page)
	}
	panic("Two pages with same sequence number found")
}

func (p *PageQueue) removePage() (removed Page, ok bool) {
	if p.high.Len()+p.low.Len() <= 1 {
		return
	}
	return p.popPage(), true
}

func (p *PageQueue) pushBack(page Page) {
	page.SetSeqNo(p.nextSeqNo)
	page.SetTS(kInf)
	p.nextSeqNo++
	insert(p.low, page)
}

func (p *PageQueue) newPage() (next Page) {
	next = p.creater()
	p.pushBack(next)
	return
}

func (p *PageQueue) nextPage() (next Page) {
	next = p.popPage()
	p.pushBack(next)
	return
}

func moveFromTo(pg Page, from, to *btree.BTree) {
	aPage := from.Delete(pg)
	if aPage == nil {
		// If we can't find our page in the from list,
		// we have to assume it is in the to list already.
		return
	}
	pageToMove := aPage.(Page)
	if pageToMove != pg {
		panic("Unrecongized page passed to ReclaimHigh or ReclaimLow")
	}
	insert(to, pageToMove)
}

func prioritiseWith(pg Page, ts float64, dest *btree.BTree) bool {
	aPage := dest.Delete(pg)
	if aPage == nil {
		// Fail
		return false
	}
	pageToPrioritise := aPage.(Page)
	if pageToPrioritise != pg {
		panic("Unrecongized page passed to Prioritise")
	}
	pageToPrioritise.SetTS(ts)
	insert(dest, pageToPrioritise)
	return true
}

func (p *PageQueue) prioritise(pg Page, ts float64) {
	if prioritiseWith(pg, ts, p.low) {
		return
	}
	if prioritiseWith(pg, ts, p.high) {
		return
	}
}

func extractTS(maybeNilItem btree.Item) float64 {
	if maybeNilItem == nil {
		return 0.0
	}
	ts := maybeNilItem.(Page).TS()
	if math.IsInf(ts, 1) {
		// Jan 1, 2070 means +Inf. This code should be turned down by then.
		ts = 25.0 * 1461.0 * 24.0 * 60.0 * 60.0
	} else if math.IsInf(ts, -1) {
		// Jan 1, 1970 means -Inf for now
		ts = 0.0
	}
	return ts
}

func (p *PageQueue) _stats(stats *PageQueueStats) {
	stats.HighPriorityCount = uint(p.high.Len())
	stats.LowPriorityCount = uint(p.low.Len())
	stats.FirstLowPriorityTS = extractTS(first(p.low))
	stats.FirstHighPriorityTS = extractTS(first(p.high))
	stats.Threshold = p.threshold
}

func insert(target *btree.BTree, item btree.Item) {
	if target.ReplaceOrInsert(item) != nil {
		panic("Insert failed. item already exists")
	}
}

func lessNilHigh(target, than btree.Item) bool {
	if target == nil && than == nil {
		return false
	}
	if target == nil {
		return false
	}
	if than == nil {
		return true
	}
	return target.Less(than)
}

func first(target *btree.BTree) (result btree.Item) {
	target.Ascend(func(i btree.Item) bool {
		result = i
		return false
	})
	return
}
