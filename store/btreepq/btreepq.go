package btreepq

import (
	"github.com/google/btree"
)

func _new(
	pageCount,
	threshhold,
	degree int,
	creater func() Page) *PageQueue {
	fl := btree.NewFreeList(btree.DefaultFreeListSize)
	high := btree.NewWithFreeList(degree, fl)
	low := btree.NewWithFreeList(degree, fl)
	for i := 0; i < pageCount; i++ {
		page := creater()
		page.SetSeqNo(uint64(i))
		insert(high, page)
	}
	// A threshhold < 1 may result in pulling from an empty high priority
	// queue.
	if threshhold < 1 {
		threshhold = 1
	}
	return &PageQueue{
		high:       high,
		low:        low,
		threshhold: threshhold,
		nextSeqNo:  uint64(pageCount)}
}

func (p *PageQueue) popPage() Page {
	if p.high.Len() >= p.threshhold {
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

func (p *PageQueue) nextPage() (next Page) {
	next = p.popPage()
	next.SetSeqNo(p.nextSeqNo)
	p.nextSeqNo++
	insert(p.low, next)
	return
}

func (p *PageQueue) moveFromTo(pg Page, from, to *btree.BTree) {
	pageToMove := from.Delete(pg)
	if pageToMove == nil {
		// If we can't find our page in the from list,
		// we have to assume it is in the to list already.
		return
	}
	if pageToMove != pg {
		panic("Unrecongized page passed to ReclaimHigh or ReclaimLow")
	}
	insert(to, pageToMove)
}

func (p *PageQueue) _stats(stats *PageQueueStats) {
	stats.HighPriorityCount = p.high.Len()
	stats.LowPriorityCount = p.low.Len()
	stats.EndSeqNo = p.nextSeqNo
	lowNext := first(p.low)
	highNext := first(p.high)
	if lowNext == nil {
		stats.NextLowPrioritySeqNo = 0
	} else {
		stats.NextLowPrioritySeqNo = lowNext.(Page).SeqNo()
	}
	if highNext == nil {
		stats.NextHighPrioritySeqNo = 0
	} else {
		stats.NextHighPrioritySeqNo = highNext.(Page).SeqNo()
	}
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
