package btreepq_test

import (
	"github.com/Symantec/scotty/store/btreepq"
	"github.com/google/btree"
	"testing"
)

type pageForTesting struct {
	seq uint64
}

func (p *pageForTesting) SetSeqNo(i uint64) {
	p.seq = i
}

func (p *pageForTesting) Less(than btree.Item) bool {
	pthan := than.(*pageForTesting)
	return p.seq < pthan.seq
}

func TestZeroThreshhold(t *testing.T) {
	var pages [4]btreepq.Page
	queue := btreepq.New(
		len(pages),
		0,
		10,
		func() btreepq.Page {
			return &pageForTesting{}
		})
	for i := range pages {
		pages[i] = queue.NextPage()
	}
	assertNextValues(t, queue, pages[:], 0)
}

func TestAPI(t *testing.T) {
	var pages [8]btreepq.Page
	queue := btreepq.New(
		len(pages),
		2,
		10,
		func() btreepq.Page {
			return &pageForTesting{}
		})

	// Get all pages in queue. We will always revisit these pages in the same
	// order unless pages are marked high priority for reclaiming.
	for i := range pages {
		pages[i] = queue.NextPage()
	}
	verifyPagesAreUnique(t, pages[:])

	// Mark page 4 high priority for reclaiming
	queue.ReclaimHigh(pages[4])

	// Next page off the queue should be page 0 as we haven't reached the
	// threshhold for pulling high priority pages
	assertValueEquals(t, pages[0], queue.NextPage())

	// Mark page 5 high priority. Now high priority queue is {4, 5}
	queue.ReclaimHigh(pages[5])

	// Next page will be page 4, not page 1 as the high priority queue has
	// reached its threshhold of 2.
	assertValueEquals(t, pages[4], queue.NextPage())

	// Once again high priority queue has fallen below its threshhold so
	// next page is page 1, not page 5
	assertValueEquals(t, pages[1], queue.NextPage())

	// Mark pages 6 and 7 as high priority. High priority queue is now {5, 6, 7}
	queue.ReclaimHigh(pages[6])
	queue.ReclaimHigh(pages[7])

	// The next pages off the queue
	assertNextValues(t, queue, pages[:], 5, 6, 2, 3, 7)

	// Now the high priority queue has nothing and the normal queue is now
	// {0, 4, 1, 5, 6, 2, 3, 7}

	// We now mark pages as high priority and then mark the same pages as
	// low priority to show that ReclaimLow works
	queue.ReclaimHigh(pages[7])
	queue.ReclaimHigh(pages[2])
	queue.ReclaimHigh(pages[3])
	queue.ReclaimHigh(pages[1])

	queue.ReclaimLow(pages[7])
	queue.ReclaimLow(pages[2])
	queue.ReclaimLow(pages[3])
	queue.ReclaimLow(pages[1])

	// Since all pages are low priority, we should visit the pages in the
	// same order as before.
	assertNextValues(t, queue, pages[:], 0, 4, 1, 5, 6, 2, 3, 7)
}

func assertValueEquals(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func assertNextValues(
	t *testing.T,
	queue *btreepq.PageQueue,
	pages []btreepq.Page,
	indexes ...int) {
	for i := range indexes {
		page := queue.NextPage()
		if page != pages[indexes[i]] {
			t.Errorf("Didn't get page %d", indexes[i])
		}
	}
}

func verifyPagesAreUnique(
	t *testing.T,
	pages []btreepq.Page) {
	uniques := make(map[btreepq.Page]bool)
	for i := range pages {
		uniques[pages[i]] = true
	}
	if len(uniques) != len(pages) {
		t.Error("Pages not unique")
	}
}
