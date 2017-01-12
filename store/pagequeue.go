package store

import (
	"github.com/Symantec/scotty/store/btreepq"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

// This file contains all the code related to the page queue.

type pageQueueType struct {
	valueCountPerPage  uint
	inactiveThreshhold float64
	degree             uint
	bytesPerPage       uint
	lock               sync.Mutex
	expanding          bool
	pq                 *btreepq.PageQueue
}

func newPageQueueType(
	bytesPerPage uint,
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *pageQueueType {
	if inactiveThreshhold < 0.0 {
		panic("inactiveThreshhold cannot be negative")
	}
	pages := btreepq.New(
		pageCount,
		uint(float64(pageCount)*inactiveThreshhold),
		degree,
		func() btreepq.Page {
			return newPageWithMetaDataType(bytesPerPage)
		})
	return &pageQueueType{
		valueCountPerPage:  bytesPerPage / kTsAndValueSize,
		inactiveThreshhold: inactiveThreshhold,
		degree:             degree,
		bytesPerPage:       bytesPerPage,
		pq:                 pages}
}

func (s *pageQueueType) MaxValuesPerPage() uint {
	return s.valueCountPerPage
}

func (s *pageQueueType) maybeRemoveOnePage() {
	result, ok := s.pq.RemovePage()
	if ok {
		removedPage := result.(*pageWithMetaDataType)
		// Force the owner of the page to give it up.
		if removedPage.owner != nil {
			removedPage.owner.GiveUpPage(removedPage)
		}
		// Removed page no longer has an owner
		removedPage.owner = nil
	}
}

func (s *pageQueueType) FreeUpBytes(bytesToFree uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var freedSoFar uint64
	for freedSoFar < bytesToFree {
		s.maybeRemoveOnePage()
		freedSoFar += uint64(s.bytesPerPage)
	}
}

func (s *pageQueueType) LessenPageCount(ratio float64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	var stats btreepq.PageQueueStats
	s.pq.Stats(&stats)
	pageCount := int(float64(stats.TotalCount()) * ratio)
	for i := 0; i < pageCount; i++ {
		s.maybeRemoveOnePage()
	}
}

func (s *pageQueueType) SetExpanding(b bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.expanding = b
}

func (s *pageQueueType) IsExpanding() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.expanding
}

func (s *pageQueueType) PageQueueStats(stats *btreepq.PageQueueStats) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pq.Stats(stats)
}

func (s *pageQueueType) RegisterMetrics(d *tricorder.DirectorySpec) (
	err error) {
	var queueStats btreepq.PageQueueStats
	queueGroup := tricorder.NewGroup()
	queueGroup.RegisterUpdateFunc(func() time.Time {
		s.PageQueueStats(&queueStats)
		return time.Now()
	})
	if err = d.RegisterMetricInGroup(
		"/highPriorityCount",
		&queueStats.HighPriorityCount,
		queueGroup,
		units.None,
		"Number of pages in high priority queue"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/lowPriorityCount",
		&queueStats.LowPriorityCount,
		queueGroup,
		units.None,
		"Number of pages in low priority queue"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/totalPages",
		queueStats.TotalCount,
		queueGroup,
		units.None,
		"Total number of pages."); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/highPriorityRatio",
		queueStats.HighPriorityRatio,
		queueGroup,
		units.None,
		"High priority page ratio"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/firstLowPriorityTS",
		func() time.Time {
			return duration.FloatToTime(queueStats.FirstLowPriorityTS)
		},
		queueGroup,
		units.None,
		"Earliest timestamp in low priority queue"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/firstHighPriorityTS",
		func() time.Time {
			return duration.FloatToTime(queueStats.FirstHighPriorityTS)
		},
		queueGroup,
		units.None,
		"Earliest timestamp in high priority queue"); err != nil {
		return
	}

	if err = d.RegisterMetric(
		"/expanding",
		s.IsExpanding,
		units.None,
		"Is page queue expanding."); err != nil {
		return
	}
	if err = d.RegisterMetric(
		"/maxValuesPerPage",
		&s.valueCountPerPage,
		units.None,
		"Maximum number ofvalues that can fit in a page."); err != nil {
		return
	}
	if err = d.RegisterMetric(
		"/inactiveThreshhold",
		&s.inactiveThreshhold,
		units.None,
		"The ratio of inactive pages needed before they are reclaimed first"); err != nil {
		return
	}
	if err = d.RegisterMetric(
		"/btreeDegree",
		&s.degree,
		units.None,
		"The degree of the btrees in the queue"); err != nil {
		return
	}
	return
}

// GivePageTo bestows a new page on t. ts is the timestamp of the value
// being added to t.
// This call may lock another pageOwnerType instance. To avoid deadlock,
// caller must not hold a lock on any pageOwnerType instance.
func (s *pageQueueType) GivePageTo(t pageOwnerType, ts float64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	fullPage := t.LatestPage()
	if fullPage != nil {
		s.pq.Prioritise(fullPage, ts)
	}
	var result *pageWithMetaDataType
	if s.expanding {
		result = s.pq.NewPage().(*pageWithMetaDataType)
	} else {
		result = s.pq.NextPage().(*pageWithMetaDataType)
	}
	if result.owner != nil {
		result.owner.GiveUpPage(result)
	}
	result.owner = t
	result.owner.AcceptPage(result)
}

func (s *pageQueueType) ReclaimHigh(
	reclaimHighList []pageListType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, pageList := range reclaimHighList {
		for i := range pageList.Pages {
			if pageList.Pages[i].owner == pageList.Owner {
				s.pq.ReclaimHigh(pageList.Pages[i])
			}
		}
	}
}

func (s *pageQueueType) ReclaimLow(
	reclaimLowList []pageListType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, pageList := range reclaimLowList {
		for i := range pageList.Pages {
			if pageList.Pages[i].owner == pageList.Owner {
				s.pq.ReclaimLow(pageList.Pages[i])
			}
		}
	}
}
