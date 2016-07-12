package store

import (
	"github.com/Symantec/scotty/store/btreepq"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

// This file contains all the code related to the page queue.

type pageQueueType struct {
	valueCountPerPage  int
	pageCount          int
	inactiveThreshhold float64
	degree             int
	lock               sync.Mutex
	pq                 *btreepq.PageQueue
}

func newPageQueueType(
	bytesPerPage int,
	pageCount int,
	inactiveThreshhold float64,
	degree int) *pageQueueType {
	pages := btreepq.New(
		pageCount,
		int(float64(pageCount)*inactiveThreshhold),
		degree,
		func() btreepq.Page {
			return newPageWithMetaDataType(bytesPerPage)
		})
	return &pageQueueType{
		valueCountPerPage:  bytesPerPage / kTsAndValueSize,
		pageCount:          pageCount,
		inactiveThreshhold: inactiveThreshhold,
		degree:             degree,
		pq:                 pages}
}

func (s *pageQueueType) MaxValuesPerPage() int {
	return s.valueCountPerPage
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
		"/nextLowPrioritySeqNo",
		&queueStats.NextLowPrioritySeqNo,
		queueGroup,
		units.None,
		"Next seq no in low priority queue, 0 if empty"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/nextHighPrioritySeqNo",
		&queueStats.NextHighPrioritySeqNo,
		queueGroup,
		units.None,
		"Next seq no in high priority queue, 0 if empty"); err != nil {
		return
	}
	if err = d.RegisterMetricInGroup(
		"/endSeqNo",
		&queueStats.EndSeqNo,
		queueGroup,
		units.None,
		"All seq no smaller than this. Marks end of both queues."); err != nil {
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

	if err = d.RegisterMetric(
		"/totalPages",
		&s.pageCount,
		units.None,
		"Total number of pages."); err != nil {
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

// GivePageTo bestows a new page on t.
// This call may lock another pageOwnerType instance. To avoid deadlock,
// caller must not hold a lock on any pageOwnerType instance.
func (s *pageQueueType) GivePageTo(t pageOwnerType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	result := s.pq.NextPage().(*pageWithMetaDataType)
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
