package store

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"sync"
	"time"
)

// This file contains all the code related to tracking scotty store metrics.

var (
	// 1 up to 1,000,000 bucketer
	kBucketer = tricorder.NewGeometricBucketer(1.0, 1e6)
)

type storePrimitiveMetricsType struct {
	UniqueMetricValueCount int64
	TimeStampPageCount     int64
	LatestEvictedTimeStamp float64
}

func (s *storePrimitiveMetricsType) TimeSpan() time.Duration {
	return time.Since(duration.FloatToTime(s.LatestEvictedTimeStamp))
}

type storeMetricsType struct {
	PagesPerMetricDist *tricorder.NonCumulativeDistribution
	lock               sync.Mutex
	values             storePrimitiveMetricsType
}

// Metrics fetches the metrics into v
func (s *storeMetricsType) Metrics(v *storePrimitiveMetricsType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	*v = s.values
}

func (s *storeMetricsType) UpdateLatestEvictedTimeStamp(ts float64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if ts > s.values.LatestEvictedTimeStamp {
		s.values.LatestEvictedTimeStamp = ts
	}
}

// Call when a value series gives up a page.
// oldLen is the number of pages series used to have.
// metricCountInPage is how many metrics are in the page being given up.
func (s *storeMetricsType) RemoveValuePage(
	oldLen int, metricCountInPage int) {
	oldLenF := float64(oldLen)
	s.PagesPerMetricDist.Update(oldLenF, oldLenF-1.0)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.UniqueMetricValueCount -= int64(metricCountInPage)
}

// Call when a timestamp series gives up a page.
func (s *storeMetricsType) RemoveTimeStampPage() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.TimeStampPageCount -= 1
}

// Call when creating a new value series
func (s *storeMetricsType) NewValueSeries() {
	s.PagesPerMetricDist.Add(0.0)
}

// Call when creating a new timestamp series
func (s *storeMetricsType) NewTimeStampSeries() {
}

// Call when adding an empty page to a value series.
// oldLen is the number of pages series used to have.
func (s *storeMetricsType) AddEmptyValuePage(oldLen int) {
	oldLenF := float64(oldLen)
	s.PagesPerMetricDist.Update(oldLenF, oldLenF+1.0)
}

// Call when adding an empty page to a timestamp series.
func (s *storeMetricsType) AddEmptyTimeStampPage() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.TimeStampPageCount += 1
}

// Call when adding values to any value series.
// count is the number of values added.
func (s *storeMetricsType) AddUniqueValues(count int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.values.UniqueMetricValueCount += int64(count)
}

func newStoreMetricsType() *storeMetricsType {
	return &storeMetricsType{
		PagesPerMetricDist: kBucketer.NewNonCumulativeDistribution(),
		values: storePrimitiveMetricsType{
			LatestEvictedTimeStamp: duration.TimeToFloat(time.Now()),
		},
	}
}
