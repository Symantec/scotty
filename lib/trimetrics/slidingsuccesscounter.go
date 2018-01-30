package trimetrics

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

type slidingSuccessCounterDataType struct {
	TotalHour   int64
	TotalDay    int64
	SuccessHour int64
	SuccessDay  int64
}

func (s *slidingSuccessCounterDataType) SuccessRatioHour() float64 {
	if s.TotalHour == 0 {
		return 1.0
	}
	return float64(s.SuccessHour) / float64(s.TotalHour)
}

func (s *slidingSuccessCounterDataType) SuccessRatioDay() float64 {
	if s.TotalDay == 0 {
		return 1.0
	}
	return float64(s.SuccessDay) / float64(s.TotalDay)
}

func newSlidingSuccessCounter() *SlidingSuccessCounter {
	result := &SlidingSuccessCounter{}
	result.totalHour.Init(60)
	result.successHour.Init(60)
	result.totalDay.Init(60 * 24)
	result.successDay.Init(60 * 24)
	return result
}

func getSlot() uint64 {
	minutes := time.Now().Unix() / 60
	if minutes < 0 {
		minutes = 0
	}
	return uint64(minutes)
}

func (s *SlidingSuccessCounter) inc(total, success int64) {
	slot := getSlot()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalHour.Inc(slot, total)
	s.totalDay.Inc(slot, total)
	s.successHour.Inc(slot, success)
	s.successDay.Inc(slot, success)
}

func (s *SlidingSuccessCounter) get(result *slidingSuccessCounterDataType) {
	slot := getSlot()
	s.mu.Lock()
	defer s.mu.Unlock()
	result.TotalHour = s.totalHour.Total(slot)
	result.TotalDay = s.totalDay.Total(slot)
	result.SuccessHour = s.successHour.Total(slot)
	result.SuccessDay = s.successDay.Total(slot)
}

func (s *SlidingSuccessCounter) registerUnder(
	dir *tricorder.DirectorySpec, path, desc string) error {
	var data slidingSuccessCounterDataType
	grp := tricorder.NewGroup()
	grp.RegisterUpdateFunc(func() time.Time {
		s.get(&data)
		return time.Now()
	})
	dg := tricorder.DirectoryGroup{Group: grp, Directory: dir}
	if err := dg.RegisterMetric(
		path+"_total_1h", &data.TotalHour, units.None, desc); err != nil {
		return err
	}
	if err := dg.RegisterMetric(
		path+"_total_1d", &data.TotalDay, units.None, desc); err != nil {
		return err
	}
	if err := dg.RegisterMetric(
		path+"_success_1h", &data.SuccessHour, units.None, desc); err != nil {
		return err
	}
	if err := dg.RegisterMetric(
		path+"_success_1d", &data.SuccessDay, units.None, desc); err != nil {
		return err
	}
	if err := dg.RegisterMetric(
		path+"_ratio_1h", data.SuccessRatioHour, units.None, desc); err != nil {
		return err
	}
	return dg.RegisterMetric(
		path+"_ratio_1d", data.SuccessRatioDay, units.None, desc)
}
