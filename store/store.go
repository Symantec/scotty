package store

import (
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

// This file contains the top level code for the store package.

func (r *Record) setValue(value interface{}) {
	if value == gInactive {
		r.Active = false
		r.Value = r.Info.Kind().ZeroValue()
	} else {
		r.Active = true
		r.Value = value
	}
}

func (s *Store) registerEndpoint(endpointId interface{}) {

	if s.byApplication[endpointId] != nil {
		return
	}
	s.byApplication[endpointId] = newTimeSeriesCollectionType(
		endpointId, s.metrics)
}

func (s *Store) shallowCopy() *Store {
	byApplicationCopy := make(
		map[interface{}]*timeSeriesCollectionType,
		len(s.byApplication))
	for k, v := range s.byApplication {
		byApplicationCopy[k] = v
	}
	return &Store{
		byApplication: byApplicationCopy,
		supplier:      s.supplier,
		metrics:       s.metrics,
	}
}

func (s *Store) addBatch(
	endpointId interface{},
	timestamp float64,
	mlist metrics.List) (int, error) {
	return s.byApplication[endpointId].AddBatch(
		timestamp, mlist, s.supplier)
}

func (s *Store) byNameAndEndpoint(
	name string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByName(name, start, end, result)
}

func (s *Store) byPrefixAndEndpoint(
	prefix string,
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix(prefix, start, end, result)
}

func (s *Store) timeLeft(name string) float64 {
	result := 0.0
	for endpointId := range s.byApplication {
		current := s.byApplication[endpointId].TimeLeft(name)
		if current > result {
			result = current
		}
	}
	return result
}

func (s *Store) namedIteratorForEndpoint(
	name string,
	endpointId interface{},
	maxFrames int) (NamedIterator, float64) {
	return s.byApplication[endpointId].NewNamedIterator(name, maxFrames)
}

func (s *Store) namedIteratorForEndpointRollUp(
	name string,
	endpointId interface{},
	duration time.Duration,
	maxFrames int) (NamedIterator, float64) {
	return s.byApplication[endpointId].NewNamedIteratorRollUp(
		name, float64(duration)/float64(time.Second), maxFrames)
}

func (s *Store) byEndpoint(
	endpointId interface{},
	start, end float64,
	result Appender) {
	s.byApplication[endpointId].ByPrefix("", start, end, result)
}

func (s *Store) markEndpointInactive(
	timestamp float64, endpointId interface{}) {
	s.byApplication[endpointId].MarkInactive(timestamp, s.supplier)
}

func (s *Store) markEndpointActive(endpointId interface{}) {
	s.byApplication[endpointId].MarkActive()
}

func (s *Store) latestByEndpoint(
	endpointId interface{},
	result Appender) {
	s.byApplication[endpointId].Latest(result)
}

func (s *Store) visitAllEndpoints(v Visitor) (err error) {
	for endpointId := range s.byApplication {
		if err = v.Visit(s, endpointId); err != nil {
			return
		}
	}
	return
}

func (s *Store) registerMetrics() (err error) {
	if err = s.supplier.RegisterMetrics(); err != nil {
		return
	}
	// Allow this store instance to be GCed
	maxValuesPerPage := s.supplier.MaxValuesPerPage()
	metrics := s.metrics

	if err = tricorder.RegisterMetric(
		"/store/pagesPerMetric",
		metrics.PagesPerMetricDist,
		units.None,
		"Number of pages used per metric"); err != nil {
		return
	}
	var primitiveMetrics storePrimitiveMetricsType
	storeGroup := tricorder.NewGroup()
	storeGroup.RegisterUpdateFunc(func() time.Time {
		metrics.Metrics(&primitiveMetrics)
		return time.Now()
	})
	if err = tricorder.RegisterMetricInGroup(
		"/store/pageUtilization",
		func() float64 {
			metricValueCount := primitiveMetrics.UniqueMetricValueCount
			pagesInUseCount := metrics.PagesPerMetricDist.Sum()
			metricCount := metrics.PagesPerMetricDist.Count()
			extraValueCount := float64(metricValueCount) - float64(metricCount)
			return extraValueCount / pagesInUseCount / float64(maxValuesPerPage)
		},
		storeGroup,
		units.None,
		"Page utilization 0.0 - 1.0"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/metricValueCount",
		&primitiveMetrics.UniqueMetricValueCount,
		storeGroup,
		units.None,
		"Number of unique metrics values"); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/valuePageCount",
		metrics.PagesPerMetricDist.Sum,
		storeGroup,
		units.None,
		"Number of pages used for values."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/timestampPageCount",
		&primitiveMetrics.TimeStampPageCount,
		storeGroup,
		units.None,
		"Number of pages used for timestamps."); err != nil {
		return
	}
	if err = tricorder.RegisterMetricInGroup(
		"/store/totalPagesInUseCount",
		func() int64 {
			return primitiveMetrics.TimeStampPageCount + int64(metrics.PagesPerMetricDist.Sum())
		},
		storeGroup,
		units.None,
		"Total number of pages used."); err != nil {
		return
	}
	return
}
