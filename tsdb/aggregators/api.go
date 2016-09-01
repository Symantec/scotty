// Package aggregators contains aggregator factory methods.
// The aggregators package must not depend on any other scotty packages
// except for tsdb.
package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

// Aggregator instances correspond to OpenTSDB aggregators such as
// sum, avg, count, max, etc.
type Aggregator struct {
	aggListCreater func(size int) aggregatorListType
	updaterCreater updaterCreaterType
}

var (
	Avg = &Aggregator{
		aggListCreater: func(size int) aggregatorListType {
			return make(averageListType, size)
		},
		updaterCreater: kNaN,
	}
	Count = &Aggregator{
		aggListCreater: func(size int) aggregatorListType {
			return make(countListType, size)
		},
		updaterCreater: kZero,
	}
	Max = &Aggregator{
		aggListCreater: func(size int) aggregatorListType {
			return make(maxListType, size)
		},
		updaterCreater: kNaN,
	}
	Sum = &Aggregator{
		aggListCreater: func(size int) aggregatorListType {
			return make(sumListType, size)
		},
		updaterCreater: kNaN,
	}
)

var (
	kAggregatorsByName = map[string]*Aggregator{
		"avg":   Avg,
		"count": Count,
		"max":   Max,
		"sum":   Sum,
	}
)

// ByName returns the aggregator with given name or nil, false if no aggregator
// matches given name
func ByName(aggregatorName string) (*Aggregator, bool) {
	result, ok := kAggregatorsByName[aggregatorName]
	return result, ok
}

// Names returns all the aggregator names.
func Names() (result []string) {
	for key := range kAggregatorsByName {
		result = append(result, key)
	}
	return
}

// FillPolicy describes how to handle missing values after downsampling
type FillPolicy int

const (
	// None is the default. Do not emit missing values.
	None FillPolicy = iota
	// NaN behaves the same as None for now
	NaN
	// Null behaves the same as None for now
	Null
	// Zero means emit zero when no values are present in a downsample range
	Zero
)

var (
	kFillPoliciesByName = map[string]FillPolicy{
		"none": None,
		"nan":  NaN,
		"null": Null,
		"zero": Zero,
	}
)

// ByFillPolicyName returns the fill policy with given name or None, false if
// no fill policy matches given name
func ByFillPolicyName(fillPolicyName string) (FillPolicy, bool) {
	result, ok := kFillPoliciesByName[fillPolicyName]
	return result, ok
}

// New returns an instance that aggregates time series.
//
// The aggregator parameter specifies the type of aggrgation.
//
// The downsampleAggregator parameter specifies the type of aggregation to
// use when downsampling.
//
// start and end are the start and end times in seconds since Jan 1, 1970 for
// the aggregation. Time series passed to the Add() method of returned
// aggregator must fall within start and end inclusive.

// downSample is the down sample time in seconds. New treats downSample
// values less than 1.0 as 1.0
//
// fillPolicy is the FillPolicy to use when downsampling.
func New(
	start, end float64,
	aggregator *Aggregator,
	downSample float64,
	downSampleAggregator *Aggregator,
	fillPolicy FillPolicy) tsdb.Aggregator {
	return _new(
		start,
		end,
		aggregator,
		downSample,
		downSampleAggregator,
		fillPolicy)
}
