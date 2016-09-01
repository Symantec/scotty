package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

const (
	kMaxSampleSize = 1000
)

type expectedResultsType struct {
	Answer float64
	Valid  bool
	Values []float64
}

type aggregatorTesterType struct {
	aggregator        aggregators.Aggregator
	noValuesAsMissing bool
	expected          []expectedResultsType
}

func newAggregatorTester(
	aggregator aggregators.Aggregator) *aggregatorTesterType {
	return &aggregatorTesterType{aggregator: aggregator}
}

func (a *aggregatorTesterType) ExpectNoneForNoValues() {
	a.expect(0.0, false, nil)
}

func (a *aggregatorTesterType) ExpectNone(values ...float64) {
	a.expect(0.0, false, values)
}

func (a *aggregatorTesterType) Expect(answer float64, values ...float64) {
	a.expect(answer, true, values)
}

func (a *aggregatorTesterType) expect(
	answer float64, valid bool, values []float64) {
	if len(values) > kMaxSampleSize {
		panic("Too many values")
	}
	a.expected = append(
		a.expected,
		expectedResultsType{
			Answer: answer,
			Valid:  valid,
			Values: values,
		})
}

func (a *aggregatorTesterType) Verify(t *testing.T) {
	length := len(a.expected)
	agg := aggregators.New(
		0, float64(length)*kMaxSampleSize,
		aggregators.Sum,
		kMaxSampleSize,
		a.aggregator,
		aggregators.None)
	var timeSeries tsdb.TimeSeries
	for i := range a.expected {
		for j, val := range a.expected[i].Values {
			timeSeries = append(
				timeSeries,
				tsdb.TsValue{
					float64(i)*kMaxSampleSize + float64(j),
					val})
		}
	}
	agg.Add(timeSeries)
	aggregatedTimeSeries := agg.Aggregate()
	aggIdx := 0
	for i := range a.expected {
		if !a.expected[i].Valid {
			continue
		}
		if aggIdx >= len(aggregatedTimeSeries) || aggregatedTimeSeries[aggIdx].Ts != float64(i)*kMaxSampleSize {
			t.Error(
				"Timestamps don't match. No value was emitted when one was expected or the other way around. Or maybe the Len() function is wrong.")
			return
		}
		if a.expected[i].Answer != aggregatedTimeSeries[aggIdx].Value {
			t.Errorf(
				"Expected %g for %v, got %g",
				a.expected[i].Answer,
				a.expected[i].Values,
				aggregatedTimeSeries[aggIdx].Value)
		}
		aggIdx++
	}
}
