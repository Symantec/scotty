package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"reflect"
	"testing"
)

func TestLinearInterpolation(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.None,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 31.0}, {1030.0, 31.5}, {1199.999, 31.25},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 75.0}})
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 31.25},
		{1300.0, 28.125},
		{1500.0, 25.0},
		{1700.0, 50.0},
		{1900.0, 75.0}}
	assertValueDeepEqual(t, expected, aggregated)
	aggregator.Add(tsdb.TimeSeries{
		{1200.0, 40.0},
		{1600.0, 46.0}})
	aggregated = aggregator.Aggregate()
	expected = tsdb.TimeSeries{
		{1100.0, 31.25},
		{1300.0, 34.0625},
		{1500.0, 34.0},
		{1700.0, 48.0},
		{1900.0, 75.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestRate(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{
			Counter: true, CounterMax: 65500.0, ResetValue: 200.0})
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 10000.0},
		{1200.0, 49000.0},
		{1400.0, 3000.0},
		{1600.0, 53000.0},
		{1800.0, 52000.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 195.0},
		{1300.0, 97.5},
		{1500.0, 0.0},
		{1700.0, 0.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestRateNoReset(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{Counter: true, CounterMax: 65500.0})
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 10000.0},
		{1200.0, 49000.0},
		{1400.0, 3000.0},
		{1600.0, 53000.0},
		{1800.0, 52000.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 195.0},
		{1300.0, 97.5},
		{1500.0, 250.0},
		{1700.0, 322.5}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestRateNoResetOrMax(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{Counter: true})
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 10000.0},
		{1200.0, 49000.0},
		{1400.0, 3000.0},
		{1600.0, 53000.0},
		{1800.0, 52000.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 195.0},
		{1300.0, 0.0},
		{1500.0, 250.0},
		{1700.0, 0.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestRateNoCounter(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{})
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 10000.0},
		{1200.0, 49000.0},
		{1400.0, 3000.0},
		{1600.0, 53000.0},
		{1800.0, 52000.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 195.0},
		{1300.0, -230.0},
		{1500.0, 250.0},
		{1700.0, -5.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestRateMissingValues(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{})
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 10000.0},
		{1600.0, 49000.0},
		{1800.0, 52000.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 65.0},
		{1300.0, 65.0},
		{1500.0, 65.0},
		{1700.0, 15.0}}
	assertValueDeepEqual(t, expected, aggregated)

	aggregator = aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		&aggregators.RateSpec{})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 10000.0},
		{1600.0, 49000.0}})
	aggregated = aggregator.Aggregate()
	expected = tsdb.TimeSeries{
		{1500.0, 195.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestCount(t *testing.T) {
	aggregator := aggregators.New(
		1043.0,
		2021.0,
		aggregators.Count,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1043.0, 42.0}, {1044.0, 54.0}, {1199.999, 49.5},
		{1200.0, 35.0},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0},
		{2019.0, 1.3}})
	aggregator.Add(nil)
	aggregator.Add(tsdb.TimeSeries{
		{1043.0, 42.0}, {1044.0, 54.0}, {1199.999, 49.5},
		{1200.0, 53.0},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0},
		{2019.0, 1.3}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1300.0, 2.0},
		{1500.0, 2.0},
		{1700.0, 0.0},
		{1900.0, 2.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 42.0}, {1030.0, 54.0}, {1199.999, 49.5},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1500.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 198.0}, {1801.0, 202.0}, {1999.1, 200.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 1975.0},
		{1450.0, 2000.0},
		{1599.0, 2025.0},
		{1599.1, 2050.0},
		{1599.2, 2075.0}})
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 48.5},
		{1500.0, 1025.0},
		{1700.0, 99.0},
		{1900.0, 149.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageZero(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.Zero,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 42.0}, {1030.0, 54.0}, {1199.999, 49.5},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1500.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 198.0}, {1801.0, 202.0}, {1999.1, 200.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 1975.0},
		{1450.0, 2000.0},
		{1599.0, 2025.0},
		{1599.1, 2050.0},
		{1599.2, 2075.0}})
	// Counts as all zeros
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 12.125},
		{1300.0, 0.0},
		{1500.0, 768.75},
		{1700.0, 24.75},
		{1900.0, 74.5}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageMax(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Max,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 42.0}, {1030.0, 54.0}, {1199.999, 49.5},
		{1401.0, 20.0}, {1599.0, 30.5},
		{1836.0, 98.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1500.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 198.0}, {1801.0, 202.0}, {1999.1, 200.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 1975.0},
		{1450.0, 2000.0},
		{1599.0, 2025.0},
		{1599.1, 2050.0},
		{1599.2, 2075.0}})
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 54.0},
		{1500.0, 1043.5},
		{1700.0, 99.0},
		{1900.0, 150.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestMaxAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Max,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 42.0}, {1030.0, 54.0}, {1199.999, 49.5},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1500.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 198.0}, {1801.0, 202.0}, {1999.1, 200.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 1975.0},
		{1450.0, 2000.0},
		{1599.0, 2025.0},
		{1599.1, 2050.0},
		{1599.2, 2075.0}})
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 48.5},
		{1500.0, 2025.0},
		{1700.0, 99.0},
		{1900.0, 200.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestSumAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Sum,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1000.0, 42.0}, {1030.0, 54.0}, {1199.999, 49.5},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1500.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 198.0}, {1801.0, 202.0}, {1999.1, 200.0}})
	aggregator.Add(tsdb.TimeSeries{
		{1400.0, 1975.0},
		{1450.0, 2000.0},
		{1599.0, 2025.0},
		{1599.1, 2050.0},
		{1599.2, 2075.0}})
	aggregator.Add(nil)
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1100.0, 48.5},
		{1500.0, 3075.0},
		{1700.0, 99.0},
		{1900.0, 298.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageStrangeStartAndEnd(t *testing.T) {
	aggregator := aggregators.New(
		1057.0,
		1938.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{1057.0, 30.0}, {1199.0, 40.0},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{{1500.0, 25.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestStartEqualsEnd(t *testing.T) {
	aggregators.New(
		1057.0,
		1057.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregators.New(
		1000.0,
		1000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
}

func TestNegativeStart(t *testing.T) {
	aggregator := aggregators.New(
		-29028.0,
		29028.0,
		aggregators.Avg,
		10000.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregator.Add(tsdb.TimeSeries{
		{-29028.0, -60.0}, {-21000.0, -50.0},
		{-3000.0, -5.0}, {3000.0, 5.0},
		{29027.0, 43.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{-5000.0, -5.0},
		{5000.0, 5.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageNone(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN,
		nil)
	aggregated := aggregator.Aggregate()
	if len(aggregated) != 0 {
		t.Error("Expected no aggregation")
	}
}

func assertValueDeepEqual(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
