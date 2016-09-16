package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"reflect"
	"testing"
)

func TestAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.None)
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
		{1000.0, 48.5},
		{1400.0, 1025.0},
		{1600.0, 99.0},
		{1800.0, 149.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageZero(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.Zero)
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
		{1000.0, 12.125},
		{1200.0, 0.0},
		{1400.0, 768.75},
		{1600.0, 24.75},
		{1800.0, 74.5},
		{2000.0, 0.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageMax(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Max,
		aggregators.None)
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
		{1000.0, 54.0},
		{1400.0, 1043.5},
		{1600.0, 99.0},
		{1800.0, 150.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestMaxAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Max,
		200.0,
		aggregators.Avg,
		aggregators.None)
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
		{1000.0, 48.5},
		{1400.0, 2025.0},
		{1600.0, 99.0},
		{1800.0, 200.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestSumAverage(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Sum,
		200.0,
		aggregators.Avg,
		aggregators.None)
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
		{1000.0, 48.5},
		{1400.0, 3075.0},
		{1600.0, 99.0},
		{1800.0, 298.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageStrangeStartAndEnd(t *testing.T) {
	aggregator := aggregators.New(
		1057.0,
		1938.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN)
	aggregator.Add(tsdb.TimeSeries{
		{1057.0, 30.0}, {1199.0, 40.0},
		{1401.0, 20.0}, {1599.0, 30.0},
		{1836.0, 98.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{1000.0, 35.0},
		{1400.0, 25.0},
		{1800.0, 98.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestStartEqualsEnd(t *testing.T) {
	aggregators.New(
		1057.0,
		1057.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.NaN)
	aggregators.New(
		1000.0,
		1000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.None)
}

func TestNegativeStart(t *testing.T) {
	aggregator := aggregators.New(
		-29028.0,
		29028.0,
		aggregators.Avg,
		10000.0,
		aggregators.Avg,
		aggregators.NaN)
	aggregator.Add(tsdb.TimeSeries{
		{-29028.0, -60.0}, {-21000.0, -50.0},
		{-3000.0, -5.0}, {3000.0, 5.0},
		{29027.0, 43.0}})
	aggregated := aggregator.Aggregate()
	expected := tsdb.TimeSeries{
		{-30000.0, -55.0},
		{-10000.0, -5.0},
		{0, 5.0}, {20000.0, 43.0}}
	assertValueDeepEqual(t, expected, aggregated)
}

func TestAverageNone(t *testing.T) {
	aggregator := aggregators.New(
		1000.0,
		2000.0,
		aggregators.Avg,
		200.0,
		aggregators.Avg,
		aggregators.None)
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
