# How to add an open TSDB aggregator to scotty

## Background

Scotty supports a small subset of open TSDB to power grafana displays.
Open TSDB allows for implementations to provide methods for aggregating
multiple time series together and for downsampling data in a single time
series. Common examples of such aggregators are avg, sum, max, min,
count etc.

This document describes how to add additional open TSDB aggregators to
scotty.

## This package

This package is the package responsible for aggregating multiple time
series into one to serve open TSDB requests. It is in this package that
contains all supported open TSDB aggregators. The Aggregator type in
this package denotes a single open TSDB aggregator. Scotty also has a
tsdb.Aggregator which should not be confused with the Aggregator type
in this package. The tsdb.Aggregator type does the actual aggregation
of time series and uses one open TSDB aggregator for merging time
series and another open TSDB aggregator for downsampling individual
time series. This document focuses on the former, Aggregator, type.
Adding a new open TSDB aggregator means creating and registering a new
Aggregator instance.

## How aggregation works in open TSDB

Open TSDB divides up time range into multiple time slices, each of equal
link. The length of each time slice is called the *down sample interval*.
For example, if the down sample interval is 5 minutes, open TSDB would
divide a one hour time range from 13:00-14:00 into time slices of
13:00-13:05; 13:05-13:10; ...; 13:55-14:00.

### Downsampling

Before aggregating open TSDB first downsamples each time series.
Downsampling means combining values in a time series so that there
exists at most one value for each time slice. For example, if the
downsampling aggregator is *avg*, the downsample interval is 5 mintues,
and we have the following values:

* 13:00 10
* 13:01 20
* 13:02 30
* 13:03 40
* 13:04 50
* 13:06 100
* 13:07 200

we would combine all these these and record 30.0 for the timeslice
13:00-13:05 and 150.0 for the timeslice 13:05-13:10

### Aggregating

After downsampling each time series open TSDB aggregates them together
using another open TSDB aggregator. Whereas downsampling combines multiple
values within the same time series togther for each time slice,
aggregation combines the values across multiple time series for each
time slice. Since downsampling produces at most one value for each time
slice, aggregating 5 time slices together means combining at most 5
different values for each time slice. The user may use the same open TSDB
aggregator or different open TSDB aggregators for downsampling and
aggregating.

### Fill policies

After downsampling a time series, a particular time slice may have a
missing value. A fill policy dictates how these missing values are treated
during the aggregation phase. Currently, open TSDB 2.2 supports the
following fill policies:

* None - No fill policy
* NaN - Ignore missing value
* Null - Same as NaN, but emit null for missing value.
* Zero - Treat missing value as 0.0

These fill policies are merely hints. In particular, if the fill policy
is None, the downsampling open TSDB aggregator is free to choose its
own fill policy.

## The Aggregator type

The aggregator type consist of two fields. The first field,
*aggListCreater*, is a function that takes the number of time slices
as input and returns a brand new aggregatorListType, to be discussed
later, that handles that many time slices. The second field,
*updaterCreater* maps a fill policy to a function that takes the number
of time slices as input and a fill policty and produces a brand new
updaterType instance that handles that many time slices.

### aggregatorListType

aggregatorListType is defined in the aggregators.go file and stores one
value for each time slice. The aggregatorListType implementation is the
heart of an open TSDB aggregator. Typically, each open TSDB aggregator
will have its own implementation of aggregatorListType.

The aggregatorListType has 4 methods. First we discuss then Len() method
which returns the number of time slices it can handle.
aggregatorListType instances know time slices by index ranging from
0 up to and including Len()-1. The code that converts a time to an
index is outside the scope of this document. Below is a description of
all four methods.

* Len() returns the time slice count
* Add() adds one new value at given index. It is up to the implementation
  to decide what to do with the added value: average it, add it to a sum,
  etc.
* Get() returns the aggregated value at given index and true or it
  returns false if value is missing
* Clear() clears data from instance

### updaterType

updaterType is also defined in the aggregators.go file. updaterType
instances updates aggregated values with the downsampled values from one
time series. Therefore, updaterTypes are responsible for how missing
values are handled. Since updaterType has access to all the
downsampled values, implementations can be made to predict missing values
rather than just ignoring them or treating them as 0.

updaterType instances have one method, Update, that updates the aggregated
values with the values from the downsampled time series.

updaterType instances cannot be used with multiple goroutines. To save
on GC activity, updaterType instances can use internal state for
computations rather than allocating arrays on the fly in each call to
Update.

## Writing your own aggregator

This section explains the mechanics of writing your own aggregator.

### Define your aggregator

Edit the api.go file in this package by adding your own aggregator to
the var block around line 17. Typically, your declaration will look
like this:

	MyAgg = &Aggregator{
		aggListCreater: func(size int) aggregatorListType {
			return make(myAggListType, size)
		},
		updaterCreater: kNaN,
	}

The kNaN is defined in the updaters.go file. It dictates the following.

* If the fill policy is None, NaN, or Null ignore missing values.
* If the fill policy is Zero, treat missing values as 0.0.

Remember that the aggregator used for downsampling dictates the handling
of fill policy not the aggregator used for aggregating.

### Register your aggregator

In the very next var block in the api.go file in this package, add
a line for your package in the map. Your line may look like this:

	"myagg": MyAgg,

This step registers your aggregator with scotty so that it shows up
in the combo boxes when connecting to scotty with grafana.

### Implement aggregatorListType

Within this package create file called myagg.go that defines the
myAggListType. Typically, this will be a slice of some struct. See
avg.go which implements averaging as an example.

## Testing your work

To test your work, add a test file to this package called myagg_test.go.
Write four tests, one for each of the four possible fill policies.
Use the *AggregatorTester* framework to test your work.

### AggregatorTester framework.

AggregatorTester uses the open TSDB aggregator under test and a fill
policy and applies them. Here are the methods explained.

* newAggregatorTester - Creates an AggregatorTester to test a particular
open TSDB aggregator and fill policy.
* Expect() - expects that a sequence of values aggregates to a
   particular value
* ExpectNoneForNoValues() - expects that no values aggregates to no value.
* ExpectNone() - expects that a list of values aggregates to no values.
     rare
* Verify() - runs the test.

Example test for the Avg aggregator taken from avg_test.go:

	func TestAvgNone(t *testing.T) {
		// Test average aggregator with no fill policy
		tester := newAggregatorTester(aggregators.Avg, aggregators.None)
		// No values should yield a missing value
		tester.ExpectNoneForNoValues()
		// 5.0 should average to 5.0
		tester.Expect(5.0, 5.0)
		// 3.5 is the average of 1.0, 2.0, 3.0, 8.0	
		tester.Expect(3.5, 1.0, 2.0, 3.0, 8.0)
		// run the test
		tester.Verify(t)
	}

	func TestAvgZero(t *testing.T) {
		// Test average aggregator with zero fill policy
		tester := newAggregatorTester(aggregators.Avg, aggregators.Zero)
		// No values treated as 0.
		tester.Expect(0.0)
		// 1.25 is the average of -1.25, 1.5, 3.5
		tester.Expect(1.25, -1.25, 1.5, 3.5)
		// run the test
		tester.Verify(t)
	}

Note that the test are run in a single verify as an aggregator may use
an updaterType that predicts missing values based on values around it.

## Example of adding the min open TSDB aggregator

https://github.com/Symantec/scotty/commit/c6e3152b116e083ad1914b90c0b9d2e43f36e5e7


