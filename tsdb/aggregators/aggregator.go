package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

// updaterType instances update aggregated values with values from downsampled
// time series. The implementation used depends on both the aggregation
// method and the fill policy.
//
// Generally, these instances CANNOT be used with multiple
// goroutines
type updaterType interface {
	// Update updates aggregatorToBeUpdated with the values in
	// downsampledTimeSeries.
	Update(
		downsampledTimeSeries getByIndexType,
		aggregatorToBeUpdated adderType)
}

// getByIndexType instances can retrieve a value by index. Each index
// represents some uniform time slice. The larger the index, the more
// recent the time slice.
type getByIndexType interface {
	Len() int
	// Get returns false for second value if value is missing.
	Get(index int) (float64, bool)
}

type adderType interface {
	// Add adds a new value at index, which represents some time slice
	// The aggregation method such as sum, avg, max, etc. dictates how
	// Add adds values.
	Add(index int, value float64)
}

type aggregatorListType interface {
	getByIndexType
	adderType
	// Clear resets this instance just as if no values have been
	// added to it.
	Clear()
}

// downSampleType is the tsdb.Aggregator implementation
type downSampleType struct {
	// aggregators stores the result of aggregating all the time series
	aggregators aggregatorListType
	// downAgg stores the current downsampled time series
	downAgg aggregatorListType
	// updater updates aggregators with the current downsampled time series
	// as dictated by the downsample aggregation method and the fill policy
	updater updaterType
	// downSamplePolicy understands how to convert between time slices and
	// indexes.
	downSamplePolicy downSamplePolicyType
	optionalRateSpec *RateSpec
}

func _new(
	start, end float64,
	agg *Aggregator,
	downSample float64,
	downAgg *Aggregator,
	fillPolicy FillPolicy,
	optionalRateSpec *RateSpec) tsdb.Aggregator {
	if end < start {
		panic("end cannot be less than start")
	}
	result := &downSampleType{
		downSamplePolicy: newDownSamplePolicyType(start, downSample),
	}
	length := result.downSamplePolicy.IndexOf(end) + 1
	result.aggregators = agg.aggListCreater(length)
	result.downAgg = downAgg.aggListCreater(length)
	result.updater = downAgg.updaterCreater.Get(length, fillPolicy)
	if optionalRateSpec != nil {
		rateSpecCopy := *optionalRateSpec
		result.optionalRateSpec = &rateSpecCopy
	}
	return result

}

func (d *downSampleType) Add(values tsdb.TimeSeries) {
	valueLen := len(values)
	// Process incoming time series one time slice at a time
	for startIdx, endIdx := 0, 0; startIdx < valueLen; startIdx = endIdx {
		endIdx = d.downSamplePolicy.NextSample(values, startIdx)
		downSampledIdx := d.downSamplePolicy.IndexOf(values[startIdx].Ts)
		// Add values in current time slice to d.downAgg one at a time.
		for i := startIdx; i < endIdx; i++ {
			d.downAgg.Add(downSampledIdx, values[i].Value)
		}
	}
	// Update d.aggregators with current downsampled time series
	d.updater.Update(d.downAgg, d.aggregators)
	// finally clear current downsampled time series so that it is ready to
	// use the next time.
	d.downAgg.Clear()
}

func computeRate(
	rateSpec *RateSpec, change, downsampleSize float64) (rate float64) {
	if !rateSpec.Counter {
		return change / downsampleSize
	}
	if change < 0.0 {
		rate = (change + rateSpec.CounterMax) / downsampleSize
	} else {
		rate = change / downsampleSize
	}
	if rate < 0.0 {
		return 0.0
	}
	if rateSpec.ResetValue > 0.0 && rate > rateSpec.ResetValue {
		return 0.0
	}
	return
}

func (d *downSampleType) Aggregate() (result tsdb.TimeSeries) {
	aggLen := d.aggregators.Len()
	if d.optionalRateSpec != nil {
		rateSpec := d.optionalRateSpec
		values, start, end := doLinearInterpolation(d.aggregators)
		dsSize := d.downSamplePolicy.DownSampleSize()
		for i := start; i < end-1; i++ {
			rate := computeRate(
				rateSpec, values[i+1]-values[i], dsSize)
			result = append(result, tsdb.TsValue{
				Ts:    d.downSamplePolicy.TSOf(i),
				Value: rate,
			})
		}
		return
	}
	// Convert aggregators field to the aggregated time series with the help
	// of the downSamplePolicy field.
	for i := 0; i < aggLen; i++ {
		value, ok := d.aggregators.Get(i)
		if ok {
			result = append(result, tsdb.TsValue{
				Ts:    d.downSamplePolicy.TSOf(i),
				Value: value,
			})
		}
	}
	return
}
