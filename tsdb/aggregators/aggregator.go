package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

type aggregatorListType interface {
	Len() int
	Add(index int, value float64)
	Clear()
	Get(index int) (float64, bool)
}

type downSampleType struct {
	aggregators      aggregatorListType
	downAgg          aggregatorListType
	fillPolicy       FillPolicy
	downSamplePolicy downSamplePolicyType
}

func _new(
	start, end float64,
	agg Aggregator,
	downSample float64,
	downAgg Aggregator,
	fillPolicy FillPolicy) tsdb.Aggregator {
	if end < start {
		panic("end cannot be less than start")
	}
	result := &downSampleType{
		downSamplePolicy: newDownSamplePolicyType(start, downSample),
		fillPolicy:       fillPolicy,
	}
	length := result.downSamplePolicy.IndexOf(end) + 1
	result.aggregators = agg(length)
	result.downAgg = downAgg(length)
	return result
}

func (d *downSampleType) Add(values tsdb.TimeSeries) {
	valueLen := len(values)
	for startIdx, endIdx := 0, 0; startIdx < valueLen; startIdx = endIdx {
		endIdx = d.downSamplePolicy.NextSample(values, startIdx)
		downSampledIdx := d.downSamplePolicy.IndexOf(values[startIdx].Ts)
		for i := startIdx; i < endIdx; i++ {
			d.downAgg.Add(downSampledIdx, values[i].Value)
		}
	}
	length := d.downAgg.Len()
	for i := 0; i < length; i++ {
		downValue, ok := d.downAgg.Get(i)
		if ok {
			d.aggregators.Add(i, downValue)
		} else if d.fillPolicy == Zero {
			d.aggregators.Add(i, 0.0)
		}
	}
	d.downAgg.Clear()
}

func (d *downSampleType) Aggregate() (result tsdb.TimeSeries) {
	aggLen := d.aggregators.Len()
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
