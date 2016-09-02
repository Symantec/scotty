package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

type averageValueType struct {
	Count uint
	Sum   float64
}

func (a *averageValueType) Add(value float64) {
	a.Count++
	a.Sum += value
}

func (a *averageValueType) Average() float64 {
	if a.Count == 0 {
		return 0.0
	}
	return a.Sum / float64(a.Count)
}

type averageType struct {
	averages         []averageValueType
	downSamplePolicy downSamplePolicyType
}

func newAverage(start, end, downSample float64) tsdb.Aggregator {
	if end < start {
		panic("end cannot be less than start")
	}
	result := &averageType{
		downSamplePolicy: newDownSamplePolicyType(start, downSample),
	}
	result.averages = make([]averageValueType, result.downSamplePolicy.IndexOf(end)+1)
	return result
}

func (a *averageType) Add(values tsdb.TimeSeries) {
	valueLen := len(values)
	for startIdx, endIdx := 0, 0; startIdx < valueLen; startIdx = endIdx {
		endIdx = a.downSamplePolicy.NextSample(values, startIdx)
		downsampledValue := average(values[startIdx:endIdx])
		downSampledIdx := a.downSamplePolicy.IndexOf(values[startIdx].Ts)
		a.averages[downSampledIdx].Add(downsampledValue)
	}
}

func (a *averageType) Aggregate() (result tsdb.TimeSeries) {
	for i := range a.averages {
		if a.averages[i].Count > 0 {
			result = append(result, tsdb.TsValue{
				Ts:    a.downSamplePolicy.TSOf(i),
				Value: a.averages[i].Average(),
			})
		}
	}
	return
}

func average(values tsdb.TimeSeries) float64 {
	count := len(values)
	if count == 0 {
		panic("Can't average empty set")
	}
	sum := 0.0
	for i := range values {
		sum += values[i].Value
	}
	return sum / float64(count)
}
