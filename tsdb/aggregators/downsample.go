package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

type downSamplePolicyType struct {
	start          int64
	downSampleSize int64
}

func newDownSamplePolicyType(
	start, downSampleSize float64) downSamplePolicyType {
	if downSampleSize < 1.0 {
		downSampleSize = 1.0
	}
	downSampleAsInt := int64(downSampleSize)
	startAsInt := int64(start)
	adjustedStartAsInt := startAsInt / downSampleAsInt * downSampleAsInt
	// Safety in case startAsInt is negative
	if adjustedStartAsInt > startAsInt {
		adjustedStartAsInt -= downSampleAsInt
	}
	return downSamplePolicyType{
		start: adjustedStartAsInt, downSampleSize: downSampleAsInt}
}

func (p *downSamplePolicyType) IndexOf(ts float64) int {
	return int((int64(ts) - p.start) / p.downSampleSize)
}

func (p *downSamplePolicyType) TSOf(index int) float64 {
	return float64(p.start + int64(index)*p.downSampleSize)
}

func (p *downSamplePolicyType) NextSample(
	values tsdb.TimeSeries, start int) int {
	index := p.IndexOf(values[start].Ts)
	length := len(values)
	for i := start + 1; i < length; i++ {
		if p.IndexOf(values[i].Ts) != index {
			return i
		}
	}
	return length
}
