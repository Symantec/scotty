package aggregators

import (
	"github.com/Symantec/scotty/tsdb"
)

type downSamplePolicyType struct {
	start          int64
	downSampleSize int64
}

// start is the starting time in seconds after Jan 1, 1970.
// downSampleSize is the length of each time slice in seconds.
// newDownSampleSize adjusts start to be a multiple of downSampleSize and makes
// that adjusted start be index 0.
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

// IndexOf converts a timestamp to an index
func (p *downSamplePolicyType) IndexOf(ts float64) int {
	return int((int64(ts) - p.start) / p.downSampleSize)
}

// TSOf converts given index to a timestamp
func (p *downSamplePolicyType) TSOf(index int) float64 {
	return float64(p.start + int64(index)*p.downSampleSize)
}

// Given a starting index, NextSample returns the first index in given
// time series that begins the next time slice. If start is in the last
// time slice in given time series, NextSample returns the length of given
// time series.
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
