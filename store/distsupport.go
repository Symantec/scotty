package store

import (
	"github.com/Symantec/tricorder/go/tricorder/messages"
)

func (d *DistributionDelta) isZero() bool {
	if d.sum != 0.0 {
		return false
	}
	for i := range d.counts {
		if d.counts[i] != 0 {
			return false
		}
	}
	return true
}

func (d *DistributionDelta) totalCountChange() (result int64) {
	for i := range d.counts {
		result += d.counts[i]
	}
	return
}

func (d *DistributionDelta) countChanges() []int64 {
	result := make([]int64, len(d.counts))
	copy(result, d.counts)
	return result
}

func (d *DistributionDelta) add(x *DistributionDelta) {
	for i := range d.counts {
		d.counts[i] += x.counts[i]
	}
	d.sum += x.sum
}

// distExtractUpperLimits extracts the upper bounds from the buckets in dist.
// Since the last bucket has no upper bound, the number of buckets is always
// one more than the length of the returned slice.
func distExtractUpperLimits(dist *messages.Distribution) []float64 {
	result := make([]float64, len(dist.Ranges)-1)
	for i := range result {
		result[i] = dist.Ranges[i].Upper
	}
	return result
}

// distExtractCounts extracts the bucket counts from dist. The length of the
// returned slice is equal to the total number of buckets.
func distExtractCounts(dist *messages.Distribution) []uint64 {
	result := make([]uint64, len(dist.Ranges))
	for i := range result {
		result[i] = dist.Ranges[i].Count
	}
	return result
}

func upperLimitsLogicallyEqual(left, right []float64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// rangesListType is an associative cache of ranges designed to store ranges
// used for a single particular metric path.
// rangesListType uses a linear, adaptive search moving hits found to the
// front of the list. We expect the desired ranges always to be at
// the front of the list providing O(1) lookup. The only time this won't
// be the case is when the ranges change for a distribution which is very
// seldom.
type rangesListType struct {
	rangesList []*Ranges
}

func newRangesList() *rangesListType {
	return &rangesListType{}
}

// Get converts a slice of bucket upper bounds to a *Ranges pointer in such
// a way that r.Get(a) == r.Get(b) if and only if a is logically equivalent
// to b.
func (r *rangesListType) Get(upperLimits []float64) *Ranges {
	idx := r.find(upperLimits)
	if idx == -1 {
		upperLimitsCopy := make([]float64, len(upperLimits))
		copy(upperLimitsCopy, upperLimits)
		r.rangesList = append(r.rangesList, &Ranges{upperLimitsCopy})
		idx = len(r.rangesList) - 1
	}
	if idx > 0 {
		r.rangesList[idx], r.rangesList[0] = r.rangesList[0], r.rangesList[idx]
	}
	return r.rangesList[0]
}

func (r *rangesListType) find(upperLimits []float64) int {
	for i := range r.rangesList {
		if upperLimitsLogicallyEqual(
			r.rangesList[i].UpperLimits, upperLimits) {
			return i
		}
	}
	return -1
}

// rangeCacheType caches ranges by path. It guarantees that it will always
// produce the same *Ranges pointer for logically equivalent upper limits
// for the same path. However, it will produce different *Ranges pointers
// for logically equivalent upper limits if the paths are different.
type rangesCacheType struct {
	rangesByPath map[string]*rangesListType
}

func (r *rangesCacheType) Init() {
	r.rangesByPath = make(map[string]*rangesListType)
}

// Get produces a *Ranges pointer for the given path and range.
func (r *rangesCacheType) Get(
	path string, upperLimits []float64) *Ranges {
	rangesList := r.rangesByPath[path]
	if rangesList == nil {
		rangesList = newRangesList()
		r.rangesByPath[path] = rangesList
	}
	return rangesList.Get(upperLimits)
}

// distributionValuesType represents distribution values and
// lets scotty compute the *DistributionDelta based
// on current values and previous values of the same distribution.
type distributionValuesType struct {
	// The Ranges pointer of this distribution
	Ranges *Ranges
	// The generation of this distribution
	Generation uint64
	// The bucket counts of this distribution
	Counts []uint64
	// The sum of values in this distribution
	Sum float64
}

func (d *distributionValuesType) isContinuation(
	previous *distributionValuesType) bool {
	return d.Ranges == previous.Ranges && d.Generation > previous.Generation
}

// ComputeDifference computes the distribution delta between these
// distribution values and previous distribution values. If these distribution
// values are a continuation of the previous distribution values,
// ComputeDifferences returns the distribution delta. If these distribution
// values are not a continuation of the previous distribution values
// (for example Generation decreased or the Ranges fields don't match)
// ComputeDifferences returns nil.
//
// If caller passes nil for previous distribution values, ComputeDifferences
// just returns nil as if these distribution values are not a continuation.
func (d *distributionValuesType) ComputeDifferences(
	maybeNilPrevious *distributionValuesType) *DistributionDelta {
	if maybeNilPrevious == nil {
		return nil
	}
	previous := maybeNilPrevious
	if d.isContinuation(previous) {
		diffCounts := make([]int64, len(d.Counts))
		var sum float64
		for i := range diffCounts {
			diffCounts[i] = int64(d.Counts[i] - previous.Counts[i])
		}
		sum = d.Sum - previous.Sum
		return NewDistributionDelta(diffCounts, sum)
	}
	return nil
}
