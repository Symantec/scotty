package store

import (
	"github.com/Symantec/tricorder/go/tricorder/messages"
)

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

type distributionRollOverType struct {
	ranges        *Ranges
	generation    uint64
	rollOverCount uint64
}

func (d *distributionRollOverType) hasRolledOver(
	ranges *Ranges, generation uint64) bool {
	return ranges != d.ranges || generation < d.generation
}

func (d *distributionRollOverType) UpdateAndFetchRollOverCount(
	ranges *Ranges, generation uint64) uint64 {
	if d.hasRolledOver(ranges, generation) {
		d.rollOverCount++
	}
	d.ranges = ranges
	d.generation = generation
	return d.rollOverCount
}
