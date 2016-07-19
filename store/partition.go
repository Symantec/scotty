package store

// partitionType is the interface for partitions
type partitionType interface {
	// Len returns the length of the slice
	Len() int
	// SubsetId returns the subset Id at element index.
	SubsetId(index int) interface{}
}

// formPartitionType instance can be mutated in place to form partitions
type formPartitionType interface {
	partitionType
	// Swap ith and jth element
	Swap(i, j int)
}

// formPartition forms a partition out of p
func formPartition(p formPartitionType) {
	counts := make(map[interface{}]int)
	length := p.Len()
	for i := 0; i < length; i++ {
		counts[p.SubsetId(i)]++
	}
	partitionOffsets := &partitionOffsetType{
		counts: counts,
		ranges: make(map[interface{}]sliceRangeType, len(counts)),
	}
	for i := 0; i < length; i++ {
		dest := partitionOffsets.offsetOf(p.SubsetId(i), i)
		for i < dest {
			p.Swap(i, dest)
			dest = partitionOffsets.offsetOf(p.SubsetId(i), i)
		}
	}
}

// nextSubset returns the starting index of the next subset in p.
// idx is the first index of the current subset.
// To iterate over all the subsets, caller should start with idx = 0.
// nextSubset returns len(p) when there are no more subsets.
func nextSubset(p partitionType, idx int) int {
	id := p.SubsetId(idx)
	length := p.Len()
	for i := idx + 1; i < length; i++ {
		if p.SubsetId(i) != id {
			return i
		}
	}
	return length
}

type sliceRangeType struct {
	start int
	end   int
}

type partitionOffsetType struct {
	counts         map[interface{}]int
	ranges         map[interface{}]sliceRangeType
	farthestOffset int
}

func (p *partitionOffsetType) offsetOf(id interface{}, index int) int {
	arange, ok := p.ranges[id]
	if !ok {
		arange.start = p.farthestOffset
		arange.end = p.farthestOffset
		p.farthestOffset += p.counts[id]
	}
	if index >= arange.start && index < arange.end {
		return index
	}
	arange.end++
	p.ranges[id] = arange
	return arange.end - 1
}
