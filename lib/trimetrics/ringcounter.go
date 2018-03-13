package trimetrics

type ringCounterType struct {
	size            int
	counts          []int64
	total           int64
	currentInterval uint64
}

func (r *ringCounterType) Init(size int) {
	if size <= 0 {
		panic("size must be > 0")
	}
	r.size = size
	r.counts = make([]int64, size)
}

func (r *ringCounterType) Inc(interval uint64, count int64) {
	if r.advanceTo(interval) {
		r.counts[r.index()] += count
		r.total += count
	}
}

func (r *ringCounterType) Total(interval uint64) int64 {
	r.advanceTo(interval)
	return r.total
}

func (r *ringCounterType) index() int {
	return int(r.currentInterval % uint64(r.size))
}

func (r *ringCounterType) advanceTo(interval uint64) bool {
	if interval < r.currentInterval {
		return false
	}
	// No need to advance more then r.size as doing so zeros out the
	// total and the counts
	advanceCount := 0
	for r.currentInterval < interval && advanceCount < r.size {
		r.advanceOne()
		advanceCount++
	}
	r.currentInterval = interval
	return true
}

func (r *ringCounterType) advanceOne() {
	r.currentInterval++
	idx := r.index()
	r.total -= r.counts[idx]
	r.counts[idx] = 0
}
