package aggregators

type countListType []uint64

func (a countListType) Len() int {
	return len(a)
}

func (a countListType) Add(index int, value float64) {
	a[index]++
}

func (a countListType) Get(index int) (float64, bool) {
	return float64(a[index]), true
}

func (a countListType) Clear() {
	for i := range a {
		a[i] = 0
	}
}
