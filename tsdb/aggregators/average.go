package aggregators

type averageListType []struct {
	count uint
	sum   float64
}

func (a averageListType) Len() int {
	return len(a)
}

func (a averageListType) Add(index int, value float64) {
	a[index].count++
	a[index].sum += value
}

func (a averageListType) Get(index int) (float64, bool) {
	if a[index].count == 0 {
		return 0.0, false
	}
	return a[index].sum / float64(a[index].count), true
}

func (a averageListType) Clear() {
	for i := range a {
		a[i].count = 0
		a[i].sum = 0
	}
}
