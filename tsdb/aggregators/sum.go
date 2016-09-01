package aggregators

type sumListType []struct {
	sum   float64
	valid bool
}

func (a sumListType) Len() int {
	return len(a)
}

func (a sumListType) Add(index int, value float64) {
	a[index].sum += value
	a[index].valid = true
}

func (a sumListType) Get(index int) (float64, bool) {
	return a[index].sum, a[index].valid
}

func (a sumListType) Clear() {
	for i := range a {
		a[i].sum = 0.0
		a[i].valid = false
	}
}
