package aggregators

type maxListType []struct {
	valid bool
	max   float64
}

func (a maxListType) Len() int {
	return len(a)
}

func (a maxListType) Add(index int, value float64) {
	if !a[index].valid || value > a[index].max {
		a[index].max = value
		a[index].valid = true
	}
}

func (a maxListType) Get(index int) (float64, bool) {
	return a[index].max, a[index].valid
}

func (a maxListType) Clear() {
	for i := range a {
		a[i].valid = false
	}
}
