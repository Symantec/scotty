package aggregators

type minListType []struct {
	valid bool
	min   float64
}

func (a minListType) Len() int {
	return len(a)
}

func (a minListType) Add(index int, value float64) {
	if !a[index].valid || value < a[index].min {
		a[index].min = value
		a[index].valid = true
	}
}

func (a minListType) Get(index int) (float64, bool) {
	return a[index].min, a[index].valid
}

func (a minListType) Clear() {
	for i := range a {
		a[i].valid = false
	}
}
