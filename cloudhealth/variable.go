package cloudhealth

func (v *FVariable) add(x float64) {
	if v.Count == 0 {
		v.Count = 1
		v.Min = x
		v.Max = x
		v.Sum = x
		return
	}
	v.Count++
	if x < v.Min {
		v.Min = x
	}
	if x > v.Max {
		v.Max = x
	}
	v.Sum += x
}

func (v *IVariable) add(x uint64) {
	if v.Count == 0 {
		v.Count = 1
		v.Min = x
		v.Max = x
		v.Sum = x
		return
	}
	v.Count++
	if x < v.Min {
		v.Min = x
	}
	if x > v.Max {
		v.Max = x
	}
	v.Sum += x
}
