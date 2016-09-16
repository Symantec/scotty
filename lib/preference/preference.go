package preference

func (p *Preference) indexes() []int {
	result := make([]int, p.count)
	result[0] = p._firstIndex
	index := 1
	for i := 0; i < p.count; i++ {
		if i != p._firstIndex {
			result[index] = i
			index++
		}
	}
	return result
}

func (p *Preference) firstIndex() int {
	return p._firstIndex
}

func (p *Preference) setFirstIndex(index int) {
	if index != p._firstIndex {
		p.consecutiveCalls = 0
	}
	p.consecutiveCalls++
	if p.consecutiveCalls == p.consecutiveCallsForReset {
		p._firstIndex = 0
		p.consecutiveCalls = 0
	} else {
		p._firstIndex = index
	}
	// Don't bother tracking consecutive calls user didn't specify
	// consecutive calls needed for reset
	if p.consecutiveCallsForReset == 0 {
		p.consecutiveCalls = 0
	}
}
