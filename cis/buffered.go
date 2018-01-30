package cis

func (b *Buffered) write(stats Stats) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statsList = append(b.statsList, stats)
	if len(b.statsList) == cap(b.statsList) {
		return b._flush()
	}
	return 0, nil
}

func (b *Buffered) flush() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b._flush()
}

func (b *Buffered) _flush() (int, error) {
	if len(b.statsList) == 0 {
		return 0, nil
	}
	l := len(b.statsList)
	result := b.writer.WriteAll(b.statsList)
	b.statsList = b.statsList[:0]
	if result != nil {
		return l, result
	}
	return l, nil
}
