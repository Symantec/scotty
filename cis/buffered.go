package cis

func (b *Buffered) write(stats Stats) ([]Stats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statsList = append(b.statsList, stats)
	if len(b.statsList) == cap(b.statsList) {
		return b._flush()
	}
	return nil, nil
}

func (b *Buffered) flush() ([]Stats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b._flush()
}

func (b *Buffered) _flush() ([]Stats, error) {
	if len(b.statsList) == 0 {
		return nil, nil
	}
	result := make([]Stats, len(b.statsList))
	copy(result, b.statsList)
	err := b.writer.WriteAll(b.statsList)
	b.statsList = b.statsList[:0]
	return result, err
}
