package gate

func _new() *Gate {
	var result Gate
	result.zeroReached.L = &result.lock
	result.wakeup.L = &result.lock
	result.noDrainsInProgress.L = &result.lock
	return &result
}

func (g *Gate) enter() bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	for g.paused && !g.ended {
		g.wakeup.Wait()
	}
	if g.ended {
		return false
	}
	g.count++
	return true
}

func (g *Gate) exit() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.count--
	if g.count < 0 {
		panic("negative count of goroutines in critical section")
	}
	if g.count == 0 {
		g.zeroReached.Broadcast()
	}
}

func (g *Gate) _drainCriticalSection() {
	g.drainsInProgress++
	for g.count > 0 {
		g.zeroReached.Wait()
	}
	g.drainsInProgress--
	if g.drainsInProgress == 0 {
		g.noDrainsInProgress.Broadcast()
	}
}

func (g *Gate) _waitForDraining() {
	for g.drainsInProgress > 0 {
		g.noDrainsInProgress.Wait()
	}
}

func (g *Gate) pause() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.paused = true
	g._drainCriticalSection()
}

func (g *Gate) resume() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g._waitForDraining()
	g.paused = false
	// Wake up anyone waiting in Enter function.
	g.wakeup.Broadcast()
}

func (g *Gate) end() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.ended = true
	g._drainCriticalSection()
	// Wake up anyone waiting in Enter function so that Enter returns false.
	g.wakeup.Broadcast()
}

func (g *Gate) start() {
	g.lock.Lock()
	defer g.lock.Unlock()
	g._waitForDraining()
	g.ended = false
}
