package gate

// Opens gate by setting channel to nil and returning previous value of
// channel in one go. Caller must close returned channel to unblock any
// goroutines waiting on gate to open.
func (g *Gate) clearChannel() (result chan struct{}) {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.ch != nil {
		result = g.ch
		g.ch = nil
	}
	return
}

func (g *Gate) resume() {
	if c := g.clearChannel(); c != nil {
		close(c)
	}
}

func (g *Gate) pause() {
	g.lock.Lock()
	defer g.lock.Unlock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
}

func (g *Gate) get() <-chan struct{} {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.ch
}

func (g *Gate) await() {
	if c := g.get(); c != nil {
		// If gate is closed, we must block the caller
		<-c
	}
}
