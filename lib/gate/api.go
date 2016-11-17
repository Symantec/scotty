// Package gate provides management of critical sections
package gate

import (
	"sync"
)

// A Gate instance represents a single critical section of code.
type Gate struct {
	lock               sync.Mutex
	count              int
	drainsInProgress   int
	paused             bool
	ended              bool
	zeroReached        sync.Cond
	noDrainsInProgress sync.Cond
	wakeup             sync.Cond
}

// New returns a new instance with both the pause and end state cleared.
func New() *Gate {
	return _new()
}

// Caller calls Enter when entering the critical section of code.
// Enter returns true if caller is granted access to the critical section of
// code or false otherwise. Enter may also block caller.
func (g *Gate) Enter() bool {
	return g.enter()
}

// If Enter returns true, Caller must call Exit when leaving the critical
//  section/ of code.
func (g *Gate) Exit() {
	g.exit()
}

// Pause sets the pause state of this instance to true.
// Pause causes calls to Enter() to block. Once Pause() returns, the caller
// of pause is guaranteed that no goroutines are in the critical section of
// code until the next call to Resume().
func (g *Gate) Pause() {
	g.pause()
}

// Resume clears the pause state of this instance so that goroutines
// may once again enter the critical section. If Pause and Resume are called
// at the same time, Resume blocks until Pause returns before clearing the
// pause state.
func (g *Gate) Resume() {
	g.resume()
}

// End sets the end state of this instance to true.
// End causes calls to Enter() to return false. once End() returns, the caller
// is guaranteed that no goroutines are in the cricial section of code. If
// both the pause and end state of this instance are set, the end state takes
// precedence so that Enter() returns false rather than blocking.
func (g *Gate) End() {
	g.end()
}

// Start clears the end state of this instance so that goroutines may
// once again enter the critical section. If Start and End are called at
// the same time, Start blocks until End returns before clearing the end
// state.
func (g *Gate) Start() {
	g.start()
}
