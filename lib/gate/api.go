// Package gate provides a gate synchronisation primitive.
// Gates can be either opened or closed.
// Open gates allow goroutines to proceed; closed gates block goroutines.
package gate

import (
	"sync"
)

// A Gate instance represents a single gate. The zero value is an opened
// gate ready to use.
type Gate struct {
	lock sync.Mutex
	// ch is nil if gate is opened. If gate is closed, ch is a non-nil, empty
	// channel, used to block callers of Await.
	ch chan struct{}
}

// Resume opens this gate.
func (g *Gate) Resume() {
	g.resume()
}

// Pause closes this gate.
func (g *Gate) Pause() {
	g.pause()
}

// If this instance is opened, Await returns immediately. Otherwise, Await
// blocks the caller until this instance is opened.
func (g *Gate) Await() {
	g.await()
}
