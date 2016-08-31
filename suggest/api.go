// Package suggest contains routines to implement a suggest engine
package suggest

import (
	"github.com/google/btree"
	"sync"
)

// Suggester is the interface for all suggest engines
type Suggester interface {
	// Suggest returns the strings in this instance that start with q.
	// If max > 0, then Suggest returns at most max results; otherwise, it
	// returns all results.
	Suggest(max int, q string) (result []string)
}

// Adder is the interface for adding to a suggest engine.
type Adder interface {
	// Add makes best effort to eventually add s to this instance.
	Add(s string)
}

// Engine represents a suggest engine which multiple goroutines may safely
// use.
type Engine struct {
	incomingCh  chan string
	awaitCh     chan bool
	lock        sync.RWMutex
	suggestions *btree.BTree
}

// NewEngine creates a new engine.
func NewEngine() *Engine {
	return newEngine()
}

// Add implements the Adder interface.
//
// Add returns immediately before s is added. While Add makes best effort
// to ensure that s is eventually added to this instance, Add makes no
// guarantee that it will happen.
func (e *Engine) Add(s string) {
	e.add(s)
}

// Await blocks the caller until all in progress add requests complete.
func (e *Engine) Await() {
	e.await()
}

// Suggest implements the Suggester interface.
func (e *Engine) Suggest(max int, q string) (result []string) {
	return e.suggest(max, q)
}

// NewSuggester returns a Suggester that presents the given suggestions in
// the same order. NewSuggester takes a defensive copy of suggestions.
func NewSuggester(suggestions ...string) Suggester {
	return newSuggester(suggestions)
}
