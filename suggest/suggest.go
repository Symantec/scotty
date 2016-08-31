// Package suggest contains routines to implement a suggest engine
package suggest

import (
	"github.com/google/btree"
	"strings"
)

type constEngineType []string

func newSuggester(suggestions []string) Suggester {
	result := make(constEngineType, len(suggestions))
	copy(result, suggestions)
	return result
}

func (e constEngineType) Suggest(max int, q string) (result []string) {
	for _, suggestion := range e {
		if strings.HasPrefix(suggestion, q) {
			result = append(result, suggestion)
			// break out if we have reached max
			if max > 0 && max == len(result) {
				return
			}
		}
	}
	return
}

func newEngine() *Engine {
	result := &Engine{
		incomingCh:  make(chan string, 10000),
		awaitCh:     make(chan bool),
		suggestions: btree.New(100),
	}
	go result.loop()
	return result

}

func (e *Engine) await() {
	e.awaitCh <- true
}

func (e *Engine) add(s string) {
	select {
	case e.incomingCh <- s:
	default:
		// If we can't add, don't sweat it. Its only a suggest engine.
	}
}

func (e *Engine) suggest(max int, q string) (result []string) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	e.suggestions.AscendGreaterOrEqual(
		item(q),
		func(i btree.Item) bool {
			str := string(i.(item))
			if !strings.HasPrefix(str, q) {
				return false
			}
			result = append(result, string(i.(item)))
			return max <= 0 || len(result) < max
		})
	return
}

func (e *Engine) loop() {
	for {
		for pendingAdds := true; pendingAdds; {
			select {
			case toBeAdded := <-e.incomingCh:
				e._add(toBeAdded)
			default:
				// No more pending adds
				pendingAdds = false
			}
		}

		// We are done adding values for now, unblock any Await calls.
		for pendingAwaits := true; pendingAwaits; {
			select {
			case <-e.awaitCh:
			default:
				pendingAwaits = false
			}
		}
	}
}

func (e *Engine) _add(s string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.suggestions.ReplaceOrInsert(item(s))
}

type item string

func (i item) Less(than btree.Item) bool {
	return string(i) < string(than.(item))
}
