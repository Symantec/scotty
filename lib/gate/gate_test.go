package gate_test

import (
	"github.com/Symantec/scotty/lib/gate"
	"sync"
	"testing"
	"time"
)

func TestPauseResume(t *testing.T) {
	g := gate.New()
	criticalSectionCount := newCounter(0)
	f := func() {
		if !g.Enter() {
			t.Error("Expected to enter critical section")
		}
		criticalSectionCount.Inc()
	}

	// Enter critical section 10x
	runParallel(f, f, f, f, f, f, f, f, f, f)

	pauseF := func() {
		g.Pause()
		// When pause is done, no one in critical section
		if out := criticalSectionCount.Get(); out != 0 {
			t.Errorf("goroutines still in crit sect: ", out)
		}
	}

	f = func() {
		// Pretend like we are doing something big in critical section
		time.Sleep(10 * time.Millisecond)
		criticalSectionCount.Dec()
		g.Exit()
	}

	// Pause + exit critical section 10x
	runParallel(pauseF, f, f, f, f, f, f, f, f, f, f)

	resumed := newCounter(0)

	resumeF := func() {
		// Pretend like we are doing something big in critical section
		time.Sleep(10 * time.Millisecond)
		resumed.Inc()
		g.Resume()
	}
	f = func() {
		if !g.Enter() {
			t.Error("Expected to enter critical section")
		}
		if resumed.Get() != 1 {
			t.Error("Resume must be called before entering crit sect")
		}
	}
	runParallel(resumeF, f)

}

func TestEndStart(t *testing.T) {
	g := gate.New()
	criticalSectionCount := newCounter(0)
	f := func() {
		if !g.Enter() {
			t.Error("Expected to enter critical section")
		}
		criticalSectionCount.Inc()
	}

	// Enter critical section 10x
	runParallel(f, f, f, f, f, f, f, f, f, f)

	endF := func() {
		g.End()
		// When end is done, no one in critical section
		if out := criticalSectionCount.Get(); out != 0 {
			t.Errorf("goroutines still in crit sect: ", out)
		}
	}

	f = func() {
		// Pretend like we are doing something big in critical section
		time.Sleep(10 * time.Millisecond)
		criticalSectionCount.Dec()
		g.Exit()
	}

	// End + exit critical section 10x
	runParallel(endF, f, f, f, f, f, f, f, f, f, f)

	if g.Enter() {
		t.Error("Critical section ended, no one allowed in.")
	}
	g.Start()
	if !g.Enter() {
		t.Error("Entering critical section expected now")
	}
}

func TestConcurrentPauseResume(t *testing.T) {
	for reps := 0; reps < 10; reps++ {
		g := gate.New()
		criticalSectionCount := newCounter(0)
		stillRunningCount := newCounter(10)
		f := func() {
			for {
				if !g.Enter() {
					stillRunningCount.Dec()
					return
				}
				criticalSectionCount.Inc()
				time.Sleep(10 * time.Millisecond)
				criticalSectionCount.Dec()
				g.Exit()
			}
		}
		// Start these critical section loops
		for i := 0; i < 10; i++ {
			go f()
		}
		// Run 5 pairs of pause / resume concurrently. This test is to
		// ensure that resuming while pausing doesn't block the pause.
		// If a pause blocks, runParallel will hang.
		runParallel(
			g.Pause, g.Resume,
			g.Pause, g.Resume,
			g.Pause, g.Resume,
			g.Pause, g.Resume,
			g.Pause, g.Resume)

		// We don't know what state we are in now. So we pause
		g.Pause()
		if out := criticalSectionCount.Get(); out != 0 {
			t.Errorf("Goroutines still in crit sect: %d", out)
		}
		if out := stillRunningCount.Get(); out != 10 {
			t.Errorf("Goroutines still in crit sect: %d", out)
		}
		g.End()
		stillRunningCount.WaitOnZero()
	}
}

func TestConcurrentStartEnd(t *testing.T) {
	for reps := 0; reps < 10; reps++ {
		g := gate.New()
		stillRunningCount := newCounter(10)
		f := func() {
			for {
				if !g.Enter() {
					stillRunningCount.Dec()
					return
				}
				time.Sleep(10 * time.Millisecond)
				g.Exit()
			}
		}
		// Start these critical section loops
		for i := 0; i < 10; i++ {
			go f()
		}
		// Run 5 pairs of end / start concurrently. This test is to
		// ensure that starting while ending doesn't block end.
		// If end blocks, runParallel will hang.
		runParallel(
			g.End, g.Start,
			g.End, g.Start,
			g.End, g.Start,
			g.End, g.Start,
			g.End, g.Start)
		// We don't know what state we are in now. So we end
		g.End()
		stillRunningCount.WaitOnZero()
	}
}

func runParallel(fs ...func()) {
	var wg sync.WaitGroup
	for _, f := range fs {
		wg.Add(1)
		go func(f func()) {
			defer wg.Done()
			f()
		}(f)
	}
	wg.Wait()
}

type counter struct {
	lock   sync.Mutex
	isZero sync.Cond
	value  int
}

func newCounter(x int) *counter {
	result := &counter{value: x}
	result.isZero.L = &result.lock
	return result
}

func (c *counter) Get() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}

func (c *counter) Inc() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
	if c.value == 0 {
		c.isZero.Broadcast()
	}
}

func (c *counter) Dec() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value--
	if c.value == 0 {
		c.isZero.Broadcast()
	}
}

func (c *counter) WaitOnZero() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for c.value != 0 {
		c.isZero.Wait()
	}
}
