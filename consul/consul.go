package consul

import (
	"github.com/hashicorp/consul/api"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
)

const (
	kNextStartKey = "service/scotty/nextStart"
	kLockKey      = "service/scotty/leader"
)

type kernelType struct {
	lock   *api.Lock
	kv     *api.KV
	logger *log.Logger
}

func (k *kernelType) mustSucceed(f func() error) {
	sleepDur := 10 * time.Second
	for err := f(); err != nil; err = f() {
		k.logger.Printf("Consul Error: %v; retry in: %v", err, sleepDur)
		time.Sleep(sleepDur)
		sleepDur *= 2
	}
}

func (k *kernelType) decode(raw []byte) (int64, error) {
	if raw == nil {
		return 0, nil
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

func (k *kernelType) encode(value int64) []byte {
	return ([]byte)(strconv.FormatInt(value, 10))
}

func (k *kernelType) EnsureLeadership() {
	k.mustSucceed(func() error {
		_, err := k.lock.Lock(nil)
		if err == api.ErrLockHeld {
			// We have the lock already so we are good
			return nil
		}
		return err
	})
	return
}

func (k *kernelType) getNextStart() (nextStart int64, modifyIndex uint64) {
	k.mustSucceed(func() error {
		kvPair, _, err := k.kv.Get(kNextStartKey, nil)
		if err != nil {
			return err
		}
		if kvPair == nil {
			return nil
		}
		ns, _ := k.decode(kvPair.Value)
		nextStart = ns
		modifyIndex = kvPair.ModifyIndex
		return nil
	})
	return
}

func (k *kernelType) GetNextStart() int64 {
	result, _ := k.getNextStart()
	return result
}

func (k *kernelType) cas(nextStart int64, modifyIndex uint64) (
	success bool) {
	k.mustSucceed(func() error {
		kvPair := &api.KVPair{
			Key:         kNextStartKey,
			ModifyIndex: modifyIndex,
			Value:       k.encode(nextStart),
		}
		outcome, _, err := k.kv.CAS(kvPair, nil)
		if err != nil {
			return err
		}
		success = outcome
		return nil
	})
	return
}

func (k *kernelType) CAS(lastStart, nextStart int64) bool {
	start, modifyIndex := k.getNextStart()
	// actual start had better match what we expect
	if start != lastStart {
		return false
	}
	return k.cas(nextStart, modifyIndex)
}

type coordinator struct {
	kernel   kernelType
	lock     sync.Mutex
	start    int64
	end      int64
	updateCh chan struct{}
}

// ExtendLease is always run by a single goroutine on behalf of any
// goroutines waiting on a new lease.
func (c *coordinator) ExtendLease(
	lastEndTime, minLeaseSpan, timeToInclude int64) {
	nextStart := c.kernel.GetNextStart()

	if nextStart != lastEndTime {
		// If we get here, someone else leased time since we leased so we
		// need to ensure we have leadership. Moreover, we must set the
		// start of our lease to nextStart.
		c.kernel.EnsureLeadership()
		// Compute the new end time of our lease.
		newEnd := timeToInclude + minLeaseSpan
		if newEnd < nextStart+minLeaseSpan {
			newEnd = nextStart + minLeaseSpan
		}
		if !c.kernel.CAS(nextStart, newEnd) {
			// Oops, either the previous leader did more writing while we
			// were waiting to become leader or we lost our leadership.
			// Just start over by calling ourselves again
			c.ExtendLease(lastEndTime, minLeaseSpan, timeToInclude)
			return
		}
		c.lock.Lock()
		defer c.lock.Unlock()
		// Update our lease
		c.start = nextStart
		c.end = newEnd
		// Signal that we are done extending and wake up anyone waiting on us.
		close(c.updateCh)
		c.updateCh = nil
		return
	}
	// If we get here, no one else has leased since we leased. We can just
	// extend our continguous block
	newEnd := timeToInclude + minLeaseSpan
	if !c.kernel.CAS(nextStart, newEnd) {
		// Oops, someone else assumed leadership role. Just start over by
		// calling ourselves again and returning immediately.
		c.ExtendLease(lastEndTime, minLeaseSpan, timeToInclude)
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	// Update our lease
	c.end = newEnd
	// Signal that we are done extending and wake up anyone waiting on us.
	close(c.updateCh)
	c.updateCh = nil
}

func (c *coordinator) CheckExistingLease(minLeaseSpan, timeToInclude int64) (
	start, end int64, updateCh <-chan struct{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if timeToInclude < c.end {
		return c.start, c.end, nil
	}
	if c.updateCh == nil {
		c.updateCh = make(chan struct{})
		go c.ExtendLease(c.end, minLeaseSpan, timeToInclude)
	}
	updateCh = c.updateCh
	return
}

func (c *coordinator) Lease(
	minLeaseSpan, timeToInclude float64, listener func(blocked bool)) (
	start, end float64) {
	iMinLeaseSpan := roundUp(minLeaseSpan)
	iTimeToInclude := roundUp(timeToInclude)
	var updateCh <-chan struct{}
	istart, iend, updateCh := c.CheckExistingLease(
		iMinLeaseSpan, iTimeToInclude)
	if listener != nil && updateCh != nil {
		listener(true)
	}
	// Keep trying until we have an acceptable lease
	for updateCh != nil {
		<-updateCh
		istart, iend, updateCh = c.CheckExistingLease(
			iMinLeaseSpan, iTimeToInclude)
		if listener != nil && updateCh == nil {
			listener(false)
		}
	}
	start = float64(istart)
	end = float64(iend)
	return
}

func newCoordinator(logger *log.Logger) (result *coordinator, err error) {
	coord := &coordinator{}

	// Best I can tell, these calls don't do any network RPC's. They seem to
	// report errors only if programmer misuses.
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return
	}
	coord.kernel.lock, err = client.LockKey(kLockKey)
	if err != nil {
		return
	}
	coord.kernel.kv = client.KV()
	coord.kernel.logger = logger
	result = coord
	return
}

func roundUp(x float64) int64 {
	fl := math.Floor(x)
	if x == fl {
		return int64(x)
	}
	return int64(fl + 1.0)
}
