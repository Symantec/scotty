package consul

import (
	"fmt"
	liblog "github.com/Symantec/Dominator/lib/log"
	"github.com/hashicorp/consul/api"
	syslog "log"
	"math"
	"strconv"
	"sync"
	"time"
)

type keyCollectionType struct {
	NextStartKey  string
	LockKey       string
	ConfigFileKey string
}

func (k *keyCollectionType) Init(namespace string) {
	k.NextStartKey = fmt.Sprintf("service/scotty/%s/nextStart", namespace)
	k.LockKey = fmt.Sprintf("service/scotty/%s/leader", namespace)
	k.ConfigFileKey = fmt.Sprintf("service/scotty/%s/configFile", namespace)
}

type connectionType struct {
	keys   keyCollectionType
	lock   *api.Lock
	kv     *api.KV
	logger liblog.Logger
}

func (c *connectionType) printf(format string, v ...interface{}) {
	if c.logger == nil {
		syslog.Printf(format, v...)
	} else {
		c.logger.Printf(format, v...)
	}
}

func (c *connectionType) mustSucceed(f func() error) {
	sleepDur := 10 * time.Second
	for err := f(); err != nil; err = f() {
		c.printf("Consul Error: %v; retry in: %v", err, sleepDur)
		time.Sleep(sleepDur)
		sleepDur *= 2
	}
}

func (c *connectionType) decode(raw []byte) (int64, error) {
	if raw == nil {
		return 0, nil
	}
	return strconv.ParseInt(string(raw), 10, 64)
}

func (c *connectionType) encode(value int64) []byte {
	return ([]byte)(strconv.FormatInt(value, 10))
}

// EnsureLeadership blocks until this process is the leader.
// If this process is already leader, EnsureLeadership returns right away.
// Note that there is no guarantee that this process will remain leader for
// any length of time after EnsureLeadership returns. This is why we have
// to use other primitives such as compare and set to stay safe.
func (c *connectionType) EnsureLeadership() {
	c.mustSucceed(func() error {
		_, err := c.lock.Lock(nil)
		if err == api.ErrLockHeld {
			// We have the lock already so we are good
			return nil
		}
		return err
	})
	return
}

func (c *connectionType) getNextStart() (nextStart int64, modifyIndex uint64) {
	c.mustSucceed(func() error {
		kvPair, _, err := c.kv.Get(c.keys.NextStartKey, nil)
		if err != nil {
			return err
		}
		if kvPair == nil {
			return nil
		}
		ns, _ := c.decode(kvPair.Value)
		nextStart = ns
		modifyIndex = kvPair.ModifyIndex
		return nil
	})
	return
}

// GetNextStart returns the next start time of lease stored in consul
// to be assigned to any process requesting a lease. If this start time
// corresponds to the end time of the current lease a process has,
// that process can just extend their lease.
// Start times are in seconds after Jan 1, 1970.
func (c *connectionType) GetNextStart() int64 {
	result, _ := c.getNextStart()
	return result
}

func (c *connectionType) cas(nextStart int64, modifyIndex uint64) (
	success bool) {
	c.mustSucceed(func() error {
		kvPair := &api.KVPair{
			Key:         c.keys.NextStartKey,
			ModifyIndex: modifyIndex,
			Value:       c.encode(nextStart),
		}
		outcome, _, err := c.kv.CAS(kvPair, nil)
		if err != nil {
			return err
		}
		success = outcome
		return nil
	})
	return
}

// CAS atomically updates the the next start time stored in consul using
// compare-and-set. In order for the CAS call to succeed lastStart must
// equal the the previously stored value. CAS returns true if it succeeded
// or false if lastStart does not equal the previously stored value.
func (c *connectionType) CAS(lastStart, nextStart int64) bool {
	start, modifyIndex := c.getNextStart()
	// actual start had better match what we expect
	if start != lastStart {
		return false
	}
	return c.cas(nextStart, modifyIndex)
}

// Put simply stores a value at a given key within consul. On error, Put
// returns the error.
func (c *connectionType) Put(key string, value string) error {
	kvPair := &api.KVPair{Key: key, Value: ([]byte)(value)}
	_, err := c.kv.Put(kvPair, nil)
	return err
}

func (c *connectionType) PutConfigFile(value string) error {
	return c.Put(c.keys.ConfigFileKey, value)
}

// get returns a the value stored in consul for a particular key. If caller
// provides a non-zero index, get blocks until the value at key changes.
// get returns the stored value, whether or not a value is stored, the
// index to pass to the next call to get in order to block until value
// changes again and a possible error.
func (c *connectionType) get(key string, index uint64) (
	value string, ok bool, nextIndex uint64, err error) {
	options := &api.QueryOptions{WaitIndex: index}
	kvPair, qm, err := c.kv.Get(key, options)
	if err != nil {
		return
	}
	if kvPair != nil {
		value = string(kvPair.Value)
		ok = true
	}
	nextIndex = qm.LastIndex
	return
}

func (c *connectionType) getConfigFile(index uint64) (
	value string, ok bool, nextIndex uint64, err error) {
	return c.get(c.keys.ConfigFileKey, index)
}

// Get works like get, but retries with exponential back off instead of
// returning an error.
func (c *connectionType) Get(key string, index uint64) (
	value string, ok bool, nextIndex uint64) {
	c.mustSucceed(func() error {
		var err error
		value, ok, nextIndex, err = c.get(key, index)
		return err
	})
	return
}

// Watch watches the value for a given key. It works just like
// Coordinator.Watch but allows caller to specify a key.
func (c *connectionType) Watch(key string, done <-chan struct{}) <-chan string {
	result := make(chan string)
	go func() {
		defer close(result)
		var index uint64
		var lastValue string
		firstValue := true
		for {
			var value string
			var ok bool
			// Unfortunately we cannot interrupt blocking Get call.
			if done != nil {
				select {
				case <-done:
					return
				default:
					value, ok, index = c.Get(key, index)
				}
			} else {
				value, ok, index = c.Get(key, index)
			}
			// If value not found, try again
			if !ok {
				continue
			}
			if firstValue || value != lastValue {
				// Check for done here as pushing to channel could
				// block forever
				if done != nil {
					select {
					case <-done:
						return
					case result <- value:
					}
				} else {
					result <- value
				}
				lastValue = value
				firstValue = false
			}
		}
	}()
	return result
}

func (c *connectionType) WatchConfigFile(done <-chan struct{}) <-chan string {
	return c.Watch(c.keys.ConfigFileKey, done)
}

// coordinator encapsulates a connection to consul and maintains the
// current lease this process has.
type coordinator struct {
	conn  connectionType
	lock  sync.Mutex
	start int64
	end   int64
	// If nil, start and end are the current lease. non-nil means that
	// lease is in the process of being changed. The finishing of the lease
	// update closes the non-nil channel.
	updateCh chan struct{}
}

// ExtendLease is always run by a single goroutine on behalf of any
// goroutines waiting on a new lease.
func (c *coordinator) ExtendLease(
	lastEndTime, minLeaseSpan, timeToInclude int64) {
	nextStart := c.conn.GetNextStart()

	if nextStart != lastEndTime {
		// If we get here, someone else leased time since we leased so we
		// need to ensure we have leadership. Moreover, we must set the
		// start of our lease to nextStart.
		c.conn.EnsureLeadership()
		// Compute the new end time of our lease.
		newEnd := timeToInclude + minLeaseSpan
		if newEnd < nextStart+minLeaseSpan {
			newEnd = nextStart + minLeaseSpan
		}
		if !c.conn.CAS(nextStart, newEnd) {
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
	if !c.conn.CAS(nextStart, newEnd) {
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

// CheckExistingLease returns the existing lease with a nil channel if
// timeToInclude falls within the lease. Otherwise it extends / changes
// the lease, and returns 0, 0, non-nil channel that caller must select on
// before retrying the call to get the updated lease.
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

// Lease is just like the public Lease method in this package.
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

func newCoordinator(namespace string, logger liblog.Logger) (
	result *coordinator, err error) {
	coord := &coordinator{}

	coord.conn.keys.Init(namespace)

	// Best I can tell, these calls don't do any network RPC's. They seem to
	// report errors only if programmer misuses.
	client, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return
	}
	coord.conn.lock, err = client.LockKey(coord.conn.keys.LockKey)
	if err != nil {
		return
	}
	coord.conn.kv = client.KV()
	coord.conn.logger = logger
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
