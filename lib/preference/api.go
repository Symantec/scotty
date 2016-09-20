// Package preference contains routines for maintaining order of preference
// for expensive processes such as RPC methods.
package preference

// Preference instances maintain the order of preference for RPC methods to
// complete an API call. Although this documentation assumes the client
// is managing RPC methods, a Preference instance may be used to maintain
// order of preference for completing any expensive process.
//
// Clients store RPC methods in a slice by order of preference. The best
// method is stored in [0]; the next best in [1] etc. A preference instance
// tells the client what order to try RPC methods according to their index in
// that slice. If there are 4 RPC methods, the Indexes() method will return
// {0, 1, 2, 3} which means try the most preferred method first; the next
// preferred second and so forth. Generally, the client will have success
// with the most preferred method, but when that method fails, the client can
// tell the instance what method was successful.
//
// When the client finds a successful method, the client calls SetFirstIndex
// to tell the instance what method was successful. If client calls
// SetFirstIndex(2), Index() will return {2, 0, 1, 3} which means try the
// method at index 2 first since that one brought success, then try the most
// preferred method then the next preferred method and finally the least
// preferred method. Sometimes it may be desirable to try the most preferred
// method first eventually.
//
// For that reason, a Preference instance may be set up so that after
// SetFirstIndex() is called n consecutive times with the same index, the
// first index to try is reset to 0 to ensure that eventually the most
// preferred RPC method is tried first once again.
type Preference struct {
	// These 2 fields stay the same
	count                    int
	consecutiveCallsForReset int
	// These fields change
	_firstIndex      int
	consecutiveCalls int
}

// New creates a brand new Preference instance.
// count is the number of RPC methods to try.
// consecutiveCallsForReset is how many consecutive times SetFirstIndex()
// must be called with the same index to reset the first index
// to 0. consecutiveCallsForReset <= 0 means never reset.
//
// New panics if count <= 0.
func New(count int, consecutiveCallsForReset int) *Preference {
	if count <= 0 {
		panic("count must be at least 1")
	}
	return &Preference{
		count: count,
		consecutiveCallsForReset: consecutiveCallsForReset,
	}
}

// Indexes returns the order to try RPC methods by index
func (p *Preference) Indexes() []int {
	return p.indexes()
}

// FirstIndex returns the index of the first RPC method to try
func (p *Preference) FirstIndex() int {
	return p.firstIndex()
}

// SetFirstIndex tells this instance the index of the RPC method that
// succeeded.
//
// SetFirstIndex panics if index < 0 or index >= count.
func (p *Preference) SetFirstIndex(index int) {
	if index < 0 || index >= p.count {
		panic("required: 0 <= index < p.count")
	}
	p.setFirstIndex(index)
}
