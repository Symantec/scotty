package datastructs

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"sync"
	"time"
)

// HostsAndPorts is a map between "hostname:port" and corresponding Id
type HostsAndPorts map[string]*scotty.Machine

// Copy returns a copy of this instance.
func (h HostsAndPorts) Copy() HostsAndPorts {
	return h.copy()
}

// AddIfAbsent adds a new host and port to this instance if they are
// not already there.
func (h HostsAndPorts) AddIfAbsent(host string, port int) {
	h.addIfAbsent(host, port)
}

// HostsPortsAndStore consists of the current active hosts and ports and the
// current metrics store. An instance of this type is expected to be global
// and can be used with multiple goroutines
type HostsPortsAndStore struct {
	mutex         sync.Mutex
	store         *store.Store
	hostsAndPorts HostsAndPorts
}

// Return the current store along with the hosts and ports
func (h *HostsPortsAndStore) Get() (
	s *store.Store, hostsAndPorts HostsAndPorts) {
	return h.get()
}

// Initialize this instance for the first time
func (h *HostsPortsAndStore) Init(
	valueCountPerPage, pageCount int, hostsAndPorts HostsAndPorts) {
	h.update(
		hostsAndPorts,
		store.NewBuilder(valueCountPerPage, pageCount))
}

// Update this instance with new hosts and ports.
func (h *HostsPortsAndStore) Update(hostsAndPorts HostsAndPorts) {
	oldStore, _ := h.Get()
	h.update(hostsAndPorts, oldStore.NewBuilder())
}

// ApplicationStatus represents the status of a single application.
type ApplicationStatus struct {
	MachineId *scotty.Machine
	Status    scotty.Status

	// The zero value means no successful read
	LastReadTime time.Time
	// A zero value means no successful poll
	PollTime time.Duration

	Down bool
}

// Staleness returns the staleness for this application.
// A negative value means no successful read happened.
func (a *ApplicationStatus) Staleness() time.Duration {
	if a.LastReadTime.IsZero() {
		return 0
	}
	return time.Now().Sub(a.LastReadTime)
}

// ApplicationStatuses is thread safe representation of application statuses
type ApplicationStatuses struct {
	lock sync.Mutex
	// The ApplicationStatus objects in the map are mutable to make
	// updates more memory efficient. lock protects each ApplicationStatus
	// object as well as the map itself.
	byMachine map[*scotty.Machine]*ApplicationStatus
}

func NewApplicationStatuses() *ApplicationStatuses {
	return &ApplicationStatuses{
		byMachine: make(map[*scotty.Machine]*ApplicationStatus),
	}
}

func (a *ApplicationStatuses) Update(
	m *scotty.Machine, newState *scotty.State) {
	a.update(m, newState)
}

func (a *ApplicationStatuses) GetAll(
	hostsAndPorts HostsAndPorts) (result []*ApplicationStatus) {
	return a.getAll(hostsAndPorts)
}
