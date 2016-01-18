package datastructs

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"sync"
	"time"
)

// HostsAndPorts is a map between "hostname:port" and corresponding Id
type HostsAndPorts map[string]*scotty.Endpoint

// Copy returns a copy of this instance.
func (h HostsAndPorts) Copy() HostsAndPorts {
	return h.copy()
}

// AddIfAbsent adds a new host and port to this instance if they are
// not already there. If orig is non nil, AddIfAbsent looks for a matching
// endpoint object in orig. If it finds one, it reuses it instead of creating
// a new one.
func (h HostsAndPorts) AddIfAbsent(
	orig HostsAndPorts, host string, port int) {
	h.addIfAbsent(orig, host, port)
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
// Caller must not modify contents of hostsAndPorts directly. Insteead,
// caller must call Copy() and hostsAndPorts to get a copy and mofiy the
// copy.
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
	EndpointId *scotty.Endpoint
	Status     scotty.Status

	// The zero value means no successful read
	LastReadTime time.Time
	// A zero value means no successful poll
	PollTime time.Duration

	// Initial metric count
	InitialMetricCount int

	// Whether or not it is currently down.
	Down bool

	changedMetrics_Sum   int64
	changedMetrics_Count int64
}

func (a *ApplicationStatus) AverageChangedMetrics() float64 {
	if a.changedMetrics_Count == 0 {
		return 0.0
	}
	return float64(a.changedMetrics_Sum) / float64(a.changedMetrics_Count)
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
	byEndpoint map[*scotty.Endpoint]*ApplicationStatus
}

func NewApplicationStatuses() *ApplicationStatuses {
	return &ApplicationStatuses{
		byEndpoint: make(map[*scotty.Endpoint]*ApplicationStatus),
	}
}

func (a *ApplicationStatuses) Update(
	e *scotty.Endpoint, newState *scotty.State) {
	a.update(e, newState)
}

func (a *ApplicationStatuses) LogChangedMetricCount(
	e *scotty.Endpoint, metricCount int) {
	a.logChangedMetricCount(e, metricCount)
}

func (a *ApplicationStatuses) GetAll(
	hostsAndPorts HostsAndPorts) (result []*ApplicationStatus) {
	return a.getAll(hostsAndPorts)
}

type Application struct {
	name string
	port int
}

func (a *Application) Name() string {
	return a.name
}

func (a *Application) Port() int {
	return a.port
}

type ApplicationList struct {
	byPort map[int]*Application
}

func (a *ApplicationList) All() []*Application {
	return a.all()
}

func (a *ApplicationList) ByPort(port int) *Application {
	return a.byPort[port]
}

type ApplicationListBuilder struct {
	listPtr **ApplicationList
}

func NewApplicationListBuilder() *ApplicationListBuilder {
	return newApplicationListBuilder()
}

func (a *ApplicationListBuilder) Add(port int, applicationName string) {
	a.add(port, applicationName)
}

func (a *ApplicationListBuilder) Build() *ApplicationList {
	return a.build()
}
