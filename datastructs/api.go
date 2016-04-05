// Package datastructs provides miscellaneous data structures for scotty.
package datastructs

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"io"
	"sync"
	"time"
)

// ApplicationStatus represents the status of a single application.
type ApplicationStatus struct {
	EndpointId *scotty.Endpoint
	Status     scotty.Status

	// The name of the endpoint's application
	Name string

	// True if this machine is active, false otherwise.
	Active bool

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

// AverageChangedMetrics returns how many metrics change value.
func (a *ApplicationStatus) AverageChangedMetrics() float64 {
	if a.changedMetrics_Count == 0 {
		return 0.0
	}
	return float64(a.changedMetrics_Sum) / float64(a.changedMetrics_Count)
}

// Staleness returns the staleness for this application.
// A zero value means no successful read happened.
func (a *ApplicationStatus) Staleness() time.Duration {
	if a.LastReadTime.IsZero() {
		return 0
	}
	return time.Now().Sub(a.LastReadTime)
}

// ApplicationStatuses is thread safe representation of application statuses
type ApplicationStatuses struct {
	appList *ApplicationList
	// A function must hold this lock when changing the status
	// (active vs. inactive) of the endpoints or when adding new
	// endpoints to ensure that when it returns, all of the time series
	// of each inactive endpoint are also inactive and that
	// all endpoints in this instance are also in the current store.
	// If this lock is acquired, it must be acquired before the normal
	// lock for this instance.
	statusChangeLock sync.Mutex
	// The normal lock for this instance
	lock sync.Mutex
	// The ApplicationStatus objects in the map are mutable to make
	// updates more memory efficient. lock protects each ApplicationStatus
	// object as well as the map itself.
	byEndpoint   map[*scotty.Endpoint]*ApplicationStatus
	byHostPort   map[hostAndPort]*scotty.Endpoint
	currentStore *store.Store
}

// NewApplicationStatuses creates a new ApplicationStatus instance.
// astore should be a newly initialized store.
// Since the newly created instance will create new store.Store instances
// whenever active machines change, caller should give up any reference it
// has to astore and use the Store() method to get the current store.
func NewApplicationStatuses(appList *ApplicationList, astore *store.Store) *ApplicationStatuses {
	return &ApplicationStatuses{
		appList:      appList,
		byEndpoint:   make(map[*scotty.Endpoint]*ApplicationStatus),
		byHostPort:   make(map[hostAndPort]*scotty.Endpoint),
		currentStore: astore,
	}
}

// Store returns the current store instance.
func (a *ApplicationStatuses) Store() *store.Store {
	return a.store()
}

// ApplicationList returns the application list of this instance.
func (a *ApplicationStatuses) ApplicationList() *ApplicationList {
	return a.appList
}

// Update updates the status of a single application / endpoint.
// This instance must have prior knowledge of e. That is, e must come from
// a method such as ActiveEndpointIds(). Otherwise, this method panics.
func (a *ApplicationStatuses) Update(
	e *scotty.Endpoint, newState *scotty.State) {
	a.update(e, newState)
}

// MarkHostsActiveExclusively marks all applications / endpoints each host
// within activeHosts as active while marking the rest inactive.
// MarkHostsActiveExclusively marks all time series of any machines that just
// became inactive as inactive by adding an inactive flag to each one with
// given time stamp.
// timestamp is seconds since Jan 1, 1970 GMT.
func (a *ApplicationStatuses) MarkHostsActiveExclusively(
	timestamp float64, activeHosts []string) {
	a.markHostsActiveExclusively(timestamp, activeHosts)
}

// LogChangedMetricCount logs how many metrics changed for a given
// application / endpoint.
// This instance must have prior knowledge of e. That is, e must come from
// a method such as ActiveEndpointIds(). Otherwise, this method panics.
func (a *ApplicationStatuses) LogChangedMetricCount(
	e *scotty.Endpoint, metricCount int) {
	a.logChangedMetricCount(e, metricCount)
}

// GetAll returns the statuses for all the applications.
// Clients are free to reorder the returned slice.
func (a *ApplicationStatuses) All() []*ApplicationStatus {
	return a.all()
}

// EndpointIdByHostAndName Returns the endpoint Id for given host and
// application name along with the current store. If no such host and
// application name combo exists, returns nil and the curent store.
func (a *ApplicationStatuses) EndpointIdByHostAndName(
	host, name string) (*scotty.Endpoint, *store.Store) {
	return a.endpointIdByHostAndName(host, name)
}

// ActiveEndpointIds  Returns all the active endpoints
// along with the current store.
func (a *ApplicationStatuses) ActiveEndpointIds() (
	[]*scotty.Endpoint, *store.Store) {
	return a.activeEndpointIds()
}

// Application represents a particular application in the fleet.
// Application instances are immutable.
type Application struct {
	name string
	port int
}

// Name returns the name of application
func (a *Application) Name() string {
	return a.name
}

// Port returns the port number of the application
func (a *Application) Port() int {
	return a.port
}

// ApplicationList represents all the applications in the fleet.
// ApplicationList instances are immutable.
type ApplicationList struct {
	byPort map[int]*Application
	byName map[string]*Application
}

// All lists all the applications in no particular order.
// Clients may reorder returned slice.
func (a *ApplicationList) All() []*Application {
	return a.all()
}

// ByPort returns the application running on a particular port or nil
// if no application is using port.
func (a *ApplicationList) ByPort(port int) *Application {
	return a.byPort[port]
}

// ByPort returns the application with a particular name or nil if no such
// application exists.
func (a *ApplicationList) ByName(name string) *Application {
	return a.byName[name]
}

// ApplicationListBuilder builds an ApplicationList instance.
type ApplicationListBuilder struct {
	listPtr **ApplicationList
}

func NewApplicationListBuilder() *ApplicationListBuilder {
	return newApplicationListBuilder()
}

// Add adds an application.
func (a *ApplicationListBuilder) Add(port int, applicationName string) {
	a.add(port, applicationName)
}

// Read application names and ports from a config file into this builder.
func (a *ApplicationListBuilder) ReadConfig(r io.Reader) error {
	return a.readConfig(r)
}

// Build builds the ApplicationList instance and destroys this builder.
func (a *ApplicationListBuilder) Build() *ApplicationList {
	return a.build()
}
