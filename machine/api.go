// Package machine keeps track of machines and applications for scotty.
package machine

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/awsinfo"
	"github.com/Symantec/scotty/namesandports"
	"github.com/Symantec/scotty/store"
	"sort"
	"sync"
	"time"
)

// EndpointObservation represents an observation of applications and ports
// running on a certain machine.
type EndpointObservation struct {
	// The observation number that increases by 1 each time
	SeqNo uint64

	// The name of each appliation and the port it is running on.
	Endpoints namesandports.NamesAndPorts
}

// EndpointObservations stores endpoint observations for all machines.
// EndpointObservations is safe to use with multiple goroutines.
type EndpointObservations struct {
	mu   sync.Mutex
	data map[string]EndpointObservation
}

// NewEndpointObservations returns an empty EndpointObservations
func NewEndpointObservations() *EndpointObservations {
	return &EndpointObservations{data: make(map[string]EndpointObservation)}
}

// Save saves namesAndPorts for hostName and increments the sequence number
// by 1.
func (e *EndpointObservations) Save(
	hostName string, namesAndPorts namesandports.NamesAndPorts) {
	e.save(hostName, namesAndPorts)
}

// GetAll returns a snapshot of all the endpoint observations.
func (e *EndpointObservations) GetAll() map[string]EndpointObservation {
	return e.getAll()
}

// MaybeAddApp atomically adds an application for hostName if its port
// isn't there already without incrementing the sequence number. If nothing
// has been previously stored for hostName, MaybeAddApp adds appName and sets
// the sequence number to 1. IsTLS for added app is false for now.
func (e *EndpointObservations) MaybeAddApp(hostName, appName string, port uint) {
	e.maybeAddApp(hostName, appName, port)
}

// Machine represents a single machine
type Machine struct {

	// The host name of the machine
	Host string

	// The region of the machine
	Region string

	// The IP address of the machine
	IpAddress string

	// Whether or not machine is active
	Active bool

	// Aws information for machine. nil if no aws information available.
	Aws *awsinfo.AwsInfo
}

// AccountId is a convenience routine that returns the aws account id.
// If no aws information, returns the empty string.
func (m *Machine) AccountId() string {
	if m.Aws != nil {
		return m.Aws.AccountId
	}
	return ""
}

// AccountId is a convenience routine that returns the aws instance id.
// If no aws information, returns the empty string.
func (m *Machine) InstanceId() string {
	if m.Aws != nil {
		return m.Aws.InstanceId
	}
	return ""
}

// CloudHealth is a convenience routine that returns true if scotty should
// write cloud health data for machine.
func (m *Machine) CloudHealth() bool {
	if m.Aws != nil {
		return m.Aws.CloudHealth
	}
	return false
}

// CloudWatchStr is a convenience routine that returns how often scotty
// should write to cloud watch for the machine as a string (e.g "5m").
// Returns the empty string if no aws information is available or if no
// data should be written to cloud watch.
func (m *Machine) CloudWatchStr() string {
	if m.Aws != nil && m.Aws.CloudWatch != 0 {
		if m.Aws.CloudWatchMemoryOnly {
			return m.Aws.CloudWatch.String() + "M"
		}
		return m.Aws.CloudWatch.String()
	}
	return ""
}

// Endpoint represents an endpoint, a single application running on a single
// machine.
type Endpoint struct {
	M   *Machine
	App *application.Application
}

// Active is a convenience routine that returns true iff this endpoint is
// active. For an endpoint to be active, both the application and the machine
// it is running on must be active.
func (e *Endpoint) Active() bool {
	return e.M.Active && e.App.Active
}

// EndpointStore stores all the endpoints for scotty along with a store
// that stores the metrics for each endpoint.
type EndpointStore struct {
	config            awsinfo.Config
	countToInactivate int

	// grab this lock when changing machines or applications.
	// grab before grabbing mu
	statusChangeLock sync.Mutex
	mu               sync.Mutex
	astore           *store.Store
	byHost           map[string]*machineDataType
}

// NewEndpointStore returns a new EndpointStore.
// astore is a prototype of the store object for storing metrics. The caller
// should not hold onto the astore reference after passing it to
// NewEndpointStore. config contains information for understanding AWS
// information. countToInactive is how many times an application must be
// reported as missing before it is marked inactive.
func NewEndpointStore(
	astore *store.Store,
	config awsinfo.Config,
	countToInactivate int) *EndpointStore {
	return &EndpointStore{
		config:            config,
		astore:            astore,
		countToInactivate: countToInactivate,
		byHost:            make(map[string]*machineDataType),
	}
}

// UpdateState updates the state of the given endpoint.
func (e *EndpointStore) UpdateState(
	ep *scotty.Endpoint, newState *scotty.State) {
	e.updateState(ep, newState)
}

// ReportError reports an error for given endpoint.
func (e *EndpointStore) ReportError(
	ep *scotty.Endpoint, err error, ts time.Time) {
	e.reportError(ep, err, ts)
}

// LogChangedMetricCount logs the number of changed metrics for given
/// endpoint.
func (e *EndpointStore) LogChangedMetricCount(
	ep *scotty.Endpoint, metricCount uint) {
	e.logChangedMetricCount(ep, metricCount)
}

// UpdateMachines updates the available machines.
func (e *EndpointStore) UpdateMachines(
	timestamp float64,
	activeHosts []mdb.Machine) {
	e.updateMachines(timestamp, activeHosts)
}

// AllWithStore returns all endpoints along with the metric store
func (e *EndpointStore) AllWithStore() (
	result []*Endpoint, astore *store.Store) {
	return e.allWithStore()
}

// Store returns the current metric store
func (e *EndpointStore) Store() *store.Store {
	return e.store()
}

// AllActiveWithStore returns all active endpoints along with the metric store
func (e *EndpointStore) AllActiveWithStore() ([]*Endpoint, *store.Store) {
	return e.allActiveWithStore()
}

// ByHostAndName returns the endpoint for given host name and application name
// along with the current metric store. If the host name application name
// combination doesn't exist, ByHostAndName returns nil for the endpoint.
func (e *EndpointStore) ByHostAndName(host, name string) (
	*Endpoint, *store.Store) {
	return e.byHostAndName(host, name)
}

// UpdateEndpints tells this instance of all the applications running on
// al the hosts. endpoints are all the applications running keyed by hostname.
// If the sequence number for a given host hasn't changed since the last call
// to UpdateEndpoints, then UpdateEndpoints() becomes a no-op for that hostname.
func (e *EndpointStore) UpdateEndpoints(
	timestamp float64, endpoints map[string]EndpointObservation) {
	e.updateEndpoints(timestamp, endpoints)
}

// ByHostAndName sorts list by the hostname and then by application name
// in ascending order.
func ByHostAndName(list []*Endpoint) {
	sort.Sort(byHostAndName(list))
}
