// Package application manages individual applications for scotty.
package application

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/hostid"
	"github.com/Symantec/scotty/namesandports"
	"time"
)

const (
	// The name of the health agent application
	HealthAgentName = "health agent"

	// The port of the health agent application
	HealthAgentPort = 6910
)

// Statistics for reading metrics for a particular application
type EndpointStats struct {
	// Time metrics were last read. Zero means never read.
	LastReadTime time.Time

	// Time spent during last poll.
	PollTime time.Duration

	LastError     error
	LastErrorTime time.Time

	// Number of metrics read initially
	InitialMetricCount uint64

	// True if this application cannot be reached.
	Down bool

	// ChangedMetricsSum and ChangedMetricsCount are used to keep track of the
	// average number of metrics that change.
	ChangedMetricsSum   uint64
	ChangedMetricsCount uint64

	// Status: e.g Synced, Polling, Connecting etc.
	Status scotty.Status
}

// Staleness is a convenience routine that returns now minus the last read
// time. If metrics were never read, Staleness returns 0.
func (e *EndpointStats) Staleness() time.Duration {
	if e.LastReadTime.IsZero() {
		return 0
	}
	return time.Now().Sub(e.LastReadTime)
}

// AverageChangedMetrics is a convenience routine that returns the average
// number of changing metrics.
func (e *EndpointStats) AverageChangedMetrics() float64 {
	if e.ChangedMetricsCount == 0 {
		return 0.0
	}
	return float64(e.ChangedMetricsSum) / float64(e.ChangedMetricsCount)
}

// LastErrorTimeStr is a convenience routine that reports last error time
// as a string formatted as 2006-01-02T15:04:05.
func (e *EndpointStats) LastErrorTimeStr() string {
	return e.LastErrorTime.Format("2006-01-02T15:04:05")
}

// Application represents an application
type Application struct {

	// Used to collect metrics for this application. Each application has an
	// unique value for this field that never changes.
	EP *scotty.Endpoint

	// The port that this application runs on. Unlike the EP field, the value
	// of this field can change.
	Port uint

	// TODO: comment
	IsTLS bool

	// The statistics for collecting metrics from this application. This
	// field changes.
	EndpointStats

	// True if this application is active. An application is active if
	// health agent reports it. The value of this field can change.
	Active bool
}

// Group contains applications running on a particular machine
type Group struct {
	host              *hostid.HostID
	apps              map[string]*applicationDataType
	countToInactivate int
}

// NewGroup returns a new group instance. host identifies the machine;
// countToInactive is the number of times an application must be reported
// missing before it is marked inactive. Each new group always contains
// the health agent running on port 6910. In addition to the newly created
// instance, NewGroup returns the scotty.Endpoint for the health agent.
func NewGroup(host *hostid.HostID, countToInactivate int) (
	*Group, *scotty.Endpoint) {
	return newGroup(host, countToInactivate)
}

// SetApplications tells this instance the names and ports of running
// applications. If a previously reported application isn't reported
// countToInactive times (see NewGroup) then SetApplications marks that
// application inactive. If an inactive application is listed in namesAndPorts,
// SetApplications marks it as active. namesandports need not include the
// health agent running on port 6910. That application is always active.
//
// SetApplications returns the scotty.Endpoint for newly reported applications,
// previously inactive applications marked active, and previously active
// applications marked inactive.
func (g *Group) SetApplications(namesAndPorts namesandports.NamesAndPorts) (
	newApps, active, inactive []*scotty.Endpoint) {
	return g.setApplications(namesAndPorts)
}

// Applications returns all the applications (both active and inactive) this
// instance has. Applications returns defensive copies of the Application
// objects.
func (g *Group) Applications() []*Application {
	return g.applications()
}

// ByName returns an application by its name or nil if no such application
// exists. ByName returns a defensive copy of the Application object.
func (g *Group) ByName(name string) *Application {
	return g.byName(name)
}

// Modify modifies statistics for an application. name is the name of the
// application; mod modifies the statistics. Modify calls mod once to
// modify statistics in place.
func (g *Group) Modify(name string, mod func(*EndpointStats)) {
	g.modify(name, mod)
}
