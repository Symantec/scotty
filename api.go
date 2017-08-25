// Package scotty collects endpoint health metrics asynchronously.
package scotty

import (
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"sync"
	"time"
)

// Status represents the status of metric collection for a particular
// endpoint. Failure statuses are negative, success statuses are
// positive.
type Status int

const (
	// failed to connect to endpoint
	FailedToConnect Status = -1 - iota
	// connected, but failed to collect metrics
	FailedToPoll
)

const (
	Unknown Status = iota
	WaitingToConnect
	Connecting
	WaitingToPoll
	Polling
	Synced
)

func (s Status) String() string {
	switch s {
	case Unknown:
		return ""
	case WaitingToConnect:
		return "waiting to connect"
	case Connecting:
		return "connecting"
	case WaitingToPoll:
		return "waiting to poll"
	case Polling:
		return "polling"
	case Synced:
		return "synced"
	case FailedToConnect:
		return "failed to connect"
	case FailedToPoll:
		return "failed to poll"
	default:
		return ""
	}
}

// State represents the state of collecting metrics from a endpoint.
// State instances are immutable.
type State struct {
	timestamp             time.Time
	sweepStartTime        time.Time
	waitToConnectDuration time.Duration
	connectDuration       time.Duration
	waitToPollDuration    time.Duration
	pollDuration          time.Duration
	connectorName         string
	status                Status
}

// ConnectorName returns the name of the connector
func (s *State) ConnectorName() string {
	return s.connectorName
}

// Timestamp returns the timestamp of the last state change.
func (s *State) Timestamp() time.Time {
	return s.timestamp
}

// SweepStartTime returns the start time of the sweep.
func (s *State) SweepStartTime() time.Time {
	return s.sweepStartTime
}

// TimeSpentWaitingToConnect returns the time elapsed from the start of
// the sweep until establishing a connection first commences.
func (s *State) TimeSpentWaitingToConnect() time.Duration {
	return s.waitToConnectDuration
}

// TimeSpentConnecting returns the time spent establishing a connection.
func (s *State) TimeSpentConnecting() time.Duration {
	return s.connectDuration
}

// TimeSpentWaitingToPoll returns time elpased from when the connection
// was established to when downloading metrics first commences.
func (s *State) TimeSpentWaitingToPoll() time.Duration {
	return s.waitToPollDuration
}

// TimeSpentPolling returns the time spent downloading the metrics.
func (s *State) TimeSpentPolling() time.Duration {
	return s.pollDuration
}

// Status returns the status of the collection.
func (s *State) Status() Status {
	return s.status
}

// Logger is the interface for instances that log metric collection events.
// Endpoint instances call Logger methods immediately after updating themselves.
// Logger instances must be safe to use among multiple goroutines.
type Logger interface {
	// Called when new metrics come in from a given endpoint. If
	// implementation returns a non-nil error, state goes to
	// FailedtoPoll. Otherwise, state goes to Synced.
	LogResponse(
		e *Endpoint, response metrics.List, timestamp time.Time) error
	// Called when error happens collecting metrics from a given
	// endpoint.
	// Also called when an error clears. In such a case both err and
	// state are nil.
	LogError(e *Endpoint, err error, state *State)
	// Called when collection status changes on a given endpoint
	LogStateChange(e *Endpoint, oldState, newState *State)
}

// Endpoint represents a particular endpoint with health metrics.
// Endpoint instances are safe to use with multiple goroutines.
type Endpoint struct {
	// These fields are immutable
	host           string
	name           string
	conn           sources.ResourceConnector
	onePollAtATime chan bool
	// These fields read and changed only by goroutine that has the
	// onePollAtATime semaphore
	state        *State
	errored      bool
	lock         sync.Mutex
	resourcePort uint
	resource     sources.Resource
}

// NewEndpointWithConnector creates a new endpoint for given host, port
// and connector.
func NewEndpointWithConnector(
	hostname, appName string, connector sources.Connector) *Endpoint {
	return newEndpoint(hostname, appName, connector)
}

// HostName returns the host name of the endpoint.
func (e *Endpoint) HostName() string {
	return e.host
}

// AppName returns the app name of the endpoint.
func (e *Endpoint) AppName() string {
	return e.name
}

// ConnectorName returns the name of the underlying connector in this endpoint.
func (e *Endpoint) ConnectorName() string {
	return e.conn.Name()
}

// Poll polls for metrics for this endpoint asynchronously.
// However, Poll may block while it waits to begin connecting if too many
// requests for metrics are in progress. Poll returns immediately if this
// instance is already in the process of collecting metrics.
// sweepStartTime is the start time of the current collection of metrics.
// port is the port to use to connect.
// logger logs collection events for this polling
func (e *Endpoint) Poll(sweepStartTime time.Time, port uint, logger Logger) {
	e.poll(sweepStartTime, port, logger)
}

// SetConcurrentPolls sets the maximum number of concurrent polls.
// Zero means no limit.
// Call SetConcurrentPolls at the beginning of the main() function before
// calling Endpoint.Poll
func SetConcurrentPolls(x uint) {
	setConcurrentPolls(x)
}

// ConcurrentPolls returns the maximum number of concurrent polls.
// Default is 2 * number of CPUs. A return of 0 means no limit.
func ConcurrentPolls() uint {
	return concurrentPolls
}

// SetConcurrentConnects sets the maximum number of concurrent connects.
// Call SetConcurrentConnects at the beginning of the main() function before
// calling Endpoint.Poll
func SetConcurrentConnects(x uint) {
	setConcurrentConnects(x)
}

// ConcurrentConnects returns the maximum number of concurrent connects.
func ConcurrentConnects() uint {
	return concurrentConnects
}
