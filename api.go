// Package scotty collects machine health metrics asynchronously.
package scotty

import (
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"sync"
	"time"
)

// Status represents the status of metric collection for a particular
// machine. Failure statuses are negative, success statuses are
// positive.
type Status int

const (
	// failed to connect to machine
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

// State represents the state of collecting metrics from a machine.
// State instances are immutable.
type State struct {
	timestamp             time.Time
	sweepStartTime        time.Time
	waitToConnectDuration time.Duration
	connectDuration       time.Duration
	waitToPollDuration    time.Duration
	pollDuration          time.Duration
	status                Status
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
// Machine instances call Logger methods immediately after updating themselves.
// Logger instances must be safe to use among multiple goroutines.
type Logger interface {
	// Called when new metrics come in from a given machine
	LogResponse(m *Machine, response messages.MetricList, state *State)
	// Called when error happens collecting metrics from a given
	// machine.
	// Also called when an error clears. In such a case both err and
	// state are nil.
	LogError(m *Machine, err error, state *State)
	// Called when collection status changes on a given machine
	LogStateChange(m *Machine, oldState, newState *State)
}

// Machine represents a particular machine with health metrics.
// Machine instances are safe to use with multiple goroutines.
type Machine struct {
	host           string
	port           int
	logger         Logger
	onePollAtATime chan bool
	lock           sync.Mutex
	state          *State
	errored        bool
}

// NewMachine creates a new machine.
// logger logs collection events for this machine
func NewMachine(
	hostname string, port int, logger Logger) *Machine {
	return newMachine(hostname, port, logger)
}

// HostName returns the host name of the machine.
func (m *Machine) HostName() string {
	return m.host
}

// Port returns the port to collect metrics.
func (m *Machine) Port() int {
	return m.port
}

// Poll polls for metrics for this machine asynchronously.
// However, Poll may block while it waits to begin connecting if too many
// requests for metrics are in progress. Poll returns immediately if this
// instance is already in the process of collecting metrics.
// sweepStartTime is the start time of the current collection of metrics.
func (m *Machine) Poll(sweepStartTime time.Time) {
	m.poll(sweepStartTime)
}

// SetConcurrentPolls sets the maximum number of concurrent polls.
// Call SetConcurrentPolls at the beginning of the main() function before
// calling Machine.Poll
func SetConcurrentPolls(x int) {
	setConcurrentPolls(x)
}

// ConcurrentPolls returns the maximum number of concurrent polls.
// Default is 2 * number of CPUs
func ConcurrentPolls() int {
	return concurrentPolls
}

// SetConcurrentConnects sets the maximum number of concurrent connects.
// Call SetConcurrentConnects at the beginning of the main() function before
// calling Machine.Poll
func SetConcurrentConnects(x int) {
	setConcurrentConnects(x)
}

// ConcurrentConnects returns the maximum number of concurrent connects.
func ConcurrentConnects() int {
	return concurrentConnects
}
