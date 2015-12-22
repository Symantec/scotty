package scotty

import (
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"net/rpc"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var (
	concurrentConnects = allowedConnectionCount()
	connectSemaphore   = make(chan bool, concurrentConnects)
	concurrentPolls    = runtime.NumCPU() * 2
	pollSemaphore      = make(chan bool, concurrentPolls)
)

func allowedConnectionCount() int {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		panic(err)
	}
	if rlim.Cur <= 50 {
		return 1
	}
	return int(rlim.Cur - 50)
}

func setConcurrentConnects(x int) {
	close(connectSemaphore)
	concurrentConnects = x
	connectSemaphore = make(chan bool, concurrentConnects)
}

func setConcurrentPolls(x int) {
	close(pollSemaphore)
	concurrentPolls = x
	pollSemaphore = make(chan bool, concurrentPolls)
}

func waitingToConnect(sweepStartTime time.Time) *State {
	return &State{
		timestamp:      sweepStartTime,
		sweepStartTime: sweepStartTime,
		status:         WaitingToConnect}
}

func (s *State) goToConnecting(t time.Time) *State {
	result := *s
	dur := t.Sub(result.timestamp)
	result.timestamp = t
	result.waitToConnectDuration = dur
	result.status = Connecting
	return &result
}

func (s *State) finishedConnecting(t time.Time, next Status) *State {
	result := *s
	dur := t.Sub(result.timestamp)
	result.timestamp = t
	result.connectDuration = dur
	result.status = next
	return &result
}

func (s *State) goToWaitingToPoll(t time.Time) *State {
	return s.finishedConnecting(t, WaitingToPoll)
}

func (s *State) goToFailedToConnect(t time.Time) *State {
	return s.finishedConnecting(t, FailedToConnect)
}

func (s *State) goToPolling(t time.Time) *State {
	result := *s
	dur := t.Sub(result.timestamp)
	result.timestamp = t
	result.waitToPollDuration = dur
	result.status = Polling
	return &result
}

func (s *State) finishedPolling(t time.Time, next Status) *State {
	result := *s
	dur := t.Sub(result.timestamp)
	result.timestamp = t
	result.pollDuration = dur
	result.status = next
	return &result
}

func (s *State) goToSynced(t time.Time) *State {
	return s.finishedPolling(t, Synced)
}

func (s *State) goToFailedToPoll(t time.Time) *State {
	return s.finishedPolling(t, FailedToPoll)
}

func newMachine(
	host string, port int, logger Logger) *Machine {
	return &Machine{
		host:           host,
		port:           port,
		logger:         logger,
		onePollAtATime: make(chan bool, 1),
	}
}

func (m *Machine) poll(sweepStartTime time.Time) {
	select {
	case m.onePollAtATime <- true:
		state := waitingToConnect(sweepStartTime)
		m.logState(state)
		connectSemaphore <- true
		go func(state *State) {
			defer func() {
				<-m.onePollAtATime
			}()
			defer func() {
				<-connectSemaphore
			}()
			state = state.goToConnecting(time.Now())
			m.logState(state)
			conn, err := m.connect()
			if err != nil {
				state = state.goToFailedToConnect(time.Now())
				m.logError(err, state)
				return
			}
			defer conn.Close()
			state = state.goToWaitingToPoll(time.Now())
			m.logState(state)
			pollSemaphore <- true
			defer func() {
				<-pollSemaphore
			}()
			state = state.goToPolling(time.Now())
			m.logState(state)
			metrics, err := m._poll(conn)
			if err != nil {
				state = state.goToFailedToPoll(time.Now())
				m.logError(err, state)
				return
			}
			state = state.goToSynced(time.Now())
			m.logMetrics(metrics, state)
		}(state)
	default:
		return
	}
}

func (m *Machine) connect() (conn *rpc.Client, err error) {
	hostname := strings.SplitN(m.host, "*", 2)[0]
	return rpc.DialHTTP(
		"tcp",
		fmt.Sprintf("%s:%d", hostname, m.port))
}

func (m *Machine) _poll(conn *rpc.Client) (
	metrics messages.MetricList, err error) {
	err = conn.Call("MetricsServer.ListMetrics", "", &metrics)
	return
}

func (m *Machine) logState(state *State) {
	oldState := m._logState(state)
	if m.logger != nil {
		m.logger.LogStateChange(m, oldState, state)
	}
}

func (m *Machine) logMetrics(metrics messages.MetricList, state *State) {
	oldState, hadError := m._setError(state, false)
	if m.logger != nil {
		m.logger.LogStateChange(m, oldState, state)
		if hadError {
			m.logger.LogError(m, nil, nil)
		}
		m.logger.LogResponse(m, metrics, state)
	}
}

func (m *Machine) logError(err error, state *State) {
	oldState, _ := m._setError(state, true)
	if m.logger != nil {
		m.logger.LogStateChange(m, oldState, state)
		m.logger.LogError(m, err, state)
	}
}

func (m *Machine) _logState(state *State) *State {
	m.lock.Lock()
	defer m.lock.Unlock()
	result := m.state
	m.state = state
	return result
}

func (m *Machine) _setError(state *State, hasError bool) (
	oldState *State, hadError bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	oldState = m.state
	hadError = m.errored
	m.state = state
	m.errored = hasError
	return
}
