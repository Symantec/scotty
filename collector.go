package scotty

import (
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
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

func newEndpoint(
	host string, port int) *Endpoint {
	return &Endpoint{
		host:           host,
		port:           port,
		onePollAtATime: make(chan bool, 1),
	}
}

func (e *Endpoint) poll(sweepStartTime time.Time, logger Logger) {
	select {
	case e.onePollAtATime <- true:
		state := waitingToConnect(sweepStartTime)
		e.logState(state, logger)
		connectSemaphore <- true
		go func(state *State) {
			defer func() {
				<-e.onePollAtATime
			}()
			defer func() {
				<-connectSemaphore
			}()
			state = state.goToConnecting(time.Now())
			e.logState(state, logger)
			conn, err := e.connect()
			if err != nil {
				state = state.goToFailedToConnect(time.Now())
				e.logError(err, state, logger)
				return
			}
			defer conn.Close()
			state = state.goToWaitingToPoll(time.Now())
			e.logState(state, logger)
			pollSemaphore <- true
			defer func() {
				<-pollSemaphore
			}()
			state = state.goToPolling(time.Now())
			e.logState(state, logger)
			metrics, err := e._poll(conn)
			if err != nil {
				state = state.goToFailedToPoll(time.Now())
				e.logError(err, state, logger)
				return
			}
			state = state.goToSynced(time.Now())
			e.logMetrics(metrics, state, logger)
		}(state)
	default:
		return
	}
}

func (e *Endpoint) connect() (conn *rpc.Client, err error) {
	hostname := strings.SplitN(e.host, "*", 2)[0]
	return rpc.DialHTTP(
		"tcp",
		fmt.Sprintf("%s:%d", hostname, e.port))
}

// TODO: Delete once all apps link with new tricorder
func fixupKind(k types.Type) types.Type {
	switch k {
	case "int":
		return types.Int64
	case "uint":
		return types.Uint64
	case "float":
		return types.Float64
	default:
		return k
	}
}

func (e *Endpoint) _poll(conn *rpc.Client) (
	metrics messages.MetricList, err error) {
	err = conn.Call("MetricsServer.ListMetrics", "", &metrics)
	for i := range metrics {
		metrics[i].Kind = fixupKind(metrics[i].Kind)
	}
	return
}

func (e *Endpoint) logState(state *State, logger Logger) {
	oldState := e._logState(state)
	if logger != nil {
		logger.LogStateChange(e, oldState, state)
	}
}

func (e *Endpoint) logMetrics(metrics messages.MetricList, state *State, logger Logger) {
	oldState, hadError := e._setError(state, false)
	if logger != nil {
		logger.LogStateChange(e, oldState, state)
		if hadError {
			logger.LogError(e, nil, nil)
		}
		logger.LogResponse(e, metrics, state)
	}
}

func (e *Endpoint) logError(err error, state *State, logger Logger) {
	oldState, _ := e._setError(state, true)
	if logger != nil {
		logger.LogStateChange(e, oldState, state)
		logger.LogError(e, err, state)
	}
}

func (e *Endpoint) _logState(state *State) *State {
	result := e.state
	e.state = state
	return result
}

func (e *Endpoint) _setError(state *State, hasError bool) (
	oldState *State, hadError bool) {
	oldState = e.state
	hadError = e.errored
	e.state = state
	e.errored = hasError
	return
}
