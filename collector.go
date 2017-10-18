package scotty

import (
	"github.com/Symantec/scotty/hostid"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"runtime"
	"syscall"
	"time"
)

const (
	kPreferenceMemory = 100
)

var (
	concurrentConnects = allowedConnectionCount()
	connectSemaphore   = make(chan bool, concurrentConnects)
	concurrentPolls    = allowedPollCount()
	pollSemaphore      = make(chan bool, concurrentPolls)
)

func allowedPollCount() uint {
	numCpus := runtime.NumCPU()
	if numCpus < 1 {
		numCpus = 1
	}
	return uint(numCpus * 2)
}

func allowedConnectionCount() uint {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		panic(err)
	}
	if rlim.Cur <= 50 {
		return 1
	}
	return uint(rlim.Cur - 50)
}

func setConcurrentConnects(x uint) {
	close(connectSemaphore)
	concurrentConnects = x
	connectSemaphore = make(chan bool, concurrentConnects)
}

func setConcurrentPolls(x uint) {
	if pollSemaphore != nil {
		close(pollSemaphore)
	}
	concurrentPolls = x
	if x > 0 {
		pollSemaphore = make(chan bool, concurrentPolls)
	} else {
		pollSemaphore = nil
	}
}

func waitingToConnect(sweepStartTime time.Time) *State {
	return &State{
		timestamp:      sweepStartTime,
		sweepStartTime: sweepStartTime,
		status:         WaitingToConnect}
}

func (s *State) goToConnecting(t time.Time, connectorName string) *State {
	result := *s
	dur := t.Sub(result.timestamp)
	result.timestamp = t
	result.waitToConnectDuration = dur
	result.connectorName = connectorName
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
	hostId *hostid.HostID,
	appName string,
	connector sources.Connector) *Endpoint {
	return &Endpoint{
		hostId:         hostId,
		name:           appName,
		conn:           sources.AsResourceConnector(connector),
		onePollAtATime: make(chan bool, 1),
	}
}

func (e *Endpoint) pollWithResource(
	state *State,
	logger Logger,
	resource sources.Resource) bool {
	state = state.goToConnecting(time.Now(), e.conn.Name())
	e.logState(state, logger)
	conn, err := e.conn.ResourceConnect(resource)
	if err != nil {
		state = state.goToFailedToConnect(time.Now())
		e.logError(err, state, logger)
		return false
	}
	defer conn.Close()
	state = state.goToWaitingToPoll(time.Now())
	e.logState(state, logger)
	if pollSemaphore != nil {
		pollSemaphore <- true
		defer func() {
			<-pollSemaphore
		}()
	}
	state = state.goToPolling(time.Now())
	e.logState(state, logger)
	metrics, err := conn.Poll()
	if err != nil {
		state = state.goToFailedToPoll(time.Now())
		e.logError(err, state, logger)
		return false
	}
	e.logMetrics(metrics, state, logger)
	return true
}

func (e *Endpoint) getResource(isTls bool, port uint) sources.Resource {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.resource == nil || port != e.resourcePort || isTls != e.isTls {
		e.resource = e.conn.NewResource(
			e.hostId.ConnectID(), port, sources.Config{IsTls: isTls})
		e.resourcePort = port
		e.isTls = isTls
	}
	return e.resource
}

func (e *Endpoint) poll(sweepStartTime time.Time, isTls bool, port uint, logger Logger) {
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
			e.pollWithResource(state, logger, e.getResource(isTls, port))
		}(state)
	default:
		return
	}
}

func (e *Endpoint) logState(state *State, logger Logger) {
	oldState := e._logState(state)
	if logger != nil {
		logger.LogStateChange(e, oldState, state)
	}
}

func (e *Endpoint) logMetrics(metrics metrics.List, state *State, logger Logger) {
	syncTime := time.Now()
	if logger != nil {
		err := logger.LogResponse(e, metrics, syncTime)
		if err != nil {
			newState := state.goToFailedToPoll(syncTime)
			e.logError(err, newState, logger)
			return
		}
	}
	newState := state.goToSynced(syncTime)
	e.logNoError(newState, logger)
}

func (e *Endpoint) logNoError(state *State, logger Logger) {
	oldState, hadError := e._setError(state, false)
	if logger != nil {
		logger.LogStateChange(e, oldState, state)
		if hadError {
			logger.LogError(e, nil, state)
		}
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
