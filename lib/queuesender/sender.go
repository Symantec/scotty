package queuesender

import (
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"log"
)

func _new(endpoint string, size int, name string, logger *log.Logger) (
	*Sender, error) {
	if size < 1 {
		panic("Size must be at least 1")
	}
	conn, err := newConn(endpoint)
	if err != nil {
		return nil, err
	}
	result := &Sender{
		queue:        newQueue(size),
		newConnCh:    make(chan *connType),
		newConnReqCh: make(chan bool),
	}
	if logger != nil {
		result.logger = &loggerType{name: name, logger: logger}
	}
	go result.sendLoop(conn)
	go result.receiveLoop(conn)
	return result, nil
}

// This loop sends requests in the queue. To coordinate connection refreshes
// between the send loop and receive loop goroutines, only this send loop
// closes stale connections, and only the receiving loop refreshes connections.
// Whenever this send loop closes a stale connection, the receiving loop has
// moved on to using a new connection.
func (s *Sender) sendLoop(conn *connType) {
	connId := 0
	for {
		// See if the receiving loop has a new connection for us.
		// If not, just keep using the same connection
		select {
		case newConn := <-s.newConnCh:
			// We know its safe to close the old connection because
			// the receiving loop is already using the connection it
			// just gave us.
			conn.Close()
			conn = newConn
			// For a given connection, both the send and receiving loop
			// use the same value for connection ID.
			connId++
		default:
		}
		req, ok := s.queue.NextNotSent(connId)
		if !ok {
			// If we get here, the receiver loop has already created
			// a new connection, and the queue is already tracking it.
			// Go to the beginning to get this new connection.
			continue
		}
		err := conn.Send(req)
		// On error we have to refresh the connection
		if err != nil {
			// Tell receiving loop to build a new connection
			s.newConnReqCh <- true
			// Block waiting for that new connection since the connection we
			// have is no good.
			newConn := <-s.newConnCh
			conn.Close()
			conn = newConn
			connId++
		}
	}
}

// This loop receives the responses from the requests.
// Responses received are expected to come in the same order as requests are
// sent.
func (s *Sender) receiveLoop(conn *connType) {
	connId := 0
	for {
		// First see if sending loop wants a new connection
		select {
		case <-s.newConnReqCh:
			// Create the new connection. Note the old connection is still
			// opened. The sending loop will close it when it gets this new
			// connection.
			conn = conn.Refresh(s.logger)
			connId++
			// Tell the queue about the new connection ID
			s.queue.ResetNextNotSent(connId)
			// Give sending loop this new connection
			s.newConnCh <- conn
		default:
		}
		ok, err := conn.Read()
		// Oops we have to refresh the connection
		if !ok {
			conn = conn.Refresh(s.logger)
			connId++
			s.queue.ResetNextNotSent(connId)
			s.newConnCh <- conn
			continue
		}
		s.logError(err)
		// A real error in response
		if err != nil {
			s.logger.Log(err)
			s.queue.MoveToNotSent()
		} else {
			s.queue.DiscardNextSent()
		}
	}
}

func (s *Sender) logError(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err == nil {
		s.successfulWrites++
		return
	}
	s.lastResponseError = err.Error()
	s.responseErrorCount++
}

func (s *Sender) LastError() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.lastResponseError
}

func (s *Sender) ErrorCount() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.responseErrorCount
}

func (s *Sender) SuccessCount() uint64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.successfulWrites
}

func (s *Sender) register(prefix string) error {
	prefix = "/asyncWriters/" + prefix
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "queueLength"),
		s.queue.Len,
		units.None,
		"count of items in queue"); err != nil {
		return err
	}
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "queueCapacity"),
		s.queue.Cap,
		units.None,
		"capacity of queue"); err != nil {
		return err
	}
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "needsAck"),
		s.queue.Sent,
		units.None,
		"items in queue needing acknowledgement"); err != nil {
		return err
	}
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "lastError"),
		s.LastError,
		units.None,
		"last error encountered"); err != nil {
		return err
	}
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "errorCount"),
		s.ErrorCount,
		units.None,
		"last error encountered"); err != nil {
		return err
	}
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "successCount"),
		s.SuccessCount,
		units.None,
		"successfulWrites"); err != nil {
		return err
	}
	return nil
}
