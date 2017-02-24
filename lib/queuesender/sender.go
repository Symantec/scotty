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
		queue:     newQueue(size),
		newConnCh: make(chan *connType),
	}
	if logger != nil {
		result.logger = &loggerType{name: name, logger: logger}
	}
	forSender := &interruptType{}
	forReceiver := &interruptType{}
	go result.sendLoop(conn, forSender, forReceiver)
	go result.receiveLoop(conn, forReceiver, forSender)
	return result, nil
}

// This loop sends requests in the queue. To coordinate connection refreshes
// between the send loop and receive loop goroutines, only this send loop
// closes stale connections, and only the receiving loop refreshes connections.
// Whenever this send loop closes a stale connection, the receiving loop has
// moved on to using a new connection.
func (s *Sender) sendLoop(
	conn *connType, inter, toReceiver *interruptType) {
	for {
		req, ok := s.queue.NextNotSent(inter)
		if !ok {
			// If we get here, the receiver loop has interrupted because
			// it created a new connection. Get this connection.
			newConn := <-s.newConnCh
			conn.Close()
			conn = newConn
			s.queue.ResetNextNotSent()
			continue
		}
		err := conn.Send(req, inter)
		if err == kErrInterrupted {
			// If we get here, receiver loop has created a new connection.
			// Get this connection.
			newConn := <-s.newConnCh
			conn.Close()
			conn = newConn
			s.queue.ResetNextNotSent()
			continue
		}
		// On error ask receiver to refresh connection. We will keep using
		// the current connection to send stuff until the receiver
		// interrupts us to get the new connection, but that is ok.
		if err != nil {
			toReceiver.Interrupt()
		}
	}
}

// This loop receives the responses from the requests.
// Responses received are expected to come in the same order as requests are
// sent.
func (s *Sender) receiveLoop(conn *connType, inter, toSender *interruptType) {
	for {
		ok, err := conn.Read(inter)
		// Oops we have to refresh the connection
		if !ok {
			conn = conn.Refresh(s.logger)
			// interrupt sender so that it can grab new connection.
			toSender.Interrupt()
			// If sender waiting on queue we have to nudge the queue in
			// addition to interrupting
			s.queue.Nudge()
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
