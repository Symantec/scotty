package queuesender

import (
	"fmt"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

func _new(endpoint string, size int, name string, logger log.Logger) (
	*Sender, error) {
	if size < 1 {
		panic("Size must be at least 1")
	}
	connManager, err := newConnManager(endpoint)
	if err != nil {
		return nil, err
	}
	result := &Sender{
		queue:       newQueue(size),
		connManager: connManager,
	}
	if logger != nil {
		result.logger = &loggerType{name: name, logger: logger}
	}
	go result.sendLoop()
	go result.receiveLoop()
	return result, nil
}

func (s *Sender) getConn() *connType {
	conn, err := s.connManager.Get()
	duration := time.Second
	for err != nil {
		s.logger.Log(err)
		time.Sleep(duration)
		duration *= 2
		conn, err = s.connManager.Get()
	}
	return conn
}

// This loop sends requests in the queue.
func (s *Sender) sendLoop() {
	var lastConn *connType
	for {
		req := s.queue.NextNotSent()
		conn := s.getConn()
		if conn != lastConn {
			lastConn = conn
			s.queue.ResetNextNotSent()
			req = s.queue.NextNotSent()
		}
		if err := conn.Send(req); err != nil {
			conn.MarkBad()
		}
		conn.Put()
	}
}

// This loop receives the responses from the requests.
// Responses received are expected to come in the same order as requests are
// sent.
func (s *Sender) receiveLoop() {
	for {
		conn := s.getConn()
		ok, err := conn.Read()
		// Oops we have to refresh the connection
		if !ok {
			conn.MarkBad()
		} else {
			s.logError(err)
			// A real error in response
			if err != nil {
				s.logger.Log(err)
				s.queue.MoveToNotSent()
			} else {
				s.queue.DiscardNextSent()
			}
		}
		conn.Put()
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
	if err := tricorder.RegisterMetric(
		fmt.Sprintf("%s/%s", prefix, "refreshes"),
		s.connManager.Refreshes,
		units.None,
		"connection refreshes"); err != nil {
		return err
	}
	return nil
}
