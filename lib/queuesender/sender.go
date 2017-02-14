package queuesender

import (
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
		queue:       newQueue(size),
		connResetCh: make(chan int, 10),
	}
	if logger != nil {
		result.logger = &loggerType{name: name, logger: logger}
	}
	result.cond.L = &result.lock
	result.conn = conn
	go result.connResetLoop()
	go result.sendLoop()
	go result.receiveLoop()
	return result, nil
}

// getConn fetches the current connection Id and connection which can
// change at any time.
func (s *Sender) getConn() (int, *connType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.connId, s.conn
}

// setConn Sets the current connection ID and connection
func (s *Sender) setConn(connId int, conn *connType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.connId, s.conn = connId, conn
	s.cond.Broadcast()
}

// waitForRefresh blocks the caller until the current connection has an id
// bigger than connId.
func (s *Sender) waitForRefresh(connId int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for s.connId <= connId {
		s.cond.Wait()
	}
}

// This loop handles requests to refresh the connection
func (s *Sender) connResetLoop() {
	for connIdToReset := range s.connResetCh {
		connId, conn := s.getConn()
		// Only do the refresh if the conn ids match. This check ensures that
		// each connection gets refreshed only once.
		if connId == connIdToReset {
			newConn := conn.RefreshAndClose(s.logger)
			s.setConn(connId+1, newConn)
			s.queue.ResetNextNotSent(connId + 1)
		}
	}
}

// This loop sends requests in the queue.
func (s *Sender) sendLoop() {
	for {
		connId, conn := s.getConn()
		req, ok := s.queue.NextNotSent(connId)
		if !ok {
			// A new connection is available
			continue
		}
		err := conn.Send(req)
		// On error we have to refresh the connection
		if err != nil {
			s.connResetCh <- connId
			// Avoids using the same bad connection over and over again
			s.waitForRefresh(connId)
		}
	}
}

// This loop receives the responses from the requests.
// Responses received are expected to come in the same order as requests are
// sent.
func (s *Sender) receiveLoop() {
	for {
		connId, conn := s.getConn()
		ok, err := conn.Read()
		// Oops we have to refresh the connection
		if !ok {
			s.connResetCh <- connId
			// Avoids using the same bad connection over and over
			s.waitForRefresh(connId)
			continue
		}
		// A real error in response
		if err != nil {
			s.logger.Log(err)
			s.queue.MoveToNotSent()
		} else {
			s.queue.DiscardNextSent()
		}
	}
}
