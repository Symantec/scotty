package queuesender

import (
	"log"
	"sync"
)

// Sender sends JSON requests asynchronously
type Sender struct {
	// log messages logged here
	logger *loggerType
	// requests queued here
	queue *queueType
	// channel to request that the connection be refreshed.
	// Send id of connection. Each connection refreshed once even if id
	// sent multiple times.
	connResetCh chan int
	// lock for connection and connection id
	lock sync.Mutex
	// condition variable for the same
	cond sync.Cond
	// connection id. Starts at 0. Increments by 1 each time
	// connection is refreshed.
	connId int
	// connection. Changes with each connection refresh.
	conn *connType
}

// New returns a new sender.
// endpoint is the common base of all endpoints
// size is the size of the queue
// name is the name of queue. Used strictly for logging
// logger is the logger to receive the logs.
func New(endpoint string, size int, name string, logger *log.Logger) (
	*Sender, error) {
	return _new(endpoint, size, name, logger)
}

// Send sends given json to the given endpoint.
// Send will block if queue if full because of encountered errors.
func (s *Sender) Send(endpoint string, json interface{}) {
	s.queue.Add(requestType{Endpoint: endpoint, Json: json})
}
