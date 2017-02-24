// Package queuesender implements asynchronous http.
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
	queue              *queueType
	connManager        *connManagerType
	lock               sync.Mutex
	lastResponseError  string
	responseErrorCount uint64
	successfulWrites   uint64
}

// New returns a new sender.
// endpoint is the common base of all endpoints.
// size is the size of the queue.
// name is the name of queue. Used strictly for logging.
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

// Register registers metrics for this instance with tricorder.
// Metrics registered under "/asyncWriters/<name>"
func (s *Sender) Register(name string) error {
	return s.register(name)
}
