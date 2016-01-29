// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sync"
	"time"
)

// Record represents one value of one metric in persistent storage.
type Record struct {
	// Originating machine
	HostName string
	// Originating application
	AppName string
	// Path of metric
	Path string
	// Kind of metric
	Kind types.Type
	// Unit of metric
	Unit units.Unit
	// Value of metric
	Value interface{}
	// Timestamp as seconds after Jan 1, 1970 GMT.
	Timestamp float64
}

type Writer interface {
	// IsTypeSupported returns true if this writer supports metrics
	// of a particular kind.
	IsTypeSupported(kind types.Type) bool

	// Write writes given collection of records to persistent storage
	Write(records []Record) error
}

// NoBlockPStore lets clients send metric values asynchronously to an
// existing Writer.
type NoBlockPStore struct {
	inChannel          chan Record
	batchSize          int
	channelSize        int
	flushDelay         time.Duration
	writer             Writer
	perMetricWriteDist *tricorder.CumulativeDistribution
	batchSizeDist      *tricorder.CumulativeDistribution
	lock               sync.Mutex
	data               noBlockPStoreDataType
}

// NewNoBlockPStore creates a new NoBlockPStore.
// writer is the existing writer to wrap.
// batchSize is the desired number of metric values to send at once to writer.
// channelSize is the fixed size of the queue buffering metric values.
// flushDelay indicates how long the returned NoBlockPStore instance must be
// idle before it flushes unwritten metrics that do not make a complete batch
// out to the underlying Writer.
func NewNoBlockPStore(
	writer Writer,
	batchSize,
	channelSize int,
	flushDelay time.Duration) *NoBlockPStore {
	return newNoBlockPStore(writer, batchSize, channelSize, flushDelay)
}

// Write writes the contents of record to the underlying writer asynchronously.
// Write adds the contents of record to this instance's queue and returns
// immediately writing the contents of record to the underlying writer
// sometime later.
// If the queue is already full or if the underlying writer does not support
// the kind of metric value in record, Write simply ignores the request.
func (n *NoBlockPStore) Write(record *Record) {
	n.write(record)
}

// RegisterMetrics registers the metrics for this instance. Generally, each
// application will have only one such instance. It is an error to call
// RegisterMetrics on multiple instances.
func (n *NoBlockPStore) RegisterMetrics() error {
	return n.registerMetrics()
}
