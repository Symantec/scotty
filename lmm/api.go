package lmm

import (
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/optiopay/kafka"
)

// IsTypeSupported returns true if LMM supports the provided type.
func IsTypeSupported(t types.Type) bool {
	return isTypeSupported(t)
}

// Record represents one metric to be written to lmm.
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

type Writer struct {
	broker   *kafka.Broker
	producer kafka.DistributingProducer
	tenantId string
	apiKey   string
	topic    string
}

func NewWriter(topic string, tenantId, apiKey string, addresses []string) (
	*Writer, error) {
	return newWriter(topic, tenantId, apiKey, addresses)
}

func (w *Writer) Write(records []Record) error {
	return w.write(records)
}
