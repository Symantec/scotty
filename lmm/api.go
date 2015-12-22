package lmm

import (
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/optiopay/kafka"
)

// IsTypeSupported returns true if LMM supports the provided type.
func IsTypeSupported(t types.Type) bool {
	return isTypeSupported(t)
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

func (w *Writer) Write(records []*store.Record) error {
	return w.write(records)
}
