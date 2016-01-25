// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
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
