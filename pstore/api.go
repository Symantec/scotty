// Packge pstore and sub packages handle writing metrics to persistent storage.
package pstore

import (
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

// Commonly used keys in TagGroup instances
const (
	TagAppName = "appname"
)

// TagGroup represents arbitrary key-value pairs describing a metric.
// Clients are to treat TagGroup instances as immutable.
type TagGroup map[string]string

// Record represents one value of one metric in persistent storage.
type Record struct {
	// Originating machine
	HostName string
	// Path of metric
	Path string
	// Arbitrary key-value pairs describing this metric
	Tags TagGroup
	// Kind of metric
	Kind types.Type
	// Unit of metric
	Unit units.Unit
	// Value of metric
	Value interface{}
	// The timestamp of the metric value.
	Timestamp time.Time
}

type Writer interface {
	// IsTypeSupported returns true if this writer supports metrics
	// of a particular kind.
	IsTypeSupported(kind types.Type) bool

	// Write writes given collection of records to persistent storage
	Write(records []Record) error
}
