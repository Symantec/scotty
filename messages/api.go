package messages

import (
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
)

// Timestamped value represents a single timestamped value.
// The type of value stored in the value field depends on the kind field
// of the enclosing MachineMetrics struct.
// See https://godoc.org/github.com/Symantec/tricorder/go/tricorder/messages#Metric
// for more detail.
type TimestampedValue struct {
	// The timestamp of the value in seconds past Jan 1, 1970 GMT
	Timestamp string `json:"timestamp"`
	// value stored here.
	Value interface{} `json:"value"`
}

// TimestampedValueList represents a list of TimestampedValue instances.
// Clients should treat TimestampedValueList instances as immutable. In
// particular, clients should not modify contained TimestampedValue instances
// in place.
type TimestampedValueList []*TimestampedValue

// MachineMetrics represents the current metrics on a machine
type MachineMetrics struct {
	HostName    string               `json:"hostName,omitempty"`
	Path        string               `json:"path,omitempty"`
	Description string               `json:"description"`
	Unit        units.Unit           `json:"unit"`
	Kind        types.Type           `json:"kind"`
	Bits        int                  `json:"bits,omitempty"`
	Values      TimestampedValueList `json:"values"`
}

// MachineMetricsList represents a list of MachineMetrics. Client should
// treat MachineMetricsList instances as immutable. In particular,
// clients should not modify contained MachineMetrics instances in place.
type MachineMetricsList []*MachineMetrics

// Error represents an error retrieving metrics from a particular machine
type Error struct {
	HostName  string `json:"hostName"`
	Timestamp string `json:"timestamp"`
	Error     string `json:"error"`
}

// ErrorList represents a list of Error instances. Clients should treat
// ErrorList instances as immutable.
type ErrorList []*Error
