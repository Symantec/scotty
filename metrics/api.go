// Package metrics provides a view of metrics that is independent of source.
package metrics

import (
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

// Value represents a single metric value.
type Value struct {
	// Required. The name of the metric. For tricorder metrics, this is
	// the metric path.
	Path string
	// Optional. The description of the metric.
	Description string
	// Required. The unit of the metric. Will be units.None if unknown.
	Unit units.Unit
	// Required. The kind of the metric.
	Kind types.Type
	// Optional. The number of bits.
	Bits int
	// Required. Value is the value of the metric. The type stored in
	// value depends on Kind according to the tricorder documentation.
	Value interface{}
	// Optional. The timestamp of the value.
	TimeStamp time.Time
	// Optional. The timestamp group ID. Required if TimeStamp is provided.
	// If a.GroupId == b.GroupId then a.TimeStamp == b.TimeStamp, but
	// the converse does not have to be true. Furthermore, GroupId must
	// remain the same for all values of a given metric. However,
	// multiple metrics can share the same group ID.
	GroupId int
}

// List represents a list of metrics from an endpoint.
type List interface {
	// Len returns the number of metrics.
	Len() int
	// Index stores the ith metric at value
	Index(i int, value *Value)
}

// SimpleList lets a slice of Value instances be a List.
type SimpleList []Value

func (s SimpleList) Len() int {
	return len(s)
}

func (s SimpleList) Index(i int, value *Value) {
	*value = s[i]
}
