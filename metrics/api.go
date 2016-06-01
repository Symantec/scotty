// Package metrics provides a view of metrics that is independent of source.
package metrics

import (
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

// Value represents a single metric value.
type Value struct {
	// Required. The name of the metric. For tricorder metrics, this is
	// the metric path. This field must be unique across a list of metrics.
	Path string
	// Optional. The description of the metric.
	Description string
	// Optional. The unit of the metric.
	Unit units.Unit
	// Required. Value is the value of the metric. The type stored in
	// value must be one of the following:
	// bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64,
	// float32, float64, string, time.Time, time.Duration, or
	// tricorder/go/tricorder/messages.Distribution
	Value interface{}
	// Optional. The timestamp of the value. If present in one value in a
	// list then it is required for ALL values in that same list.
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

// VerifyList verifies that the given list adheres to the specification.
func VerifyList(list List) error {
	return verifyList(list)
}

// SimpleList lets a slice of Value instances be a List.
type SimpleList []Value

func (s SimpleList) Len() int {
	return len(s)
}

func (s SimpleList) Index(i int, value *Value) {
	*value = s[i]
}
