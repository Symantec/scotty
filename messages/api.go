// Package messages provides types for JSON and go rpc.
package messages

import (
	"encoding/gob"
	_ "github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
)

// Distribution represents a distribution of values
//
// The Value field of the TimestampedValue struct will hold
// a *Distribution for distributions.
//
// Distribution instances should be treated as immutable.
type Distribution struct {
	// Sum of the values
	Sum float64 `json:"sum"`
	// Total count of values
	Count uint64 `json:"count"`
	// The number of values within each range. Length is always one more
	// than length of UpperLimis field in the EndpointMetric structure
	Counts []uint64 `json:"counts"`
	// Count of rollovers starting at 1
	RollOverCount uint64 `json:"rollOverCount"`
}

// Timestamped value represents a single timestamped value.
// The type of value stored in the value field depends on the kind field
// of the enclosing EndpointMetric struct.
// See https://godoc.org/github.com/Symantec/tricorder/go/tricorder/messages#Metric
// for more detail.
type TimestampedValue struct {
	// The timestamp of the value in seconds past Jan 1, 1970 GMT
	Timestamp string `json:"timestamp"`
	// value stored here. zero equivalent stored for inactive markers.
	// For lists, the zero equivalent is an empty list.
	// For distributions, the zero equivalent is nil *Distribution
	// or for JSON, None.
	Value interface{} `json:"value"`
	// True for real values, false for inactive markers. Used to
	// distinguish inactive markers from real 0 values.
	Active bool `json:"active"`
}

// TimestampedValueList represents a list of TimestampedValue instances.
// Clients should treat TimestampedValueList instances as immutable. In
// particular, clients should not modify contained TimestampedValue instances
// in place.
type TimestampedValueList []*TimestampedValue

type LatestMetric struct {
	// The hostname, appname combination identify the endpoint
	HostName string `json:"hostName,omitempty"`
	AppName  string `json:"appName,omitempty"`

	Path        string     `json:"path,omitempty"`
	Description string     `json:"description"`
	Unit        units.Unit `json:"unit,omitempty"`
	Kind        types.Type `json:"kind"`
	SubType     types.Type `json:"subType,omitempty"`
	Bits        int        `json:"bits,omitempty"`
	// The timestamp of the value in seconds past Jan 1, 1970 GMT
	// For JSON this is a string like "123456789.987654321";
	// For GoRPC this is a time.Time instance.
	Timestamp interface{} `json:"timestamp"`
	// value stored here. The actual type stored depends on the Kind and
	// SubType fields
	Value interface{} `json:"value"`
	// The following fields only apply to distribution metrics.

	// This field is true if this distribution is not cumulative.
	IsNotCumulative bool `json:"isNotCumulative,omitempty"`
	// The upper limits for the distribution buckets.
	UpperLimits []float64 `json:"upperLimits,omitempty"`
}

// EndpointMetric represents the values of a metric on an endpoint
type EndpointMetric struct {
	// The hostname, appname combination identify the endpoint
	HostName string `json:"hostName,omitempty"`
	AppName  string `json:"appName,omitempty"`

	Path        string               `json:"path,omitempty"`
	Description string               `json:"description"`
	Unit        units.Unit           `json:"unit,omitempty"`
	Kind        types.Type           `json:"kind"`
	SubType     types.Type           `json:"subType,omitempty"`
	Bits        int                  `json:"bits,omitempty"`
	Values      TimestampedValueList `json:"values"`
	// The following fields only apply to distribution metrics.

	// This field is true if this distribution is not cumulative.
	IsNotCumulative bool `json:"isNotCumulative,omitempty"`
	// The upper limits for the distribution buckets.
	UpperLimits []float64 `json:"upperLimits,omitempty"`
}

// EndpointMetricList represents a list of EndpointMetric. Client should
// treat EndpointMetricList instances as immutable. In particular,
// clients should not modify contained EndpointMetric instances in place.
type EndpointMetricList []*EndpointMetric

// Error represents an error retrieving metrics from a particular endpoint
type Error struct {
	HostName  string `json:"hostName"`
	Timestamp string `json:"timestamp"`
	Error     string `json:"error"`
}

// ErrorList represents a list of Error instances. Clients should treat
// ErrorList instances as immutable.
type ErrorList []*Error

func init() {
	var dist *Distribution
	gob.RegisterName("*scotty.messages.Distribution", dist)
}
