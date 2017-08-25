// Package endpointdata contains a data structure for collecting data from
// endpoints. Although scotty uses multiple goroutines to collect data from
// endpoints, only one goroutine at a time collects data from any given
// endpoint. A goroutine collecting for an endpoint uses the EndpointData
// data structure to store state information that needs to persists between
// collections for that endpoint. Each endpoint gets its own EndpointData
// data structure which is available during the collection process.
//
// The lifespan of an EndpointData instance is as follows. Each endpoint gets
// the same EndpointData instance for each collection until either the cloud
// health or cloudwatch configuration changes for that endpoint. When
// configuration for an endpoint changes, that endpoint gets a brand new
// EndpointData instance.
package endpointdata

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/machine"
)

// EndpointData stores data for an endpoint that needs to persist between
// collections.
type EndpointData struct {

	// Stores metric names already sent to the suggest engine.
	NamesSentToSuggest map[string]bool

	// Stores rolled up statistics to write to cloudhealth. nil if
	// not writing to cloudhealth
	CHRollup *chpipeline.RollUpStats

	// Stores data written to cloudhealth for the last 48 hours. nil if
	// not writing to cloudhealth
	CHStore *chpipeline.SnapshotStore

	// Stores rolled up statistics to write to cloudwatch. nil if not sending
	// data to cloudwatch
	CWRollup *chpipeline.RollUpStats
}

// NewEndpointData creates a brand new EndpointData
func NewEndpointData() *EndpointData {
	return &EndpointData{
		NamesSentToSuggest: make(map[string]bool),
	}
}

// UpdateForCloudHealth either returns e or a brand new EndpointData
// instance depending on whether or not e matches the cloudhealth
// configuration of endpoint.
func (e *EndpointData) UpdateForCloudHealth(
	endpoint *machine.Endpoint) *EndpointData {
	return e.updateForCloudHealth(endpoint)
}

// UpdateForCloudWatch either returns e or a brand new EndpointData
// instance depending on whether or not e matches the cloudwatch
// configuration of endpoint.
func (e *EndpointData) UpdateForCloudWatch(
	endpoint *machine.Endpoint) *EndpointData {
	return e.updateForCloudWatch(endpoint)
}
