package endpointdata

import (
	"flag"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/machine"
	"time"
)

var fCloudHealthPath = flag.String("cloudhHealthStoragePath", "/var/lib/scotty/cloudhealth", "Directory where scotty stores cloud health data")

func (e *EndpointData) updateForCloudHealth(
	endpoint *machine.Endpoint) *EndpointData {
	if endpoint.M.Aws == nil {
		return e
	}
	if endpoint.App.EP.AppName() != application.HealthAgentName {
		return e
	}
	needToDoCloudHealth := endpoint.M.Aws.CloudHealth
	doingCloudHealth := e.CHRollup != nil
	if needToDoCloudHealth != doingCloudHealth {
		result := *e
		if !needToDoCloudHealth {
			result.CHRollup = nil
			result.CHStore = nil
			return &result
		}
		result.CHRollup = chpipeline.NewRollUpStats(
			endpoint.M.Aws.AccountId,
			endpoint.M.Aws.InstanceId,
			time.Hour)
		result.CHStore = chpipeline.NewSnapshotStore(
			*fCloudHealthPath,
			endpoint.App.EP.HostName(),
			endpoint.App.EP.AppName(), 48*time.Hour)
		result.CHStore.Load()
		return &result
	}
	return e
}

func (e *EndpointData) updateForCloudWatch(
	endpoint *machine.Endpoint) *EndpointData {
	if endpoint.M.Aws == nil {
		return e
	}
	if endpoint.App.EP.AppName() != application.HealthAgentName {
		return e
	}
	newCloudWatchRefresh := endpoint.M.Aws.CloudWatch
	newCloudWatchRefreshOk := newCloudWatchRefresh != 0
	cwExists := e.CWRollup != nil

	// Calling RoundDuration() here is safe because its returned value never
	// changes for a particular instance
	if cwExists != newCloudWatchRefreshOk || (cwExists && newCloudWatchRefresh != e.CWRollup.RoundDuration()) {
		result := *e
		if !newCloudWatchRefreshOk {
			result.CWRollup = nil
		} else {
			result.CWRollup = chpipeline.NewRollUpStats(
				endpoint.M.Aws.AccountId,
				endpoint.M.Aws.InstanceId,
				newCloudWatchRefresh)
		}
		return &result
	}
	return e
}
