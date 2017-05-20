package datastructs

import (
	"github.com/Symantec/scotty/chpipeline"
	"time"
)

func (e *EndpointData) updateForCloudHealth(
	app *ApplicationStatus,
	combineFsMap map[string]bool,
	bTestRun bool) *EndpointData {
	// If we don't have any aws data don't do anything
	if app.Aws == nil {
		return e
	}
	needToDoCloudHealth := bTestRun == app.CloudHealthTest()
	doingCloudHealth := e.CHRollup != nil
	if needToDoCloudHealth != doingCloudHealth {
		result := *e
		if !needToDoCloudHealth {
			result.CHRollup = nil
			result.CHCombineFS = false
			return &result
		}
		instanceId := app.InstanceId()
		result.CHRollup = chpipeline.NewRollUpStats(
			app.AccountNumber(),
			instanceId,
			time.Hour)
		result.CHCombineFS = true
		if combineFsMap != nil {
			result.CHCombineFS = combineFsMap[instanceId]
		}
		return &result
	}
	return e
}

func (e *EndpointData) updateForCloudWatch(
	app *ApplicationStatus,
	defaultFreq time.Duration,
	bTestRun bool) *EndpointData {
	rate, rateOk := app.CloudWatchRefreshRate(defaultFreq)
	rateOk = rateOk && (bTestRun == app.CloudWatchTest())
	cwExists := e.CWRollup != nil

	// Calling RoundDuration() here is safe because its returned value never
	// changes for a particular instance
	if cwExists != rateOk || (cwExists && rate != e.CWRollup.RoundDuration()) {
		result := *e
		if !rateOk {
			result.CWRollup = nil
		} else {
			result.CWRollup = chpipeline.NewRollUpStats(
				app.AccountNumber(),
				app.InstanceId(),
				rate)
		}
		return &result
	}
	return e
}
