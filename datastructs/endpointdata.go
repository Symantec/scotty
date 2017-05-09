package datastructs

import (
	"github.com/Symantec/scotty/chpipeline"
	"time"
)

const (
	kCloudWatchRate = "CloudWatchRate"
)

func (e *EndpointData) updateForCloudHealth(
	app *ApplicationStatus, combineFsMap map[string]bool) *EndpointData {
	// If we don't have any aws data don't do anything
	if app.Aws == nil {
		return e
	}
	if e.CHRollup == nil {
		result := *e
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
	app *ApplicationStatus, defaultFreq time.Duration) *EndpointData {
	// If we don't have any aws data don't do anything
	if app.Aws == nil {
		return e
	}
	rateStr, rateOk := app.Aws.Tags[kCloudWatchRate]
	var rate time.Duration
	if rateOk {
		var err error
		rate, err = time.ParseDuration(rateStr)
		if err != nil {
			rate = defaultFreq
		}
	}
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
