// Package awsinfo contains routines to extract from AWS metadata
package awsinfo

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"time"
)

// AwsInfo represents information on an AWS machine for scotty.
type AwsInfo struct {
	// The account ID
	AccountId string

	// The account Name
	AccountName string

	// The InstanceId
	InstanceId string

	// The region
	Region string

	// Whether or not scotty should write health-agent data to cloud health
	CloudHealth bool

	// How often scotty is to write health-agent data to cloud watch. 0
	// means don't write to cloud watch.
	CloudWatch time.Duration
}

// Config represents scotty configuration for AWS
type Config struct {
	// Whether or not scotty is running in cloud health test mode
	CloudHealthTest bool

	// Whether or not scotty is running in cloud watch test mode
	CloudWatchTest bool

	// The default frequency scotty is to write to cloud watch.
	// Zero value means the default, 5 minutes.
	CloudWatchRefresh time.Duration
}

// GetAwsInfo creates an AwsInfo instance from existing mdb metadata.
// If aws is nil, GetAwsInfo returns nil.
func (c Config) GetAwsInfo(aws *mdb.AwsMetadata) *AwsInfo {
	return c.getAwsInfo(aws)
}
