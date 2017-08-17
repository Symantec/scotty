package awsinfo

import (
	"errors"
	"github.com/Symantec/Dominator/lib/mdb"
	"time"
)

const (
	kCloudWatchRefreshDefault = 5 * time.Minute
)

var (
	kDurationTooSmall = errors.New("Duration too small")
)

func (c Config) getAwsInfo(aws *mdb.AwsMetadata) *AwsInfo {
	if aws == nil {
		return nil
	}
	if c.CloudWatchRefresh == 0 {
		c.CloudWatchRefresh = kCloudWatchRefreshDefault
	}
	result := AwsInfo{
		AccountId:   aws.AccountId,
		AccountName: aws.AccountName,
		InstanceId:  aws.InstanceId,
		Region:      aws.Region,
	}
	result.CloudHealth = c.CloudHealthTest == forTest(aws, "ScottyCloudHealthTest")
	if c.CloudWatchTest == forTest(aws, "ScottyCloudWatchTest") {
		durStr, ok := aws.Tags["PushMetricsToCloudWatch"]
		if ok {
			var err error
			result.CloudWatch, err = parseDuration(durStr)
			if err != nil {
				result.CloudWatch = c.CloudWatchRefresh
			}
		}
	}
	return &result
}

func forTest(aws *mdb.AwsMetadata, tagName string) bool {
	_, ok := aws.Tags[tagName]
	return ok
}

func parseDuration(durStr string) (time.Duration, error) {
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return 0, err
	}
	if dur < time.Minute {
		return 0, kDurationTooSmall
	}
	return dur, nil
}
