package awsinfo_test

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty/awsinfo"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestAwsInfo(t *testing.T) {
	Convey("Regular mode", t, func() {
		var config awsinfo.Config
		Convey("regular machine", func() {
			So(config.GetAwsInfo(nil), ShouldBeNil)
			aws := &mdb.AwsMetadata{
				AccountId:   "123",
				AccountName: "abc",
				InstanceId:  "i-456",
				Region:      "us-east-1",
			}
			info := config.GetAwsInfo(aws)
			So(info.AccountId, ShouldEqual, "123")
			So(info.AccountName, ShouldEqual, "abc")
			So(info.InstanceId, ShouldEqual, "i-456")
			So(info.Region, ShouldEqual, "us-east-1")
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 0)
		})
		Convey("cloud watch machine default rate", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "bad",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 5*time.Minute)
		})
		Convey("cloud watch machine rate too small", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "1s",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 5*time.Minute)
		})
		Convey("cloud watch machine special rate", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 3*time.Minute)
		})
		Convey("cloud watch test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudWatchTest":    "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 0)
		})
		Convey("cloud health test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudHealthTest":   "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeFalse)
			So(info.CloudWatch, ShouldEqual, 3*time.Minute)
		})
	})
	Convey("Cloud health test mode", t, func() {
		config := awsinfo.Config{CloudHealthTest: true}
		Convey("cloud watch test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudWatchTest":    "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeFalse)
			So(info.CloudWatch, ShouldEqual, 0)
		})
		Convey("cloud health test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudHealthTest":   "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 3*time.Minute)
		})
	})
	Convey("Cloud watch test mode", t, func() {
		config := awsinfo.Config{CloudWatchTest: true}
		Convey("cloud watch test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudWatchTest":    "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeTrue)
			So(info.CloudWatch, ShouldEqual, 3*time.Minute)
		})
		Convey("cloud health test machine", func() {
			aws := &mdb.AwsMetadata{
				Tags: map[string]string{
					"PushMetricsToCloudWatch": "3m",
					"ScottyCloudHealthTest":   "",
				},
			}
			info := config.GetAwsInfo(aws)
			So(info.CloudHealth, ShouldBeFalse)
			So(info.CloudWatch, ShouldEqual, 0)
		})
	})
}
