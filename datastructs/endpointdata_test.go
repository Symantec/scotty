package datastructs_test

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/trisource"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var (
	kSources = sources.ConnectorList{trisource.GetConnector()}
)

func TestEndpointData(t *testing.T) {
	Convey("With EndpointData", t, func() {
		data := datastructs.NewEndpointData()
		So(data.NamesSentToSuggest, ShouldNotBeNil)
		Convey("ScottyCloudWatchTest and ScottyCloudHealthTest tags set", func() {
			app := &datastructs.ApplicationStatus{
				Aws: &mdb.AwsMetadata{
					AccountId:  "2468",
					InstanceId: "1357",
					Tags: map[string]string{
						"ScottyCloudWatchTest":    "true",
						"ScottyCloudHealthTest":   "true",
						"PushMetricsToCloudWatch": "",
					},
				},
			}
			Convey("Machines with ScottyCloudHealthTest tag applicable for test scotty with CloudHealth", func() {
				newData := data.UpdateForCloudHealth(app, nil, true)
				So(newData, ShouldNotEqual, data)
			})
			Convey("Machines with ScottyCloudWatchTest tag applicable for test scotty with CloudWatch", func() {
				newData := data.UpdateForCloudWatch(app, 5*time.Minute, true)
				So(newData, ShouldNotEqual, data)
			})
			Convey("Machines with ScottyCloudHealthTest tag not applicable for production scotty with cloud health", func() {
				newData := data.UpdateForCloudHealth(app, nil, false)
				So(newData, ShouldEqual, data)
			})
			Convey("Machines with ScottyCloudWatchTest tag not applicable for production scotty with cloud watch", func() {
				newData := data.UpdateForCloudWatch(app, 5*time.Minute, false)
				So(newData, ShouldEqual, data)
			})
		})
		Convey("With accountId and instanceId", func() {
			app := &datastructs.ApplicationStatus{
				EndpointId: scotty.NewEndpointWithConnector(
					"host1", 1, kSources),
				Aws: &mdb.AwsMetadata{
					AccountId:  "2468",
					InstanceId: "1357",
				},
			}

			Convey("CloudHealth not applicable for test scotty", func() {
				newData := data.UpdateForCloudHealth(app, nil, true)
				So(newData, ShouldEqual, data)
			})

			Convey("CloudHealth should work", func() {
				newData := data.UpdateForCloudHealth(app, nil, false)
				So(newData, ShouldNotEqual, data)
				So(newData.CHRollup.InstanceId(), ShouldEqual, "1357")
				So(newData.CHRollup.AccountNumber(), ShouldEqual, "2468")
				So(newData.CHCombineFS, ShouldBeTrue)
				So(newData.CHStore.HostName(), ShouldEqual, "host1")
				So(newData.CHStore.Port(), ShouldEqual, 1)

				Convey("No new memory allocation if nothing changed", func() {
					newData2 := newData.UpdateForCloudHealth(app, nil, false)
					So(newData2, ShouldEqual, newData)
				})
			})
			Convey("CloudHealth shouldn't combine fs if turned off.", func() {
				newData := data.UpdateForCloudHealth(
					app, map[string]bool{}, false)
				So(newData.CHCombineFS, ShouldBeFalse)
			})
			Convey("CloudHealth shouldn't combine fs if instanceId doesn't match.", func() {
				newData := data.UpdateForCloudHealth(
					app, map[string]bool{"9999": true}, false)
				So(newData.CHCombineFS, ShouldBeFalse)
			})
			Convey("CloudHealth should combine fs if instanceId matches.", func() {
				newData := data.UpdateForCloudHealth(
					app, map[string]bool{"1357": true}, false)
				So(newData.CHCombineFS, ShouldBeTrue)
			})
		})
		Convey("CloudWatch shoud work", func() {
			app := &datastructs.ApplicationStatus{
				Aws: &mdb.AwsMetadata{
					AccountId:  "2468",
					InstanceId: "1357",
					Tags: map[string]string{
						"PushMetricsToCloudWatch": "2m",
					},
				},
			}
			// Scotty for testing ignores this machine for cloudwatch
			So(
				data.UpdateForCloudWatch(app, 5*time.Minute, true),
				ShouldEqual,
				data)
			newData := data.UpdateForCloudWatch(app, 5*time.Minute, false)
			So(newData, ShouldNotEqual, data)
			So(newData.CWRollup.InstanceId(), ShouldEqual, "1357")
			So(newData.CWRollup.AccountNumber(), ShouldEqual, "2468")
			So(newData.CWRollup.RoundDuration(), ShouldEqual, 2*time.Minute)
			Convey("No new memory allocation if nothing changed", func() {
				noChange := newData.UpdateForCloudWatch(
					app, 5*time.Minute, false)
				So(noChange, ShouldEqual, newData)
			})
			Convey("CloudWatch should accept changes", func() {
				data := newData
				Convey("No tag", func() {
					app := &datastructs.ApplicationStatus{
						Aws: &mdb.AwsMetadata{
							AccountId:  "2468",
							InstanceId: "1357",
						},
					}
					newData := data.UpdateForCloudWatch(
						app, 5*time.Minute, false)
					So(newData, ShouldNotEqual, data)
					So(newData.CWRollup, ShouldBeNil)
				})
				Convey("Tag but no value", func() {
					app := &datastructs.ApplicationStatus{
						Aws: &mdb.AwsMetadata{
							AccountId:  "2468",
							InstanceId: "1357",
							Tags: map[string]string{
								"PushMetricsToCloudWatch": "",
							},
						},
					}
					newData := data.UpdateForCloudWatch(
						app, 5*time.Minute, false)
					So(newData, ShouldNotEqual, data)
					So(newData.CWRollup.InstanceId(), ShouldEqual, "1357")
					So(newData.CWRollup.AccountNumber(), ShouldEqual, "2468")
					So(newData.CWRollup.RoundDuration(), ShouldEqual, 5*time.Minute)
				})
				Convey("Tag but different value", func() {
					app := &datastructs.ApplicationStatus{
						Aws: &mdb.AwsMetadata{
							AccountId:  "2468",
							InstanceId: "1357",
							Tags: map[string]string{
								"PushMetricsToCloudWatch": "3m",
							},
						},
					}
					newData := data.UpdateForCloudWatch(
						app, 5*time.Minute, false)
					So(newData, ShouldNotEqual, data)
					So(newData.CWRollup.InstanceId(), ShouldEqual, "1357")
					So(newData.CWRollup.AccountNumber(), ShouldEqual, "2468")
					So(newData.CWRollup.RoundDuration(), ShouldEqual, 3*time.Minute)
				})
			})
		})
	})
}
