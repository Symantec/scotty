package datastructs_test

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty/datastructs"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestEndpointData(t *testing.T) {
	Convey("With EndpointData", t, func() {
		data := datastructs.NewEndpointData()
		So(data.NamesSentToSuggest, ShouldNotBeNil)
		Convey("With accountId and instanceId", func() {
			app := &datastructs.ApplicationStatus{
				Aws: &mdb.AwsMetadata{
					AccountId:  "2468",
					InstanceId: "1357",
				},
			}
			Convey("CloudHealth should work", func() {
				newData := data.UpdateForCloudHealth(app, nil)
				So(newData, ShouldNotEqual, data)
				So(newData.CHRollup.InstanceId(), ShouldEqual, "1357")
				So(newData.CHRollup.AccountNumber(), ShouldEqual, "2468")
				So(newData.CHCombineFS, ShouldBeTrue)

				Convey("No new memory allocation if nothing changed", func() {
					newData2 := newData.UpdateForCloudHealth(app, nil)
					So(newData2, ShouldEqual, newData)
				})
			})
			Convey("CloudHealth shouldn't combine fs if turned off.", func() {
				newData := data.UpdateForCloudHealth(
					app,
					map[string]bool{})
				So(newData.CHCombineFS, ShouldBeFalse)
			})
			Convey("CloudHealth shouldn't combine fs if instanceId doesn't match.", func() {
				newData := data.UpdateForCloudHealth(
					app,
					map[string]bool{"9999": true})
				So(newData.CHCombineFS, ShouldBeFalse)
			})
			Convey("CloudHealth should combine fs if instanceId matches.", func() {
				newData := data.UpdateForCloudHealth(
					app,
					map[string]bool{"1357": true})
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
			newData := data.UpdateForCloudWatch(app, 5*time.Minute)
			So(newData, ShouldNotEqual, data)
			So(newData.CWRollup.InstanceId(), ShouldEqual, "1357")
			So(newData.CWRollup.AccountNumber(), ShouldEqual, "2468")
			So(newData.CWRollup.RoundDuration(), ShouldEqual, 2*time.Minute)
			Convey("No new memory allocation if nothing changed", func() {
				noChange := newData.UpdateForCloudWatch(app, 5*time.Minute)
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
					newData := data.UpdateForCloudWatch(app, 5*time.Minute)
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
					newData := data.UpdateForCloudWatch(app, 5*time.Minute)
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
					newData := data.UpdateForCloudWatch(app, 5*time.Minute)
					So(newData, ShouldNotEqual, data)
					So(newData.CWRollup.InstanceId(), ShouldEqual, "1357")
					So(newData.CWRollup.AccountNumber(), ShouldEqual, "2468")
					So(newData.CWRollup.RoundDuration(), ShouldEqual, 3*time.Minute)
				})
			})
		})
	})
}
