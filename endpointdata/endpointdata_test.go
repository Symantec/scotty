package endpointdata_test

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/awsinfo"
	"github.com/Symantec/scotty/endpointdata"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/trisource"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var (
	kSources = sources.ConnectorList{trisource.GetConnector()}
)

func TestNonAws(t *testing.T) {
	endpoint := &machine.Endpoint{
		M:   &machine.Machine{},
		App: &application.Application{},
	}
	Convey("Non aws endpoints skipped", t, func() {
		data := endpointdata.NewEndpointData()
		So(data.NamesSentToSuggest, ShouldNotBeNil)
		newData := data.UpdateForCloudHealth(endpoint)
		newData = newData.UpdateForCloudWatch(endpoint)
		So(newData, ShouldEqual, data)
	})
}

func TestNonHealthAgent(t *testing.T) {
	endpoint := &machine.Endpoint{
		M: &machine.Machine{
			Aws: &awsinfo.AwsInfo{},
		},
		App: &application.Application{
			EP: scotty.NewEndpointWithConnector("host", "not health agent", nil),
		},
	}
	Convey("Non health agent endpoints skipped", t, func() {
		data := endpointdata.NewEndpointData()
		So(data.NamesSentToSuggest, ShouldNotBeNil)
		newData := data.UpdateForCloudHealth(endpoint)
		newData = newData.UpdateForCloudWatch(endpoint)
		So(newData, ShouldEqual, data)
	})
}

func TestCloudHealth(t *testing.T) {
	Convey("Non health agent endpoints skipped", t, func() {
		endpoint := &machine.Endpoint{
			M: &machine.Machine{
				Aws: &awsinfo.AwsInfo{
					AccountId:   "12345",
					InstanceId:  "i-12345",
					CloudHealth: true},
			},
			App: &application.Application{
				EP: scotty.NewEndpointWithConnector("host", application.HealthAgentName, nil),
			},
		}
		data := endpointdata.NewEndpointData()
		newData := data.UpdateForCloudHealth(endpoint)
		So(newData, ShouldNotEqual, data)
		data = newData
		So(data.CHRollup.InstanceId(), ShouldEqual, "i-12345")
		So(data.CHRollup.AccountNumber(), ShouldEqual, "12345")
		So(data.CHStore.HostName(), ShouldEqual, "host")
		So(data.CHStore.AppName(), ShouldEqual, application.HealthAgentName)

		newData = data.UpdateForCloudHealth(endpoint)
		So(newData, ShouldEqual, data)

		// Now cloud health off
		endpoint.M.Aws.CloudHealth = false

		newData = data.UpdateForCloudHealth(endpoint)
		So(newData, ShouldNotEqual, data)
		data = newData
		So(data.CHRollup, ShouldBeNil)
		So(data.CHStore, ShouldBeNil)
		newData = data.UpdateForCloudHealth(endpoint)
		So(newData, ShouldEqual, data)
	})
}

func TestCloudWatch(t *testing.T) {
	Convey("Non health agent endpoints skipped", t, func() {
		endpoint := &machine.Endpoint{
			M: &machine.Machine{
				Aws: &awsinfo.AwsInfo{
					AccountId:  "12345",
					InstanceId: "i-12345",
					CloudWatch: 4 * time.Minute},
			},
			App: &application.Application{
				EP: scotty.NewEndpointWithConnector("host", application.HealthAgentName, nil),
			},
		}
		data := endpointdata.NewEndpointData()
		newData := data.UpdateForCloudWatch(endpoint)
		So(newData, ShouldNotEqual, data)
		data = newData
		So(data.CWRollup.InstanceId(), ShouldEqual, "i-12345")
		So(data.CWRollup.AccountNumber(), ShouldEqual, "12345")
		So(data.CWRollup.RoundDuration(), ShouldEqual, 4*time.Minute)

		newData = data.UpdateForCloudWatch(endpoint)
		So(newData, ShouldEqual, data)

		// Now cloud watch 6 minutes
		endpoint.M.Aws.CloudWatch = 6 * time.Minute

		newData = data.UpdateForCloudWatch(endpoint)
		So(newData, ShouldNotEqual, data)
		data = newData
		So(data.CWRollup.RoundDuration(), ShouldEqual, 6*time.Minute)

		// Now cloud watch off
		endpoint.M.Aws.CloudWatch = 0

		newData = data.UpdateForCloudWatch(endpoint)
		So(newData, ShouldNotEqual, data)
		data = newData
		So(data.CWRollup, ShouldBeNil)
		newData = data.UpdateForCloudWatch(endpoint)
		So(newData, ShouldEqual, data)
	})
}
