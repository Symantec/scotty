package machine_test

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/awsinfo"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/namesandports"
	"github.com/Symantec/scotty/store"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	Convey("Test API", t, func() {
		aStore := store.NewStore(10, 100, 1.0, 10)
		endpointStore := machine.NewEndpointStore(
			aStore,
			awsinfo.Config{CloudWatchRefresh: 5 * time.Minute},
			0)
		endpointStore.UpdateMachines(
			100.0,
			[]mdb.Machine{
				{
					Hostname:  "host1",
					IpAddress: "10.1.1.1",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId:   "12345",
						AccountName: "prod",
						InstanceId:  "i-2468",
						Region:      "us-east-1",
					},
				},
				{
					Hostname:  "host2",
					IpAddress: "10.1.1.2",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId:   "67890",
						AccountName: "dev",
						InstanceId:  "i-1357",
						Region:      "us-west-2",
					},
				},
			})
		// did adding the machines work

		endpoints, store := endpointStore.AllWithStore()
		So(endpoints, ShouldHaveLength, 2)

		healthAgent1, store := endpointStore.ByHostAndName(
			"host1", application.HealthAgentName)
		So(healthAgent1.M.Host, ShouldEqual, "host1")
		So(healthAgent1.M.IpAddress, ShouldEqual, "10.1.1.1")
		So(healthAgent1.M.Active, ShouldBeTrue)
		So(healthAgent1.M.Region, ShouldEqual, "us-east-1")
		So(healthAgent1.M.Aws.AccountId, ShouldEqual, "12345")
		So(healthAgent1.App.EP.HostName(), ShouldEqual, "host1")
		So(healthAgent1.App.EP.AppName(), ShouldEqual, application.HealthAgentName)
		So(healthAgent1.App.Port, ShouldEqual, application.HealthAgentPort)
		So(store.IsRegistered(healthAgent1.App.EP), ShouldBeTrue)
		So(store.IsEndpointActive(healthAgent1.App.EP), ShouldBeTrue)
		So(healthAgent1.App.Active, ShouldBeTrue)

		healthAgent2, store := endpointStore.ByHostAndName(
			"host2", application.HealthAgentName)
		So(healthAgent2.M.Host, ShouldEqual, "host2")
		So(healthAgent2.M.IpAddress, ShouldEqual, "10.1.1.2")
		So(healthAgent2.M.Active, ShouldBeTrue)
		So(healthAgent2.M.Region, ShouldEqual, "us-west-2")
		So(healthAgent2.M.Aws.AccountId, ShouldEqual, "67890")
		So(healthAgent2.App.EP.HostName(), ShouldEqual, "host2")
		So(healthAgent2.App.EP.AppName(), ShouldEqual, application.HealthAgentName)
		So(healthAgent2.App.Port, ShouldEqual, application.HealthAgentPort)
		So(store.IsRegistered(healthAgent2.App.EP), ShouldBeTrue)
		So(store.IsEndpointActive(healthAgent2.App.EP), ShouldBeTrue)
		So(healthAgent2.App.Active, ShouldBeTrue)

		// Add some apps
		endpointStore.UpdateEndpoints(
			100.0,
			map[string]machine.EndpointObservation{
				"host1": {
					SeqNo: 1,
					Endpoints: namesandports.NamesAndPorts{
						"scotty": {Port: 6980},
						"subd":   {Port: 6912},
					},
				},
				"host2": {
					SeqNo: 1,
					Endpoints: namesandports.NamesAndPorts{
						"scotty":    {Port: 6980},
						"dominator": {Port: 6970, IsTLS: true},
					},
				},
			})

		endpoints, store = endpointStore.AllWithStore()
		So(endpoints, ShouldHaveLength, 6)
		scotty1, store := endpointStore.ByHostAndName("host1", "scotty")
		So(scotty1.M.Host, ShouldEqual, "host1")
		So(scotty1.M.Active, ShouldBeTrue)
		So(scotty1.M.Aws.AccountId, ShouldEqual, "12345")
		So(scotty1.App.EP.HostName(), ShouldEqual, "host1")
		So(scotty1.App.EP.AppName(), ShouldEqual, "scotty")
		So(scotty1.App.Active, ShouldBeTrue)
		So(scotty1.App.Port, ShouldEqual, 6980)
		So(scotty1.App.IsTLS, ShouldBeFalse)
		So(store.IsRegistered(scotty1.App.EP), ShouldBeTrue)
		So(store.IsEndpointActive(scotty1.App.EP), ShouldBeTrue)
		dominator2, store := endpointStore.ByHostAndName("host2", "dominator")
		So(dominator2.M.Host, ShouldEqual, "host2")
		So(dominator2.M.Active, ShouldBeTrue)
		So(dominator2.M.Aws.AccountId, ShouldEqual, "67890")
		So(dominator2.App.EP.HostName(), ShouldEqual, "host2")
		So(dominator2.App.EP.AppName(), ShouldEqual, "dominator")
		So(dominator2.App.Active, ShouldBeTrue)
		So(dominator2.App.Port, ShouldEqual, 6970)
		So(dominator2.App.IsTLS, ShouldBeTrue)
		So(store.IsRegistered(dominator2.App.EP), ShouldBeTrue)
		So(store.IsEndpointActive(dominator2.App.EP), ShouldBeTrue)

		// machines change host1, host3: host2 inactive
		endpointStore.UpdateMachines(
			100.0,
			[]mdb.Machine{
				{
					Hostname: "host1",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId:   "12345",
						AccountName: "prod",
						InstanceId:  "i-2468",
						Region:      "us-east-2",
					},
				},
				{
					Hostname: "host3",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId: "24680",
					},
				},
			})
		endpoints, store = endpointStore.AllWithStore()
		So(endpoints, ShouldHaveLength, 7)
		endpoints, store = endpointStore.AllActiveWithStore()
		// host2 inactive 3 apps in host1 and 1 in host3
		So(endpoints, ShouldHaveLength, 4)

		scotty2, store := endpointStore.ByHostAndName("host2", "scotty")
		So(scotty2.M.Active, ShouldBeFalse)
		So(scotty2.App.Active, ShouldBeTrue)
		So(store.IsEndpointActive(scotty2.App.EP), ShouldBeFalse)

		healthAgent2, store = endpointStore.ByHostAndName(
			"host2", application.HealthAgentName)
		So(healthAgent2.M.Active, ShouldBeFalse)
		So(healthAgent2.App.Active, ShouldBeTrue)
		So(store.IsEndpointActive(healthAgent2.App.EP), ShouldBeFalse)

		healthAgent3, store := endpointStore.ByHostAndName(
			"host3", application.HealthAgentName)
		So(healthAgent3.M.Active, ShouldBeTrue)
		So(healthAgent3.App.Active, ShouldBeTrue)
		So(store.IsEndpointActive(healthAgent3.App.EP), ShouldBeTrue)

		healthAgent1, store = endpointStore.ByHostAndName(
			"host1", application.HealthAgentName)
		So(healthAgent1.M.Region, ShouldEqual, "us-east-2")

		endpointStore.UpdateEndpoints(
			100.0,
			map[string]machine.EndpointObservation{
				"host1": {
					SeqNo: 2,
					Endpoints: namesandports.NamesAndPorts{
						"scotty": {Port: 6990, IsTLS: true}, // changed from 6980. subd gone
					},
				},
				"host2": {
					SeqNo: 2,
					Endpoints: namesandports.NamesAndPorts{
						"scotty":    {Port: 6980},
						"dominator": {Port: 6970},
					},
				},
			})

		endpoints, store = endpointStore.AllActiveWithStore()
		So(endpoints, ShouldHaveLength, 3)

		scotty1, store = endpointStore.ByHostAndName("host1", "scotty")
		So(scotty1.App.Port, ShouldEqual, 6990)
		So(scotty1.App.IsTLS, ShouldBeTrue)
		So(store.IsEndpointActive(scotty1.App.EP), ShouldBeTrue)

		subd1, store := endpointStore.ByHostAndName("host1", "subd")
		So(subd1.App.Active, ShouldBeFalse)
		So(subd1.M.Active, ShouldBeTrue)
		So(store.IsEndpointActive(subd1.App.EP), ShouldBeFalse)

		// now host1 and host2 are active while host3 is not
		endpointStore.UpdateMachines(
			100.0,
			[]mdb.Machine{
				{
					Hostname: "host1",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId: "12349",
					},
				},
				{
					Hostname: "host2",
					AwsMetadata: &mdb.AwsMetadata{
						AccountId: "66666",
					},
				},
			})
		endpoints, store = endpointStore.AllActiveWithStore()
		// host1 has 2 active apps, while host2 has 3
		So(endpoints, ShouldHaveLength, 5)
		healthAgent1, store = endpointStore.ByHostAndName(
			"host1", application.HealthAgentName)
		So(healthAgent1.M.Aws.AccountId, ShouldEqual, "12349")
		dominator2, store = endpointStore.ByHostAndName("host2", "dominator")
		So(dominator2.M.Active, ShouldBeTrue)
		So(dominator2.App.Active, ShouldBeTrue)
		So(store.IsEndpointActive(dominator2.App.EP), ShouldBeTrue)
		healthAgent3, store = endpointStore.ByHostAndName(
			"host3", application.HealthAgentName)
		So(healthAgent3.M.Active, ShouldBeFalse)
		So(store.IsEndpointActive(healthAgent3.App.EP), ShouldBeFalse)

		// Change apps on host1
		endpointStore.UpdateEndpoints(
			100.0,
			map[string]machine.EndpointObservation{
				"host1": {
					SeqNo: 3,
					Endpoints: namesandports.NamesAndPorts{
						"scotty": {Port: 6990},
						"subd":   {Port: 6912}, // subd back
					},
				},
				"host2": {
					SeqNo: 3,
					Endpoints: namesandports.NamesAndPorts{
						"scotty":    {Port: 6980},
						"dominator": {Port: 6970},
					},
				},
			})

		endpoints, store = endpointStore.AllActiveWithStore()
		So(endpoints, ShouldHaveLength, 6)

		subd1, store = endpointStore.ByHostAndName("host1", "subd")
		So(subd1.M.Active, ShouldBeTrue)
		So(subd1.App.Active, ShouldBeTrue)
		So(store.IsEndpointActive(subd1.App.EP), ShouldBeTrue)

		endpointStore.UpdateEndpoints(
			100.0,
			map[string]machine.EndpointObservation{
				"host1": {
					SeqNo:     3,
					Endpoints: namesandports.NamesAndPorts{},
				},
				"host2": {
					SeqNo:     3,
					Endpoints: namesandports.NamesAndPorts{},
				},
			})
		endpoints, store = endpointStore.AllActiveWithStore()
		So(endpoints, ShouldHaveLength, 6)

	})
}
