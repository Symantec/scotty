package application_test

import (
	"fmt"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/hostid"
	"github.com/Symantec/scotty/namesandports"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestApi(t *testing.T) {
	Convey("Api testing", t, func() {
		group, healthAgentEp := application.NewGroup(&hostid.HostID{HostName: "ahost"}, 3)
		So(healthAgentEp.HostName(), ShouldEqual, "ahost")
		So(healthAgentEp.AppName(), ShouldEqual, application.HealthAgentName)
		newapps, active, inactive := group.SetApplications(
			namesandports.NamesAndPorts{
				"scotty":    6980,
				"dominator": 6970,
			})
		So(
			newapps,
			shouldHaveHostAndNames,
			"ahost",
			"scotty",
			"dominator")
		So(active, ShouldHaveLength, 0)
		So(inactive, ShouldHaveLength, 0)
		Convey("Applications works", func() {
			apps := group.Applications()
			So(apps, ShouldHaveLength, 3)
		})
		Convey("ByName works", func() {
			healthAgent := group.ByName(application.HealthAgentName)
			So(healthAgent.EP.HostName(), ShouldEqual, "ahost")
			So(healthAgent.EP.AppName(), ShouldEqual, application.HealthAgentName)
			So(healthAgent.Port, ShouldEqual, application.HealthAgentPort)
			So(healthAgent.Active, ShouldBeTrue)
		})
		Convey("ByName returns nil on not found", func() {
			So(group.ByName("does not exist"), ShouldBeNil)
		})
		Convey("Modify works", func() {
			group.Modify(
				"does not exist",
				func(stats *application.EndpointStats) {
					stats.Down = true
				},
			)
			group.Modify(
				"scotty",
				func(stats *application.EndpointStats) {
					stats.Down = true
				},
			)
			scotty := group.ByName("scotty")
			So(scotty.Down, ShouldBeTrue)
		})
		Convey("SetApplications works", func() {
			newApps, active, inactive := group.SetApplications(
				namesandports.NamesAndPorts{
					application.HealthAgentName: 7007,
					"scotty":                    6981,
					"new app":                   7000,
				})
			So(
				newApps,
				shouldHaveHostAndNames,
				"ahost",
				"new app",
			)
			So(active, ShouldHaveLength, 0)
			So(inactive, ShouldHaveLength, 0)

			newApps, active, inactive = group.SetApplications(
				namesandports.NamesAndPorts{
					application.HealthAgentName: 7007,
					"scotty":                    6981,
					"new app":                   7000,
				})
			So(newApps, ShouldHaveLength, 0)
			So(active, ShouldHaveLength, 0)
			So(inactive, ShouldHaveLength, 0)

			newApps, active, inactive = group.SetApplications(
				namesandports.NamesAndPorts{
					application.HealthAgentName: 7007,
					"scotty":                    6981,
					"new app":                   7000,
				})
			So(newApps, ShouldHaveLength, 0)
			So(active, ShouldHaveLength, 0)
			So(inactive,
				shouldHaveHostAndNames,
				"ahost",
				"dominator")
			Convey("port numbers update", func() {
				So(group.ByName("scotty").Port, ShouldEqual, 6981)
				So(
					group.ByName(application.HealthAgentName).Port,
					ShouldEqual,
					application.HealthAgentPort)
			})
			Convey("Active updates", func() {
				So(group.ByName("scotty").Active, ShouldBeTrue)
				So(group.ByName("dominator").Active, ShouldBeFalse)
			})
			Convey("Reactivating works", func() {
				newApps, active, inactive := group.SetApplications(
					namesandports.NamesAndPorts{
						"dominator": 6972,
					})
				So(newApps, ShouldHaveLength, 0)
				So(
					active,
					shouldHaveHostAndNames,
					"ahost",
					"dominator",
				)
				So(inactive, ShouldHaveLength, 0)
				So(group.ByName("dominator").Active, ShouldBeTrue)
				So(group.ByName("dominator").Port, ShouldEqual, 6972)
			})
		})
	})
}

func shouldHaveHostAndNames(
	endpointList interface{}, expected ...interface{}) string {
	hostName := expected[0].(string)
	names := make(map[string]bool)
	for _, name := range expected[1:] {
		names[name.(string)] = true
	}
	for _, ep := range endpointList.([]*scotty.Endpoint) {
		if ep.HostName() != hostName {
			return fmt.Sprintf("Expected host '%s' got '%s'", hostName, ep.HostName())
		}
		if _, ok := names[ep.AppName()]; !ok {
			return fmt.Sprintf("Did not expect app name '%s'", ep.AppName())
		}
		delete(names, ep.AppName())
	}
	if len(names) > 0 {
		return fmt.Sprintf("Expected app names: %v", names)
	}
	return ""
}
