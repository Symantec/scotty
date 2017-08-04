package application

import (
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/namesandports"
	"github.com/Symantec/scotty/sources/trisource"
)

var (
	kConnector = trisource.GetConnector()
)

type applicationDataType struct {
	A             Application
	InactiveCount int
}

func newGroup(host string, countToInactivate int) (*Group, *scotty.Endpoint) {
	ep := scotty.NewEndpointWithConnector(
		host, HealthAgentName, kConnector)
	appData := &applicationDataType{
		A: Application{
			EP:     ep,
			Port:   HealthAgentPort,
			Active: true},
	}
	apps := make(map[string]*applicationDataType)
	apps[HealthAgentName] = appData
	return &Group{
		host:              host,
		apps:              apps,
		countToInactivate: countToInactivate}, ep
}

func (g *Group) modify(name string, mod func(*EndpointStats)) {
	appData := g.apps[name]
	if appData != nil {
		mod(&appData.A.EndpointStats)
	}
}

func (g *Group) byName(name string) *Application {
	appData := g.apps[name]
	if appData == nil {
		return nil
	}
	acopy := appData.A
	return &acopy
}

func (g *Group) applications() (result []*Application) {
	result = make([]*Application, 0, len(g.apps))
	for _, appData := range g.apps {
		acopy := appData.A
		result = append(result, &acopy)
	}
	return
}

func (g *Group) setApplications(namesAndPorts namesandports.NamesAndPorts) (
	newApps, active, inactive []*scotty.Endpoint) {
	for name, port := range namesAndPorts {
		if name == HealthAgentName {
			continue
		}
		appData := g.apps[name]
		if appData == nil {
			ep := scotty.NewEndpointWithConnector(
				g.host, name, kConnector)
			appData := &applicationDataType{
				A: Application{
					EP:     ep,
					Port:   port,
					Active: true,
				},
			}
			g.apps[name] = appData
			newApps = append(newApps, ep)
		} else if !appData.A.Active {
			appData.A.Active = true
			appData.InactiveCount = 0
			appData.A.Port = port
			active = append(active, appData.A.EP)
		} else {
			appData.InactiveCount = 0
			appData.A.Port = port
		}
	}
	// inactivate apps
	for name, appData := range g.apps {
		if name == HealthAgentName {
			continue
		}
		if _, ok := namesAndPorts[name]; !ok {
			appData.InactiveCount++
			if appData.InactiveCount >= g.countToInactivate {
				appData.A.Active = false
				appData.InactiveCount = 0
				inactive = append(inactive, appData.A.EP)
			}
		}
	}
	return
}
