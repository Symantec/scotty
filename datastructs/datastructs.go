package datastructs

import (
	"bytes"
	"errors"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"gopkg.in/yaml.v2"
	"io"
	"time"
)

type hostAndPort struct {
	Host string
	Port int
}

func (a *ApplicationStatuses) newApplicationStatus(
	e *scotty.Endpoint) *ApplicationStatus {
	app := a.appList.ByPort(e.Port())
	if app == nil {
		panic("Oops, unknown port in endpoint.")
	}
	return &ApplicationStatus{
		EndpointId: e, Name: app.Name(), Active: true}
}

func (a *ApplicationStatuses) store() *store.Store {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.currentStore
}

func (a *ApplicationStatuses) update(
	e *scotty.Endpoint, newState *scotty.State) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		panic("Unknown endpoint in Update.")
	}
	record.Status = newState.Status()
	if record.Status == scotty.Synced {
		record.PollTime = newState.TimeSpentPolling()
		record.LastReadTime = time.Now()
		record.Down = false
	} else if record.Status < 0 {
		record.Down = true
	}
}

func newStringSet(activeHosts []string) (result map[string]bool) {
	result = make(map[string]bool)
	for i := range activeHosts {
		result[activeHosts[i]] = true
	}
	return
}

func (a *ApplicationStatuses) _markHostsActiveExclusively(
	activeHosts []string) (
	toBecomeActive []*scotty.Endpoint,
	toBecomeInactive []*scotty.Endpoint,
	astore *store.Store) {
	activeHostSet := newStringSet(activeHosts)
	apps := a.appList.All()
	a.lock.Lock()
	defer a.lock.Unlock()
	storeCopy := a.currentStore
	// First mark everything that is not part of the new list of hosts
	// as inactive.
	for e, val := range a.byEndpoint {
		if !activeHostSet[e.HostName()] {
			// Make endpoint inactive
			if val.Active {
				val.Active = false
				toBecomeInactive = append(toBecomeInactive, e)
			}
		}
	}
	// Now mark new hosts active
	for i := range activeHosts {
		for j := range apps {
			hp := hostAndPort{
				Host: activeHosts[i],
				Port: apps[j].Port(),
			}
			activeEndpoint := a.byHostPort[hp]

			// If new endpoint
			if activeEndpoint == nil {
				activeEndpoint = scotty.NewEndpoint(
					hp.Host, hp.Port)
				a.byHostPort[hp] = activeEndpoint
				a.byEndpoint[activeEndpoint] = a.newApplicationStatus(activeEndpoint)
				if storeCopy == a.currentStore {
					storeCopy = a.currentStore.ShallowCopy()
				}
				storeCopy.RegisterEndpoint(activeEndpoint)
			} else {
				val := a.byEndpoint[activeEndpoint]
				if !val.Active {
					val.Active = true
					toBecomeActive = append(
						toBecomeActive, activeEndpoint)
				}
			}
		}
	}
	a.currentStore = storeCopy
	astore = a.currentStore
	return
}

func (a *ApplicationStatuses) markHostsActiveExclusively(
	timestamp float64, activeHosts []string) {
	a.statusChangeLock.Lock()
	defer a.statusChangeLock.Unlock()
	toBecomeActive, toBecomeInactive, astore := a._markHostsActiveExclusively(activeHosts)
	for i := range toBecomeInactive {
		astore.MarkEndpointInactive(
			timestamp, toBecomeInactive[i])
	}
	for i := range toBecomeActive {
		astore.MarkEndpointActive(toBecomeActive[i])
	}
}

func (a *ApplicationStatuses) logChangedMetricCount(
	e *scotty.Endpoint, metricCount int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		panic("Unknown endpoint in LogChangedMetricCount.")
	}
	if record.InitialMetricCount == 0 {
		record.InitialMetricCount = metricCount
	} else {
		record.changedMetrics_Sum += int64(metricCount)
		record.changedMetrics_Count++
	}
}

func (a *ApplicationStatuses) all() (result []*ApplicationStatus) {
	a.lock.Lock()
	defer a.lock.Unlock()
	result = make([]*ApplicationStatus, len(a.byEndpoint))
	idx := 0
	for _, val := range a.byEndpoint {
		acopy := *val
		result[idx] = &acopy
		idx++
	}
	return
}

func (a *ApplicationStatuses) endpointIdByHostAndName(host, name string) (
	id *scotty.Endpoint, astore *store.Store) {
	app := a.appList.ByName(name)
	a.lock.Lock()
	defer a.lock.Unlock()
	if app == nil {
		return nil, a.currentStore
	}
	hp := hostAndPort{
		Host: host,
		Port: app.Port(),
	}
	return a.byHostPort[hp], a.currentStore
}

func (a *ApplicationStatuses) activeEndpointIds() (
	activeEndpoints []*scotty.Endpoint, astore *store.Store) {
	a.lock.Lock()
	defer a.lock.Unlock()
	astore = a.currentStore
	for e, stat := range a.byEndpoint {
		if stat.Active {
			activeEndpoints = append(activeEndpoints, e)
		}
	}
	return
}

func (a *ApplicationList) all() (result []*Application) {
	result = make([]*Application, len(a.byPort))
	idx := 0
	for _, val := range a.byPort {
		result[idx] = val
		idx++
	}
	return
}

func newApplicationListBuilder() *ApplicationListBuilder {
	list := &ApplicationList{
		byPort: make(map[int]*Application),
		byName: make(map[string]*Application),
	}
	return &ApplicationListBuilder{listPtr: &list}
}

func (a *ApplicationListBuilder) add(port int, applicationName string) {
	if (*a.listPtr).byPort[port] != nil || (*a.listPtr).byName[applicationName] != nil {
		panic("Both name and port must be unique.")
	}
	app := &Application{name: applicationName, port: port}
	(*a.listPtr).byPort[port] = app
	(*a.listPtr).byName[applicationName] = app
}

func (a *ApplicationListBuilder) build() *ApplicationList {
	result := *a.listPtr
	*a.listPtr = nil
	return result
}

type nameAndPortType struct {
	Name string
	Port int
}

func (a *ApplicationListBuilder) readConfig(r io.Reader) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	var nameAndPorts []nameAndPortType
	if err := yaml.Unmarshal(content.Bytes(), &nameAndPorts); err != nil {
		return err
	}
	for _, nameAndPort := range nameAndPorts {
		if nameAndPort.Name == "" || nameAndPort.Port == 0 {
			return errors.New(
				"Both name and port required for each application")
		}
		a.Add(nameAndPort.Port, nameAndPort.Name)
	}
	return nil
}
