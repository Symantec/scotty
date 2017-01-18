package datastructs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/jsonsource"
	"github.com/Symantec/scotty/sources/snmpsource"
	"github.com/Symantec/scotty/sources/trisource"
	"github.com/Symantec/scotty/store"
	"gopkg.in/yaml.v2"
	"io"
	"time"
)

type byHostAndName []*ApplicationStatus

func (b byHostAndName) Len() int { return len(b) }

func (b byHostAndName) Less(i, j int) bool {
	ihostname := b[i].EndpointId.HostName()
	jhostname := b[j].EndpointId.HostName()
	if ihostname < jhostname {
		return true
	} else if jhostname < ihostname {
		return false
	} else if b[i].Name < b[j].Name {
		return true
	}
	return false
}

func (b byHostAndName) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

type protocolType func(map[string]string) (sources.ConnectorList, error)

func newTricorder(unused map[string]string) (sources.ConnectorList, error) {
	return sources.ConnectorList{
		trisource.GetConnector(), jsonsource.GetConnector()}, nil
}

func newSnmp(params map[string]string) (sources.ConnectorList, error) {
	community := params["community"]
	if community == "" {
		return nil, errors.New("parameter 'community' required for SNMP")
	}
	return sources.ConnectorList{snmpsource.NewConnector(community)}, nil
}

var (
	kProtocols = map[string]protocolType{
		"tricorder": newTricorder,
		"snmp":      newSnmp,
	}
)

type hostAndPort struct {
	Host string
	Port uint
}

func (a *ApplicationStatuses) newApplicationStatus(
	e *scotty.Endpoint, instanceId string) *ApplicationStatus {
	app := a.appList.ByPort(e.Port())
	if app == nil {
		panic("Oops, unknown port in endpoint.")
	}
	return &ApplicationStatus{
		EndpointId: e,
		Name:       app.Name(),
		Active:     true,
		InstanceId: instanceId}
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
		record.LastReadTime = newState.Timestamp()
	}
}

func (a *ApplicationStatuses) reportError(
	e *scotty.Endpoint, err error, ts time.Time) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		panic("Unknown endpoint in ReportError.")
	}
	if err != nil {
		record.LastError = err
		record.LastErrorTime = ts
		record.Down = true
	} else {
		record.Down = false
	}
}

func newStringSet(activeHosts []mdb.Machine) (result map[string]bool) {
	result = make(map[string]bool)
	for i := range activeHosts {
		result[activeHosts[i].Hostname] = true
	}
	return
}

func (a *ApplicationStatuses) _markHostsActiveExclusively(
	activeHosts []mdb.Machine) (
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
				Host: activeHosts[i].Hostname,
				Port: apps[j].Port(),
			}
			activeEndpoint := a.byHostPort[hp]

			// If new endpoint
			if activeEndpoint == nil {
				activeEndpoint = scotty.NewEndpointWithConnector(
					hp.Host, hp.Port, apps[j].Connectors())
				a.byHostPort[hp] = activeEndpoint
				var instanceId string
				if activeHosts[i].AwsMetadata != nil {
					instanceId = activeHosts[i].AwsMetadata.InstanceId
				}
				a.byEndpoint[activeEndpoint] = a.newApplicationStatus(
					activeEndpoint, instanceId)
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
	timestamp float64, activeHosts []mdb.Machine) {
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
	e *scotty.Endpoint, metricCount uint) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		panic("Unknown endpoint in LogChangedMetricCount.")
	}
	if record.InitialMetricCount == 0 {
		record.InitialMetricCount = metricCount
	} else {
		record.changedMetrics_Sum += uint64(metricCount)
		record.changedMetrics_Count++
	}
}

func (a *ApplicationStatuses) _all(
	filter func(*ApplicationStatus) bool) (result []*ApplicationStatus) {
	for _, val := range a.byEndpoint {
		if filter == nil || filter(val) {
			acopy := *val
			result = append(result, &acopy)
		}
	}
	return
}

func (a *ApplicationStatuses) all() (result []*ApplicationStatus) {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a._all(nil)
}

func (a *ApplicationStatuses) allWithStore(
	filter func(*ApplicationStatus) bool) (
	result []*ApplicationStatus, astore *store.Store) {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a._all(filter), a.currentStore
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

func (a *ApplicationStatuses) byEndpointId(
	e *scotty.Endpoint) (copyOfStatus *ApplicationStatus) {
	a.lock.Lock()
	defer a.lock.Unlock()
	status := a.byEndpoint[e]
	if status == nil {
		return
	}
	acopy := *status
	return &acopy
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
		byPort: make(map[uint]*Application),
		byName: make(map[string]*Application),
	}
	return &ApplicationListBuilder{listPtr: &list}
}

func (a *ApplicationListBuilder) add(
	port uint, applicationName string, connectors sources.ConnectorList) {
	if (*a.listPtr).byPort[port] != nil || (*a.listPtr).byName[applicationName] != nil {
		panic("Both name and port must be unique.")
	}
	app := &Application{
		name: applicationName, port: port, connectors: connectors}
	(*a.listPtr).byPort[port] = app
	(*a.listPtr).byName[applicationName] = app
}

func (a *ApplicationListBuilder) build() *ApplicationList {
	result := *a.listPtr
	*a.listPtr = nil
	return result
}

type configLineType struct {
	Name     string
	Protocol string
	Port     uint
	Params   map[string]string
}

func (c *configLineType) UnmarshalYAML(
	unmarshal func(interface{}) error) error {
	type configLineFieldsType configLineType
	return yamlutil.StrictUnmarshalYAML(
		unmarshal, (*configLineFieldsType)(c))
}

func (a *ApplicationListBuilder) readConfig(r io.Reader) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	var configLines []configLineType
	if err := yaml.Unmarshal(content.Bytes(), &configLines); err != nil {
		return err
	}
	for _, configLine := range configLines {
		if configLine.Name == "" || configLine.Port == 0 {
			return errors.New(
				"Both name and port required for each application")
		}
		protocol := kProtocols[configLine.Protocol]
		if protocol == nil {
			return errors.New(
				fmt.Sprintf(
					"Unrecognized protocol '%s'",
					configLine.Protocol))
		}
		connectors, err := protocol(configLine.Params)
		if err != nil {
			return err
		}
		a.Add(
			configLine.Port,
			configLine.Name,
			connectors)
	}
	return nil
}
