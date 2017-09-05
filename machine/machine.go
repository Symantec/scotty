package machine

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/hostid"
	"github.com/Symantec/scotty/store"
	"time"
)

type machineDataType struct {
	M     Machine
	Group *application.Group
	SeqNo uint64
}

func (e *EndpointStore) updateMachines(
	timestamp float64,
	activeHosts []mdb.Machine) {
	e.statusChangeLock.Lock()
	defer e.statusChangeLock.Unlock()
	active, inactive, astore := e._updateMachines(activeHosts)
	for _, ep := range active {
		astore.MarkEndpointActive(ep)
	}
	for _, ep := range inactive {
		astore.MarkEndpointInactive(timestamp, ep)
	}
}

func (e *EndpointStore) updateEndpoints(
	timestamp float64, endpoints map[string]EndpointObservation) {
	e.statusChangeLock.Lock()
	defer e.statusChangeLock.Unlock()
	active, inactive, astore := e._updateEndpoints(endpoints)
	for _, ep := range active {
		astore.MarkEndpointActive(ep)
	}
	for _, ep := range inactive {
		astore.MarkEndpointInactive(timestamp, ep)
	}
}

func (e *EndpointStore) _updateMachines(
	activeHosts []mdb.Machine) (
	active, inactive []*scotty.Endpoint, astore *store.Store) {
	activeHostSet := newStringSet(activeHosts)
	e.mu.Lock()
	defer e.mu.Unlock()
	// Mark everything that is not part of the new list as inactive
	for _, md := range e.byHost {
		if !activeHostSet[md.M.Host] {
			md.M.Active = false
			// Mark active apps as inactive
			for _, app := range md.Group.Applications() {
				if app.Active {
					inactive = append(inactive, app.EP)
				}
			}
		}
	}
	storeCopy := e.astore
	for _, ahost := range activeHosts {
		lookedUpHost := e.byHost[ahost.Hostname]
		if lookedUpHost == nil {
			// A new machine
			var m machineDataType
			m.M.Host = ahost.Hostname
			m.M.Active = true
			m.M.Aws = e.config.GetAwsInfo(ahost.AwsMetadata)
			var ep *scotty.Endpoint
			m.Group, ep = application.NewGroup(
				&hostid.HostID{
					HostName:  ahost.Hostname,
					IPAddress: ahost.IpAddress},
				e.countToInactivate)
			if storeCopy == e.astore {
				storeCopy = e.astore.ShallowCopy()
			}
			storeCopy.RegisterEndpoint(ep)
			e.byHost[ahost.Hostname] = &m
		} else {
			lookedUpHost.M.Aws = e.config.GetAwsInfo(ahost.AwsMetadata)
			if !lookedUpHost.M.Active {
				lookedUpHost.M.Active = true
				for _, app := range lookedUpHost.Group.Applications() {
					if app.Active {
						active = append(active, app.EP)
					}
				}
			}
		}
	}
	e.astore = storeCopy
	astore = e.astore
	return
}

func (e *EndpointStore) _updateEndpoints(
	endpoints map[string]EndpointObservation) (
	active, inactive []*scotty.Endpoint, astore *store.Store) {
	e.mu.Lock()
	defer e.mu.Unlock()
	storeCopy := e.astore
	for hostName, md := range e.byHost {
		eo, ok := endpoints[hostName]
		if !ok || md.SeqNo >= eo.SeqNo {
			continue
		}
		md.SeqNo = eo.SeqNo
		newep, activeep, inactiveep := md.Group.SetApplications(
			eo.Endpoints)
		if len(newep) > 0 {
			if storeCopy == e.astore {
				storeCopy = e.astore.ShallowCopy()
			}
			for _, ep := range newep {
				storeCopy.RegisterEndpoint(ep)
			}
		}
		active = append(active, activeep...)
		inactive = append(inactive, inactiveep...)
	}
	e.astore = storeCopy
	astore = e.astore
	return
}

func (e *EndpointStore) byHostAndName(
	host, name string) (*Endpoint, *store.Store) {
	e.mu.Lock()
	defer e.mu.Unlock()
	md := e.byHost[host]
	if md == nil {
		return nil, e.astore
	}
	app := md.Group.ByName(name)
	if app == nil {
		return nil, e.astore
	}
	machineCopy := md.M
	return &Endpoint{M: &machineCopy, App: app}, e.astore
}

func (e *EndpointStore) allWithStore() (
	result []*Endpoint, astore *store.Store) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, md := range e.byHost {
		machineCopy := md.M
		for _, app := range md.Group.Applications() {
			result = append(
				result,
				&Endpoint{M: &machineCopy, App: app})
		}
	}
	astore = e.astore
	return
}

func (e *EndpointStore) allActiveWithStore() (
	result []*Endpoint, astore *store.Store) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, md := range e.byHost {
		if !md.M.Active {
			continue
		}
		machineCopy := md.M
		for _, app := range md.Group.Applications() {
			if !app.Active {
				continue
			}
			result = append(
				result,
				&Endpoint{M: &machineCopy, App: app})
		}
	}
	astore = e.astore
	return
}

func (e *EndpointStore) store() *store.Store {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.astore
}

func (e *EndpointStore) update(
	endpoint *scotty.Endpoint,
	updater func(*application.EndpointStats)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	md := e.byHost[endpoint.HostName()]
	if md != nil {
		md.Group.Modify(endpoint.AppName(), updater)
	}
}

func (e *EndpointStore) logChangedMetricCount(
	endpoint *scotty.Endpoint, metricCount uint) {
	e.update(
		endpoint,
		func(es *application.EndpointStats) {
			if es.InitialMetricCount == 0 {
				es.InitialMetricCount = uint64(metricCount)
			} else {
				es.ChangedMetricsSum += uint64(metricCount)
				es.ChangedMetricsCount++
			}
		})
}

func (e *EndpointStore) updateState(
	ep *scotty.Endpoint, newState *scotty.State) {
	e.update(
		ep,
		func(es *application.EndpointStats) {
			es.Status = newState.Status()
			if es.Status == scotty.Synced {
				es.PollTime = newState.TimeSpentPolling()
				es.LastReadTime = newState.Timestamp()
			}
		})
}

func (e *EndpointStore) reportError(
	ep *scotty.Endpoint, err error, ts time.Time) {
	e.update(
		ep,
		func(es *application.EndpointStats) {
			if err != nil {
				es.LastError = err
				es.LastErrorTime = ts
				es.Down = true
			} else {
				es.Down = false
			}
		})
}

func newStringSet(activeHosts []mdb.Machine) (result map[string]bool) {
	result = make(map[string]bool)
	for i := range activeHosts {
		result[activeHosts[i].Hostname] = true
	}
	return
}

type byHostAndName []*Endpoint

func (b byHostAndName) Len() int { return len(b) }

func (b byHostAndName) Less(i, j int) bool {
	ihostname := b[i].App.EP.HostName()
	jhostname := b[j].App.EP.HostName()
	if ihostname < jhostname {
		return true
	} else if jhostname < ihostname {
		return false
	} else if b[i].App.EP.AppName() < b[j].App.EP.AppName() {
		return true
	}
	return false
}

func (b byHostAndName) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}
