package datastructs

import (
	"fmt"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"time"
)

func (h HostsAndPorts) copy() HostsAndPorts {
	result := make(HostsAndPorts, len(h))
	for key, value := range h {
		result[key] = value
	}
	return result
}

func (h HostsAndPorts) addIfAbsent(host string, port int) {
	hostAndPort := fmt.Sprintf("%s:%d", host, port)
	if h[hostAndPort] == nil {
		h[hostAndPort] = scotty.NewMachine(host, port)
	}
}

func (h HostsAndPorts) updateBuilder(builder *store.Builder) {
	for _, value := range h {
		builder.RegisterMachine(value)
	}
}

func (h *HostsPortsAndStore) get() (
	s *store.Store, hostsAndPorts HostsAndPorts) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.store, h.hostsAndPorts
}

func (h *HostsPortsAndStore) update(
	hostsAndPorts HostsAndPorts, builder *store.Builder) {
	hostsAndPorts.updateBuilder(builder)
	newStore := builder.Build()
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.hostsAndPorts = hostsAndPorts
	h.store = newStore
}

func newApplicationStatus(m *scotty.Machine) *ApplicationStatus {
	return &ApplicationStatus{MachineId: m}
}

func (a *ApplicationStatuses) update(
	m *scotty.Machine, newState *scotty.State) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byMachine[m]
	if record == nil {
		record = newApplicationStatus(m)
		a.byMachine[m] = record
	}
	record.Status = newState.Status()
	if record.Status == scotty.Synced {
		record.PollTime = newState.TimeSpentPolling()
		record.LastReadTime = time.Now()
	}
}

func (a *ApplicationStatuses) getAll(
	hostsAndPorts HostsAndPorts) (result []*ApplicationStatus) {
	result = make([]*ApplicationStatus, len(hostsAndPorts))
	a.lock.Lock()
	defer a.lock.Unlock()
	idx := 0
	for _, id := range hostsAndPorts {
		record := a.byMachine[id]
		if record == nil {
			result[idx] = newApplicationStatus(id)
		} else {
			acopy := *record
			result[idx] = &acopy
		}
		idx++
	}
	return
}
