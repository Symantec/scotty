package datastructs

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"io"
	"strconv"
	"strings"
	"time"
)

func (h HostsAndPorts) copy() HostsAndPorts {
	result := make(HostsAndPorts, len(h))
	for key, value := range h {
		result[key] = value
	}
	return result
}

func (h HostsAndPorts) addIfAbsent(orig HostsAndPorts, host string, port int) {
	hostAndPort := fmt.Sprintf("%s:%d", host, port)
	origEndpoint := orig[hostAndPort]
	if origEndpoint == nil {
		h[hostAndPort] = scotty.NewEndpoint(host, port)
	} else {
		h[hostAndPort] = origEndpoint
	}
}

func (h HostsAndPorts) updateBuilder(builder *store.Builder) {
	for _, value := range h {
		builder.RegisterEndpoint(value)
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

func newApplicationStatus(e *scotty.Endpoint) *ApplicationStatus {
	return &ApplicationStatus{EndpointId: e}
}

func (a *ApplicationStatuses) update(
	e *scotty.Endpoint, newState *scotty.State) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		record = newApplicationStatus(e)
		a.byEndpoint[e] = record
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

func (a *ApplicationStatuses) logChangedMetricCount(
	e *scotty.Endpoint, metricCount int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	record := a.byEndpoint[e]
	if record == nil {
		record = newApplicationStatus(e)
		a.byEndpoint[e] = record
	}
	if record.InitialMetricCount == 0 {
		record.InitialMetricCount = metricCount
	} else {
		record.changedMetrics_Sum += int64(metricCount)
		record.changedMetrics_Count++
	}
}

func (a *ApplicationStatuses) getAll(
	hostsAndPorts HostsAndPorts) (result []*ApplicationStatus) {
	result = make([]*ApplicationStatus, len(hostsAndPorts))
	a.lock.Lock()
	defer a.lock.Unlock()
	idx := 0
	for _, id := range hostsAndPorts {
		record := a.byEndpoint[id]
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

func (a *ApplicationListBuilder) ReadConfig(r io.Reader) error {
	return a.readConfig(r)
}

func (a *ApplicationListBuilder) build() *ApplicationList {
	result := *a.listPtr
	*a.listPtr = nil
	return result
}

func (a *ApplicationListBuilder) readConfig(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}
		splits := strings.SplitN(line, "\t", 2)
		if len(splits) < 2 {
			return errors.New(
				"Config file lines must be portNum<tab>name")
		}
		port, err := strconv.Atoi(splits[0])
		if err != nil {
			return err
		}
		a.Add(port, splits[1])
	}
	return scanner.Err()
}
