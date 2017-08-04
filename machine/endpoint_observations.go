package machine

import (
	"github.com/Symantec/scotty/namesandports"
)

func (e *EndpointObservations) save(
	hostName string, namesAndPorts namesandports.NamesAndPorts) {
	e.mu.Lock()
	defer e.mu.Unlock()
	eo := e.data[hostName]
	eo.SeqNo += 1
	eo.Endpoints = namesAndPorts
	e.data[hostName] = eo
}

func (e *EndpointObservations) getAll() map[string]EndpointObservation {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make(map[string]EndpointObservation, len(e.data))
	for k, v := range e.data {
		result[k] = v
	}
	return result
}

func (e *EndpointObservations) maybeAddApp(
	hostName, appName string, port uint) {
	e.mu.Lock()
	defer e.mu.Unlock()
	eo := e.data[hostName]
	if eo.SeqNo > 0 && !eo.Endpoints.HasPort(port) {
		endpointsCopy := eo.Endpoints.Copy()
		endpointsCopy.Add(appName, port)
		eo.Endpoints = endpointsCopy
		e.data[hostName] = eo
	}
}
