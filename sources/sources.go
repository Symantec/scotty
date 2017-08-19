package sources

import (
	"github.com/Symantec/scotty/lib/preference"
	"strings"
	"sync"
)

type preferenceResourceType struct {
	Resources []Resource
	mu        sync.Mutex
	pref      *preference.Preference
}

func (p *preferenceResourceType) Indexes() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pref.Indexes()
}

func (p *preferenceResourceType) SetFirstIndex(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pref.SetFirstIndex(index)
}

type multiResourceConnectorType struct {
	conns                    []ResourceConnector
	consecutiveCallsForReset int
}

func (m *multiResourceConnectorType) NewResource(
	host string, port uint) Resource {
	resources := make([]Resource, len(m.conns))
	for i := range resources {
		resources[i] = m.conns[i].NewResource(host, port)
	}
	return &preferenceResourceType{
		Resources: resources,
		pref:      preference.New(len(m.conns), m.consecutiveCallsForReset),
	}
}

func (m *multiResourceConnectorType) ResourceConnect(r Resource) (
	poll Poller, err error) {
	resource := r.(*preferenceResourceType)
	for _, index := range resource.Indexes() {
		poll, err = m.conns[index].ResourceConnect(resource.Resources[index])
		if err == nil {
			resource.SetFirstIndex(index)
			return
		}
	}
	return
}

func (m *multiResourceConnectorType) Connect(host string, port uint) (
	Poller, error) {
	return m.ResourceConnect(m.NewResource(host, port))
}

func (m *multiResourceConnectorType) Name() string {
	names := make([]string, len(m.conns))
	for i := range names {
		names[i] = m.conns[i].Name()
	}
	return strings.Join(names, ",")
}

func multiResourceConnector(
	conns []Connector, consecutiveCallsForReset int) ResourceConnector {
	resourceConnectors := make([]ResourceConnector, len(conns))
	for i := range resourceConnectors {
		resourceConnectors[i] = simpleResourceConnector(conns[i])
	}
	return &multiResourceConnectorType{
		conns: resourceConnectors,
		consecutiveCallsForReset: consecutiveCallsForReset,
	}
}

type hostAndPort struct {
	Host string
	Port uint
}

type simpleResourceConnectorType struct {
	Connector
}

func (c *simpleResourceConnectorType) NewResource(
	host string, port uint) Resource {
	return &hostAndPort{Host: host, Port: port}
}

func (c *simpleResourceConnectorType) ResourceConnect(r Resource) (
	Poller, error) {
	hAndP := r.(*hostAndPort)
	return c.Connect(hAndP.Host, hAndP.Port)
}

func simpleResourceConnector(conn Connector) ResourceConnector {
	rc, ok := conn.(ResourceConnector)
	if ok {
		return rc
	}
	return &simpleResourceConnectorType{conn}
}
