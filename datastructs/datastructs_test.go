package datastructs

import (
	"bytes"
	"fmt"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/snmpsource"
	"github.com/Symantec/scotty/sources/trisource"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder"
	"math"
	"reflect"
	"sort"
	"testing"
)

const (
	kConfigFile = `
- port: 6910
  name: Health Metrics
  protocol: tricorder
- port: 6970
  name: Dominator
  protocol: snmp
  params:
    community: Groovy
- port: 2222
  name: An application
  protocol: tricorder
`
	kSomeBadPort = 9876
	kSomeBadName = "A bad name"
)

func TestConfigFile(t *testing.T) {
	config := bytes.NewBuffer(([]byte)(kConfigFile))
	builder := NewApplicationListBuilder()
	if err := builder.ReadConfig(config); err != nil {
		t.Fatal("Got error reading config file", err)
	}
	applicationList := builder.Build()
	assertApplication(
		t,
		6910,
		"Health Metrics",
		"tricorder",
		applicationList.ByPort(6910))
	assertApplication(
		t, 6970, "Dominator", "snmp", applicationList.ByPort(6970))
	assertApplication(
		t,
		2222,
		"An application",
		"tricorder",
		applicationList.ByPort(2222))

	assertApplication(
		t,
		6910,
		"Health Metrics",
		"tricorder",
		applicationList.ByName("Health Metrics"))
	assertApplication(
		t,
		6970,
		"Dominator",
		"snmp",
		applicationList.ByName("Dominator"))
	assertApplication(
		t,
		2222,
		"An application",
		"tricorder",
		applicationList.ByName("An application"))
	if applicationList.ByPort(kSomeBadPort) != nil {
		t.Error("Expected no application at given port.")
	}
	if applicationList.ByName(kSomeBadName) != nil {
		t.Error("Expected no application at given name.")
	}
	applications := applicationList.All()
	if len(applications) != 3 {
		t.Error("Expected 3 applications.")
	}
	portAndName := make(map[uint]string)
	for _, app := range applications {
		portAndName[app.Port()] = app.Name()
	}
	expected := map[uint]string{
		2222: "An application",
		6910: "Health Metrics",
		6970: "Dominator",
	}
	if !reflect.DeepEqual(expected, portAndName) {
		t.Errorf("Expected %v, got %v", expected, portAndName)
	}
}

type activeInactiveListType struct {
	Active      map[string]bool
	Inactive    map[string]bool
	activeFound bool
}

func newActiveInactiveLists() *activeInactiveListType {
	return &activeInactiveListType{
		Active:   make(map[string]bool),
		Inactive: make(map[string]bool),
	}
}

func (a *activeInactiveListType) Append(r *store.Record) bool {
	if r.Active {
		a.activeFound = true
		return false
	}
	return true
}

func (a *activeInactiveListType) Visit(
	s *store.Store, endpoint interface{}) error {
	a.activeFound = false
	s.LatestByEndpoint(endpoint, a)
	scottyEndpoint := endpoint.(*scotty.Endpoint)
	hostPortStr := fmt.Sprintf(
		"%s:%d", scottyEndpoint.HostName(), scottyEndpoint.Port())
	if a.activeFound {
		a.Active[hostPortStr] = true
	} else {
		a.Inactive[hostPortStr] = true
	}
	return nil
}

func activateEndpoints(endpoints []*scotty.Endpoint, s *store.Store) {
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo/first",
			Description: "A description",
		},
		{
			Path:        "/foo/second",
			Description: "A description",
		},
		{
			Path:        "/foo/third",
			Description: "A description",
		},
		{
			Path:        "/foo/fourth",
			Description: "A description",
		},
	}
	for i := range aMetric {
		aMetric[i].Value = int64(i)
	}
	for i := range endpoints {
		s.AddBatch(endpoints[i], 1.0, aMetric[:])
	}
}

func newStore(
	t *testing.T,
	testName string,
	valueCount,
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *store.Store {
	result := store.NewStore(
		valueCount, pageCount, inactiveThreshhold, degree)
	dirSpec, err := tricorder.RegisterDirectory("/" + testName)
	if err != nil {
		t.Fatalf("Duplicate test: %s", testName)
	}
	result.RegisterMetrics(dirSpec)
	return result
}

func TestMarkHostsActiveExclusively(t *testing.T) {
	alBuilder := NewApplicationListBuilder()
	alBuilder.Add(
		35, "AnApp", sources.ConnectorList{trisource.GetConnector()})
	alBuilder.Add(
		92,
		"AnotherApp",
		sources.ConnectorList{snmpsource.NewConnector("community")})
	appList := alBuilder.Build()
	appStatus := NewApplicationStatuses(
		appList,
		newStore(t, "TestMarkHostsActiveExclusively", 1, 100, 1.0, 10))
	appStatus.MarkHostsActiveExclusively(
		92.5,
		[]string{"host1", "host2", "host3"})
	activateEndpoints(appStatus.ActiveEndpointIds())
	astore := appStatus.Store()
	visitor := newActiveInactiveLists()
	astore.VisitAllEndpoints(visitor)
	assertDeepEqual(
		t,
		map[string]bool{
			"host1:35": true,
			"host1:92": true,
			"host2:35": true,
			"host2:92": true,
			"host3:35": true,
			"host3:92": true,
		},
		visitor.Active)
	assertDeepEqual(
		t,
		make(map[string]bool),
		visitor.Inactive)
	appStatus.MarkHostsActiveExclusively(61.7, []string{"host2", "host4"})
	activeEndpointIds, astore := appStatus.ActiveEndpointIds()
	assertValueEquals(t, 4, len(activeEndpointIds))
	activateEndpoints(activeEndpointIds, astore)
	visitor = newActiveInactiveLists()
	astore.VisitAllEndpoints(visitor)
	assertDeepEqual(
		t,
		map[string]bool{
			"host2:35": true,
			"host2:92": true,
			"host4:35": true,
			"host4:92": true,
		},
		visitor.Active)
	assertDeepEqual(
		t,
		map[string]bool{
			"host1:35": true,
			"host1:92": true,
			"host3:35": true,
			"host3:92": true,
		},
		visitor.Inactive)

	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	assertValueEquals(t, "host3", endpointId.HostName())
	assertValueEquals(t, uint(35), endpointId.Port())

	// Trying to add to inactive endpoint should fail
	if _, err := aStore.AddBatch(endpointId, 9999.0, metrics.SimpleList(nil)); err != store.ErrInactive {
		t.Error("Adding to inactive endpoint should fail.")
	}

	stats := appStatus.All()
	sort.Sort(sortByHostPort(stats))
	assertValueEquals(t, 8, len(stats))
	assertValueEquals(t, "host1", stats[0].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[0].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[0].Name)
	assertValueEquals(t, false, stats[0].Active)

	assertValueEquals(t, "host1", stats[1].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[1].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[1].Name)
	assertValueEquals(t, false, stats[1].Active)

	assertValueEquals(t, "host2", stats[2].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[2].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[2].Name)
	assertValueEquals(t, true, stats[2].Active)

	assertValueEquals(t, "host2", stats[3].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[3].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[3].Name)
	assertValueEquals(t, true, stats[3].Active)

	assertValueEquals(t, "host3", stats[4].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[4].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[4].Name)
	assertValueEquals(t, false, stats[4].Active)

	assertValueEquals(t, "host3", stats[5].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[5].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[5].Name)
	assertValueEquals(t, false, stats[5].Active)

	assertValueEquals(t, "host4", stats[6].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[6].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[6].Name)
	assertValueEquals(t, true, stats[6].Active)

	assertValueEquals(t, "host4", stats[7].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[7].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[7].Name)
	assertValueEquals(t, true, stats[7].Active)

	appStatus.MarkHostsActiveExclusively(61.7, []string{"host2", "host3", "host4"})

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")

	var noMetrics metrics.SimpleList

	// Trying to add to active endpoint should succeed
	if _, err := aStore.AddBatch(endpointId, 9999.0, noMetrics); err != nil {
		t.Error("Adding to active endpoint should succeed.")
	}

	stats = appStatus.All()
	sort.Sort(sortByHostPort(stats))
	assertValueEquals(t, 8, len(stats))
	assertValueEquals(t, "host1", stats[0].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[0].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[0].Name)
	assertValueEquals(t, false, stats[0].Active)

	assertValueEquals(t, "host1", stats[1].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[1].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[1].Name)
	assertValueEquals(t, false, stats[1].Active)

	assertValueEquals(t, "host2", stats[2].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[2].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[2].Name)
	assertValueEquals(t, true, stats[2].Active)

	assertValueEquals(t, "host2", stats[3].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[3].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[3].Name)
	assertValueEquals(t, true, stats[3].Active)

	assertValueEquals(t, "host3", stats[4].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[4].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[4].Name)
	assertValueEquals(t, true, stats[4].Active)

	assertValueEquals(t, "host3", stats[5].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[5].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[5].Name)
	assertValueEquals(t, true, stats[5].Active)

	assertValueEquals(t, "host4", stats[6].EndpointId.HostName())
	assertValueEquals(
		t, "tricorder", stats[6].EndpointId.ConnectorName())
	assertValueEquals(t, "AnApp", stats[6].Name)
	assertValueEquals(t, true, stats[6].Active)

	assertValueEquals(t, "host4", stats[7].EndpointId.HostName())
	assertValueEquals(
		t, "snmp", stats[7].EndpointId.ConnectorName())
	assertValueEquals(t, "AnotherApp", stats[7].Name)
	assertValueEquals(t, true, stats[7].Active)

}

func TestHighPriorityEviction(t *testing.T) {
	alBuilder := NewApplicationListBuilder()
	alBuilder.Add(37, "AnApp", sources.ConnectorList{trisource.GetConnector()})
	appList := alBuilder.Build()
	// 9 values
	appStatus := NewApplicationStatuses(
		appList,
		newStore(
			t, "TestHighPriorityEviction", 1, 6, 1.0, 10))
	// host1, host2, host3 marked active
	// 2 values to AnApp:/foo on host1, host2 and host3
	// 3rd value to anApp:/foo on host1, host2 only
	// host1 and host2 only marked active giving host3:AnApp:/foo
	// an inactive marker as third value
	// 4th value added to AnApp:/foo on host1 and host2 only
	addDataForHighPriorityEvictionTest(appStatus)

	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")

	var result []store.Record
	aStore.ByNameAndEndpoint(
		"/foo",
		endpointId,
		0.0,
		math.Inf(0),
		store.AppendTo(&result))

	// Since the high priority threshhold is 100%,
	// we don't expect all the
	// pages of host3:AnApp:/foo to get reclaimed.
	if len(result) <= 1 {
		t.Error("Expected host3:AnApp:/foo to have at least 2 values")
	}

	// 9 values
	appStatus = NewApplicationStatuses(
		appList,
		newStore(t, "TestHighPriorityEviction2", 1, 6, 0.0, 10))
	// host1, host2, host3 marked active
	// 3 values to AnApp:/foo on host1, host2 and host3
	// host1 and host2 only marked active
	// 4th value added to AnApp:/foo on host1 and host2 only
	addDataForHighPriorityEvictionTest(appStatus)

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")

	result = nil
	aStore.ByNameAndEndpoint(
		"/foo",
		endpointId,
		0.0,
		math.Inf(0),
		store.AppendTo(&result))

	// Since the high priority threshhold is 0%,
	// we do expect all the pages for host3:Anpp:/foo to get reclaimed
	if len(result) > 1 {
		t.Error("Expected host3:AnApp:/foo to have only 1 value")
	}
}

func addDataForHighPriorityEvictionTest(appStatus *ApplicationStatuses) {
	aMetric := metrics.SimpleList{
		{
			Path:        "/foo",
			Description: "A description",
		},
	}
	appStatus.MarkHostsActiveExclusively(
		10.0,
		[]string{"host1", "host2", "host3"})

	aMetric[0].Value = int64(50)

	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	aMetric[0].Value = int64(55)

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	aMetric[0].Value = int64(60)

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 120.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 120.0, aMetric[:])

	appStatus.MarkHostsActiveExclusively(
		120.0,
		[]string{"host1", "host2"})

	aMetric[0].Value = int64(65)

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 130.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 130.0, aMetric[:])
}

func assertDeepEqual(
	t *testing.T,
	expected interface{},
	actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func assertValueEquals(
	t *testing.T,
	expected interface{},
	actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func assertApplication(
	t *testing.T,
	port uint,
	name string,
	protocol string,
	app *Application) {
	if name != app.Name() {
		t.Errorf("Expected '%s', got '%s'", name, app.Name())
	}
	if port != app.Port() {
		t.Errorf("Expected '%d', got '%d'", port, app.Port())
	}
	if protocol != app.Connectors()[0].Name() {
		t.Errorf(
			"Expected '%s', got '%s'",
			protocol,
			app.Connectors()[0].Name())
	}
}

type sortByHostPort []*ApplicationStatus

func (s sortByHostPort) Len() int { return len(s) }

func (s sortByHostPort) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortByHostPort) Less(i, j int) bool {
	if s[i].EndpointId.HostName() < s[j].EndpointId.HostName() {
		return true
	}
	if s[i].EndpointId.HostName() > s[j].EndpointId.HostName() {
		return false
	}
	if s[i].EndpointId.Port() < s[j].EndpointId.Port() {
		return true
	}
	return false
}
