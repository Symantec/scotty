package datastructs

import (
	"bytes"
	"fmt"
	"github.com/Symantec/scotty"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"math"
	"reflect"
	"sort"
	"testing"
)

const (
	kConfigFile = `
- port: 6910
  name: Health Metrics
- port: 6970
  name: Dominator
- port: 2222
  name: An application
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
	assertApplication(t, 6910, "Health Metrics", applicationList.ByPort(6910))
	assertApplication(t, 6970, "Dominator", applicationList.ByPort(6970))
	assertApplication(t, 2222, "An application", applicationList.ByPort(2222))

	assertApplication(t, 6910, "Health Metrics", applicationList.ByName("Health Metrics"))
	assertApplication(t, 6970, "Dominator", applicationList.ByName("Dominator"))
	assertApplication(t, 2222, "An application", applicationList.ByName("An application"))
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
	portAndName := make(map[int]string)
	for _, app := range applications {
		portAndName[app.Port()] = app.Name()
	}
	expected := map[int]string{
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

func (a *activeInactiveListType) Append(r *store.Record) {
	if r.Active {
		a.activeFound = true
	}
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
	aMetric := [4]*messages.Metric{
		{
			Path:        "/foo/first",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/second",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/third",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
		{
			Path:        "/foo/fourth",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}
	for i := range aMetric {
		aMetric[i].Value = i
	}
	for i := range endpoints {
		s.AddBatch(endpoints[i], 1.0, aMetric[:])
	}
}

func TestMarkHostsActiveExclusively(t *testing.T) {
	alBuilder := NewApplicationListBuilder()
	alBuilder.Add(35, "AnApp")
	alBuilder.Add(92, "AnotherApp")
	appList := alBuilder.Build()
	appStatus := NewApplicationStatuses(appList, store.NewStore(1, 100, 1.0, 10))
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
	activateEndpoints(appStatus.ActiveEndpointIds())
	astore = appStatus.Store()
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
	endpointId, _ := appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	assertValueEquals(t, "host3", endpointId.HostName())
	assertValueEquals(t, 35, endpointId.Port())
	stats := appStatus.All()
	sort.Sort(sortByHostPort(stats))
	assertValueEquals(t, 8, len(stats))
	assertValueEquals(t, "host1", stats[0].EndpointId.HostName())
	assertValueEquals(t, "AnApp", stats[0].Name)
	assertValueEquals(t, false, stats[0].Active)

	assertValueEquals(t, "host1", stats[1].EndpointId.HostName())
	assertValueEquals(t, "AnotherApp", stats[1].Name)
	assertValueEquals(t, false, stats[1].Active)

	assertValueEquals(t, "host2", stats[2].EndpointId.HostName())
	assertValueEquals(t, "AnApp", stats[2].Name)
	assertValueEquals(t, true, stats[2].Active)

	assertValueEquals(t, "host2", stats[3].EndpointId.HostName())
	assertValueEquals(t, "AnotherApp", stats[3].Name)
	assertValueEquals(t, true, stats[3].Active)

	assertValueEquals(t, "host3", stats[4].EndpointId.HostName())
	assertValueEquals(t, "AnApp", stats[4].Name)
	assertValueEquals(t, false, stats[4].Active)

	assertValueEquals(t, "host3", stats[5].EndpointId.HostName())
	assertValueEquals(t, "AnotherApp", stats[5].Name)
	assertValueEquals(t, false, stats[5].Active)

	assertValueEquals(t, "host4", stats[6].EndpointId.HostName())
	assertValueEquals(t, "AnApp", stats[6].Name)
	assertValueEquals(t, true, stats[6].Active)

	assertValueEquals(t, "host4", stats[7].EndpointId.HostName())
	assertValueEquals(t, "AnotherApp", stats[7].Name)
	assertValueEquals(t, true, stats[7].Active)
}

func TestHighPriorityEviction(t *testing.T) {
	alBuilder := NewApplicationListBuilder()
	alBuilder.Add(37, "AnApp")
	appList := alBuilder.Build()
	// 9 values
	appStatus := NewApplicationStatuses(appList, store.NewStore(1, 6, 1.0, 10))
	// host1, host2, host3 marked active
	// 2 values to AnApp:/foo on host1, host2 and host3
	// 3rd value to anApp:/foo on host1, host2 only
	// host1 and host2 only marked active giving host3:AnApp:/foo
	// an inactive marker as third value
	// 4th value added to AnApp:/foo on host1 and host2 only
	addDataForHighPriorityEvictionTest(appStatus)

	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")

	var result []*store.Record
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
	appStatus = NewApplicationStatuses(appList, store.NewStore(1, 6, 0.0, 10))
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
	aMetric := [1]*messages.Metric{
		{
			Path:        "/foo",
			Description: "A description",
			Unit:        units.None,
			Kind:        types.Int64,
			Bits:        64,
		},
	}
	appStatus.MarkHostsActiveExclusively(
		10.0,
		[]string{"host1", "host2", "host3"})

	aMetric[0].Value = 50

	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	aStore.AddBatch(endpointId, 100.0, aMetric[:])

	aMetric[0].Value = 55

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	aStore.AddBatch(endpointId, 110.0, aMetric[:])

	aMetric[0].Value = 60

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	aStore.AddBatch(endpointId, 120.0, aMetric[:])

	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	aStore.AddBatch(endpointId, 120.0, aMetric[:])

	appStatus.MarkHostsActiveExclusively(
		120.0,
		[]string{"host1", "host2"})

	aMetric[0].Value = 65

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
	t *testing.T, port int, name string, app *Application) {
	if name != app.Name() {
		t.Errorf("Expected '%s', got '%s'", name, app.Name())
	}
	if port != app.Port() {
		t.Errorf("Expected '%d', got '%d'", port, app.Port())
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
