package datastructs

import (
	"bytes"
	"reflect"
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

func assertApplication(
	t *testing.T, port int, name string, app *Application) {
	if name != app.Name() {
		t.Errorf("Expected '%s', got '%s'", name, app.Name())
	}
	if port != app.Port() {
		t.Errorf("Expected '%d', got '%d'", port, app.Port())
	}
}
