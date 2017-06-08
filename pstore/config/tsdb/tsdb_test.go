package tsdb

import (
	"bytes"
	"github.com/Symantec/scotty/lib/yamlutil"
	"reflect"
	"testing"
)

func TestTsdbConfig(t *testing.T) {
	configFile := `
hostAndPort: localhost:8085
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := yamlutil.Read(buffer, &aconfig); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		HostAndPort: "localhost:8085",
	}
	if !reflect.DeepEqual(expected, aconfig) {
		t.Errorf("Expected %v, got %v", expected, aconfig)
	}
}

func TestTsdbConfigError(t *testing.T) {
	configFile := `
hostAndPort: localhost:8085
timeouts: 35s
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := yamlutil.Read(buffer, &aconfig); err == nil {
		t.Error("Expected error: misspelled fields timeouts")
	}
}
