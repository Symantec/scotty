package tsdb

import (
	"bytes"
	"github.com/Symantec/scotty/pstore/config"
	"reflect"
	"testing"
	"time"
)

func TestInfluxConfig(t *testing.T) {
	configFile := `
hostAndPort: localhost:8085
timeout: 35s
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := config.Read(buffer, &aconfig); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		HostAndPort: "localhost:8085",
		Timeout:     35 * time.Second,
	}
	if !reflect.DeepEqual(expected, aconfig) {
		t.Errorf("Expected %v, got %v", expected, aconfig)
	}
}
