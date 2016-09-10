package tsdb

import (
	"bytes"
	"github.com/Symantec/scotty/pstore/config"
	"reflect"
	"testing"
	"time"
)

func TestTsdbConfig(t *testing.T) {
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

func TestTsdbConfigPlus(t *testing.T) {
	configFile := `
# A comment
writer:
  hostAndPort: localhost:8084
  timeout: 37s
consumer:
  recordsPerSecond: 20
  debugMetricRegex: foo
  debugHostRegex: bar
  debugFilePath: hello
  name: r15i11
  concurrency: 2
  batchSize: 700
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig ConfigPlus
	if err := config.Read(buffer, &aconfig); err != nil {
		t.Fatal(err)
	}
	expected := ConfigPlus{
		Writer: Config{
			HostAndPort: "localhost:8084",
			Timeout:     37 * time.Second,
		},
		Consumer: config.ConsumerConfig{
			Name:             "r15i11",
			Concurrency:      2,
			BatchSize:        700,
			DebugMetricRegex: "foo",
			DebugHostRegex:   "bar",
			RecordsPerSecond: 20,
			DebugFilePath:    "hello",
		},
	}
	if !reflect.DeepEqual(expected, aconfig) {
		t.Errorf("Expected %v, got %v", expected, aconfig)
	}
}
