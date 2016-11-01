package config_test

import (
	"bytes"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config"
	"github.com/Symantec/scotty/pstore/config/utils"
	"testing"
	"time"
)

func TestReadConfig(t *testing.T) {
	configFile := `
# a comment
- kafka:
    endpoints:
      - "http://10.0.0.1:9092"
    topic: someTopic
    apiKey: someApiKey
    tenantId: someTenantId
    clientId: someClientId
  consumer:
    recordsPerSecond: 124
    name: "kafka pstore"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPort: "localhost:8084"
    timeout: 37s
  consumer:
    name: "tsdb store"
    rollUpSpan: -2m
`
	consumerBuilders, err := consumerBuildersFromString(configFile)
	if err != nil {
		t.Fatal(err)
	}
	consumers := buildAll(consumerBuilders)
	assertValueEquals(t, 3, len(consumers))
	assertValueEquals(t, "kafka pstore", consumers[0].Name())
	var attributes pstore.ConsumerAttributes
	consumers[0].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency:      7,
			BatchSize:        730,
			RecordsPerSecond: 124,
			RollUpSpan:       135 * time.Second,
		},
		attributes)
	assertValueEquals(t, "influx store", consumers[1].Name())
	consumers[1].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency: 1,
			BatchSize:   1000,
		},
		attributes)
	assertValueEquals(t, "tsdb store", consumers[2].Name())
	consumers[2].Attributes(&attributes)
	assertValueEquals(
		t,
		pstore.ConsumerAttributes{
			Concurrency: 1,
			BatchSize:   1000,
		},
		attributes)
}

func TestReadConfigMisspelled(t *testing.T) {
	configFile := `
# a comment
- kafka:
    endpoints:
      - "http://10.0.0.1:9092"
    topic: someTopic
    apiKey: someApiKey
    tenantId: someTenantId
    clientId: someClientId
  consumer:
    recordsPerSecond: 124
    name: "kafka pstore"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influxx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPort: "localhost:8084"
    timeout: 37s
  consumer:
    name: "tsdb store"
    rollUpSpan: -2m
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected error: misspelled field influxx")
	}
}

func TestReadConfigMisspelled2(t *testing.T) {
	configFile := `
# a comment
- kafka:
    endpoints:
      - "http://10.0.0.1:9092"
    topic: someTopic
    apiKey: someApiKey
    tenantId: someTenantId
    clientId: someClientId
  consumer:
    recordsPerSecond: 124
    name: "kafka pstore"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPorts: "localhost:8084"
    timeout: 37s
  consumer:
    name: "tsdb store"
    rollUpSpan: -2m
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected error: misspelled field hostAndPorts")
	}
}

func TestReadConfigMissingName(t *testing.T) {
	configFile := `
# a comment
- kafka:
    endpoints:
      - "http://10.0.0.1:9092"
    topic: someTopic
    apiKey: someApiKey
    tenantId: someTenantId
    clientId: someClientId
  consumer:
    recordsPerSecond: 124
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPort: "localhost:8084"
    timeout: 37s
  consumer:
    name: "tsdb store"
    rollUpSpan: -2m
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected an error: missing name for kafka")
	}
}

func TestReadConfigDupName(t *testing.T) {
	configFile := `
# a comment
- kafka:
    endpoints:
      - "http://10.0.0.1:9092"
    topic: someTopic
    apiKey: someApiKey
    tenantId: someTenantId
    clientId: someClientId
  consumer:
    recordsPerSecond: 124
    name: "kafka pstore"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPort: "localhost:8084"
    timeout: 37s
  consumer:
    name: "kafka pstore"
    rollUpSpan: -2m
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected an error: dup name 'kafka pstore'")
	}
}

func TestReadConfigNoPersistentStore(t *testing.T) {
	configFile := `
# a comment
- consumer:
    recordsPerSecond: 124
    name: "kafka pstore"
    concurrency: 7
    batchSize: 730
    rollUpSpan: 2m15s
- influx:
    database: aDatabase
    hostAndPort: "http://10.0.1.1:8086"
    username: foo
    password: apassword
    precision: ms
    writeConsistency: one
    retentionPolicy: myPolicy
  consumer:
    name: "influx store"
- openTSDB:
    hostAndPort: "localhost:8084"
    timeout: 37s
  consumer:
    name: "tsdb pstore"
    rollUpSpan: -2m
`
	_, err := consumerBuildersFromString(configFile)
	if err == nil {
		t.Error("Expected an error: Missing store in first block'")
	}
}

type mixedType struct {
	Public  int
	private int
	YAML    int `yaml:"boo"`
}

func (m *mixedType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type mixedFieldsType mixedType
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*mixedFieldsType)(m))
}

func (m *mixedType) Reset() {
	*m = mixedType{}
}

func TestMixYAMLDefault(t *testing.T) {
	configFile := `
public: 1
private: 2
boo: 3
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var m mixedType
	if err := utils.Read(buffer, &m); err == nil {
		t.Error("Expected error here.")
	}

	configFile = `
public: 1
boo: 3
`
	buffer = bytes.NewBuffer(([]byte)(configFile))
	if err := utils.Read(buffer, &m); err != nil {
		t.Error("Expected this to unmarshal.")
	}
	assertValueEquals(
		t, mixedType{Public: 1, YAML: 3}, m)
}

func assertValueEquals(
	t *testing.T,
	expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func consumerBuildersFromString(s string) (
	result []*pstore.ConsumerWithMetricsBuilder, err error) {
	buffer := bytes.NewBuffer(([]byte)(s))
	var c config.ConfigList
	if err = utils.Read(buffer, &c); err != nil {
		return
	}
	return c.CreateConsumerBuilders()
}

func buildAll(list []*pstore.ConsumerWithMetricsBuilder) (
	result []*pstore.ConsumerWithMetrics) {
	result = make([]*pstore.ConsumerWithMetrics, len(list))
	for i := range result {
		result[i] = list[i].Build()
	}
	return
}
