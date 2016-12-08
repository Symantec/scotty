package kafka

import (
	"bytes"
	"encoding/json"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"reflect"
	"testing"
	"time"
)

func TestKafkaConfig(t *testing.T) {
	configFile := `
# A comment
endpoints:
    - 10.0.0.1:9092
    - 10.0.1.3:9092
    - 10.0.1.6:9092
topic: someTopic
apiKey: someApiKey
tenantId: someTenantId
clientId: someClientId
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := yamlutil.Read(buffer, &aconfig); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		ApiKey:   "someApiKey",
		TenantId: "someTenantId",
		ClientId: "someClientId",
		Topic:    "someTopic",
		Endpoints: []string{
			"10.0.0.1:9092", "10.0.1.3:9092", "10.0.1.6:9092"},
	}
	if !reflect.DeepEqual(expected, aconfig) {
		t.Errorf("Expected %v, got %v", expected, aconfig)
	}
}

func TestKafkaConfigError(t *testing.T) {
	configFile := `
# A comment
endpoint:
    - 10.0.0.1:9092
    - 10.0.1.3:9092
    - 10.0.1.6:9092
topic: someTopic
apiKey: someApiKey
tenantId: someTenantId
clientId: someClientId
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := yamlutil.Read(buffer, &aconfig); err == nil {
		t.Error("Expected error: misspelled endpoint")
	}
}

func TestSerializeBool(t *testing.T) {
	ser := recordSerializerType{TenantId: "myTenantId", ApiKey: "myApiKey"}
	bytes, err := ser.Serialize(
		&pstore.Record{
			Kind:      types.Bool,
			Timestamp: time.Date(2014, 5, 13, 9, 53, 20, 125000000, time.UTC),
			Value:     false,
			Path:      "/my/path/bool",
			HostName:  "ash2",
			Tags:      pstore.TagGroup{pstore.TagAppName: "Health"}})
	if err != nil {
		t.Fatal(err)
	}
	verifySerialization(
		t,
		bytes,
		"1",
		"myTenantId",
		"myApiKey",
		"2014-05-13T09:53:20.125Z",
		0,
		"_my_path_bool",
		"ash2",
		"Health")

	bytes, err = ser.Serialize(
		&pstore.Record{
			Kind:      types.Bool,
			Timestamp: time.Date(2014, 5, 13, 9, 53, 20, 375000000, time.UTC),
			Value:     true,
			Path:      "/my/path/bools",
			HostName:  "ash3",
			Tags:      pstore.TagGroup{pstore.TagAppName: "cat"}})
	if err != nil {
		t.Fatal(err)
	}
	verifySerialization(
		t,
		bytes,
		"1",
		"myTenantId",
		"myApiKey",
		"2014-05-13T09:53:20.375Z",
		1,
		"_my_path_bools",
		"ash3",
		"cat")
}

func TestSerializeUint(t *testing.T) {
	quickVerify(t, types.Uint64, uint64(13579), 13579)
}

func TestSerializeFloat(t *testing.T) {
	quickVerify(t, types.Float64, -79.236, -79.236)
}

func TestSerializeTime(t *testing.T) {
	quickVerify(
		t,
		types.GoTime,
		time.Date(2015, 12, 17, 16, 40, 23, 0, time.UTC),
		1450370423)
}

func TestSerializeDuration(t *testing.T) {
	quickVerify(
		t,
		types.GoDuration,
		-time.Minute-120*time.Millisecond,
		-60.12)
	quickVerifyWithUnit(
		t,
		types.GoDuration,
		units.Second,
		-time.Minute-120*time.Millisecond,
		-60.12)
	quickVerifyWithUnit(
		t,
		types.GoDuration,
		units.Millisecond,
		-time.Minute-120*time.Millisecond,
		-60120)
}

func quickVerify(
	t *testing.T,
	kind types.Type,
	value interface{},
	expected float64) {
	quickVerifyWithUnit(
		t, kind, units.None, value, expected)
}

func quickVerifyWithUnit(
	t *testing.T,
	kind types.Type,
	unit units.Unit,
	value interface{},
	expected float64) {
	ser := recordSerializerType{
		TenantId: "myTenant", ApiKey: "myApi"}
	bytes, err := ser.Serialize(
		&pstore.Record{
			Kind:      kind,
			Unit:      unit,
			Timestamp: time.Date(2014, 5, 13, 9, 53, 20, 875000000, time.UTC),
			Value:     value,
			Path:      "/my/path/someValue",
			HostName:  "someHost",
			Tags:      pstore.TagGroup{pstore.TagAppName: "someApp"}})
	if err != nil {
		t.Fatal(err)
	}
	verifySerialization(
		t,
		bytes,
		"1",
		"myTenant",
		"myApi",
		"2014-05-13T09:53:20.875Z",
		expected,
		"_my_path_someValue",
		"someHost",
		"someApp")

}

func verifySerialization(
	t *testing.T,
	ser []byte,
	version string,
	tenantId, apiKey string,
	timeStamp string,
	value float64,
	path string,
	hostName string,
	appName string) {
	var result map[string]interface{}
	if err := json.Unmarshal(ser, &result); err != nil {
		t.Fatalf("Error unmarshalling byte array: %v", err)
	}
	expected := map[string]interface{}{
		kVersion:          version,
		kTenantId:         tenantId,
		kApiKey:           apiKey,
		kTimestamp:        timeStamp,
		kValue:            value,
		kName:             path,
		kHost:             hostName,
		pstore.TagAppName: appName}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
