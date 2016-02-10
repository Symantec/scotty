package kafka

import (
	"bytes"
	"encoding/json"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"reflect"
	"testing"
	"time"
)

func TestKafkaConfigError(t *testing.T) {
	configFile := `
apiKey: someApiKey
tenantId: someTenantId
topic: someTopic
clinetId: someClientId
	`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err == nil {
		t.Error("Expected error reading config.")
	}
}

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
allowDuplicates: true
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		ApiKey:          "someApiKey",
		TenantId:        "someTenantId",
		ClientId:        "someClientId",
		Topic:           "someTopic",
		AllowDuplicates: true,
		Endpoints: []string{
			"10.0.0.1:9092", "10.0.1.3:9092", "10.0.1.6:9092"},
	}
	if !reflect.DeepEqual(expected, config) {
		t.Errorf("Expected %v, got %v", expected, config)
	}
}

func TestSerializeInt(t *testing.T) {
	ser := recordSerializerType{TenantId: "myTenantId", ApiKey: "myApiKey"}
	bytes, err := ser.Serialize(
		&pstore.Record{
			Kind:      types.Int,
			Timestamp: time.Date(2014, 5, 13, 9, 53, 20, 0, time.UTC),
			Value:     int64(-59),
			Path:      "/my/path",
			HostName:  "ash1",
			Tags:      pstore.TagGroup{pstore.TagAppName: "horse"}})
	if err != nil {
		t.Fatal(err)
	}
	verifySerialization(
		t,
		bytes,
		"1",
		"myTenantId",
		"myApiKey",
		"2014-05-13T09:53:20.000Z",
		-59,
		"/my/path",
		"ash1",
		"horse")
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
		"/my/path/bool",
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
		"/my/path/bools",
		"ash3",
		"cat")
}

func TestSerializeUint(t *testing.T) {
	quickVerify(t, types.Uint, uint64(13579), 13579)
}

func TestSerializeFloat(t *testing.T) {
	quickVerify(t, types.Float, -79.236, -79.236)
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
		"/my/path/someValue",
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
