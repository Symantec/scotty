package lmm

import (
	"bytes"
	"encoding/json"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"reflect"
	"testing"
	"time"
)

func TestLmmConfigError(t *testing.T) {
	configFile := `
	apiKey	someApiKey
	tenantId	someTenantId
	`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err == nil {
		t.Error("Expected error reading config.")
	}
}

func TestLmmConfig(t *testing.T) {
	configFile := `
	endpoint	10.0.0.1:9092
	endpoint	10.0.1.3:9092
	endpoint	10.0.1.6:9092

	topic	someTopic
	apiKey	someApiKey
	tenantId	someTenantId
	`

	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		ApiKey:   "someApiKey",
		TenantId: "someTenantId",
		Topic:    "someTopic",
		Endpoints: []string{
			"10.0.0.1:9092", "10.0.1.3:9092", "10.0.1.6:9092"},
	}
	if !reflect.DeepEqual(expected, config) {
		t.Errorf("Expected %v, got %v", expected, config)
	}
}

func TestSerializeInt(t *testing.T) {
	ser := newRecordSerializer("myTenantId", "myApiKey")
	bytes, err := ser.Serialize(
		makeRecord(
			types.Int,
			1400000000.0,
			int64(-59),
			"/my/path",
			"ash1"))
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
		"-59",
		"/my/path",
		"ash1")
}

func TestSerializeBool(t *testing.T) {
	ser := newRecordSerializer("myTenantId", "myApiKey")
	bytes, err := ser.Serialize(
		makeRecord(
			types.Bool,
			1400000000.125,
			false,
			"/my/path/bool",
			"ash2"))
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
		"0",
		"/my/path/bool",
		"ash2")

	bytes, err = ser.Serialize(
		makeRecord(
			types.Bool,
			1400000000.375,
			true,
			"/my/path/bools",
			"ash3"))
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
		"1",
		"/my/path/bools",
		"ash3")
}

func TestSerializeUint(t *testing.T) {
	quickVerify(t, types.Uint, uint64(13579), "13579")
}

func TestSerializeFloat(t *testing.T) {
	quickVerify(t, types.Float, float64(-79.236), "-79.236")
}

func TestSerializeTime(t *testing.T) {
	quickVerify(
		t,
		types.GoTime,
		time.Date(2015, 12, 17, 16, 40, 23, 0, time.UTC),
		"1450370423")
}

func TestSerializeDuration(t *testing.T) {
	quickVerify(
		t,
		types.GoDuration,
		-time.Minute-120*time.Millisecond,
		"-60.12")
	quickVerifyWithUnit(
		t,
		types.GoDuration,
		units.Second,
		-time.Minute-120*time.Millisecond,
		"-60.12")
	quickVerifyWithUnit(
		t,
		types.GoDuration,
		units.Millisecond,
		-time.Minute-120*time.Millisecond,
		"-60120")
}

func quickVerify(
	t *testing.T,
	kind types.Type,
	value interface{},
	expected string) {
	quickVerifyWithUnit(
		t, kind, units.None, value, expected)
}

func quickVerifyWithUnit(
	t *testing.T,
	kind types.Type,
	unit units.Unit,
	value interface{},
	expected string) {
	ser := newRecordSerializer("myTenant", "myApi")
	bytes, err := ser.Serialize(
		makeRecordWithUnit(
			kind,
			unit,
			1400000000.875,
			value,
			"/my/path/someValue",
			"someHost"))
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
		"someHost")

}

func verifySerialization(
	t *testing.T,
	ser []byte,
	version string,
	tenantId, apiKey string,
	timeStamp string,
	value string,
	path string,
	hostName string) {
	var result map[string]string
	if err := json.Unmarshal(ser, &result); err != nil {
		t.Fatalf("Error unmarshalling byte array: %v", err)
	}
	expected := map[string]string{
		kVersion:   version,
		kTenantId:  tenantId,
		kApiKey:    apiKey,
		kTimestamp: timeStamp,
		kValue:     value,
		kName:      path,
		kHost:      hostName}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func makeRecord(
	kind types.Type,
	ts float64,
	value interface{},
	path string,
	hostName string) iRecordType {
	return makeRecordWithUnit(
		kind, units.None, ts, value, path, hostName)
}

func makeRecordWithUnit(
	kind types.Type,
	unit units.Unit,
	ts float64,
	value interface{},
	path string,
	hostName string) iRecordType {
	return &recordForTestingType{
		kind:     kind,
		unit:     unit,
		ts:       ts,
		value:    value,
		path:     path,
		hostname: hostName}
}

type recordForTestingType struct {
	kind     types.Type
	unit     units.Unit
	ts       float64
	value    interface{}
	path     string
	hostname string
}

func (r *recordForTestingType) Kind() types.Type   { return r.kind }
func (r *recordForTestingType) Unit() units.Unit   { return r.unit }
func (r *recordForTestingType) Timestamp() float64 { return r.ts }
func (r *recordForTestingType) Value() interface{} { return r.value }
func (r *recordForTestingType) Path() string       { return r.path }
func (r *recordForTestingType) HostName() string   { return r.hostname }
