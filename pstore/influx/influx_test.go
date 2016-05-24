package influx

import (
	"bytes"
	"github.com/Symantec/scotty/pstore/config"
	"reflect"
	"testing"
)

func TestInfluxConfig(t *testing.T) {
	configFile := `
database: aDatabase
hostAndPort: localhost:8085
username: foo
password: apassword
precision: ms
writeConsistency: one
retentionPolicy: myPolicy
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var aconfig Config
	if err := config.Read(buffer, &aconfig); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		Database:         "aDatabase",
		HostAndPort:      "localhost:8085",
		UserName:         "foo",
		Password:         "apassword",
		RetentionPolicy:  "myPolicy",
		WriteConsistency: "one",
		Precision:        "ms",
	}
	if !reflect.DeepEqual(expected, aconfig) {
		t.Errorf("Expected %v, got %v", expected, aconfig)
	}
}
