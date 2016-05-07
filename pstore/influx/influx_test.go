package influx

import (
	"bytes"
	"reflect"
	"testing"
)

func TestInfluxConfig(t *testing.T) {
	configFile := `
database: aDatabase
endpoints:
- hostAndPort: localhost:8085
  username: foo
  password: apassword
- hostAndPort: http://mysite.com:8087
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err != nil {
		t.Fatal(err)
	}
	expected := Config{
		Database: "aDatabase",
		Endpoints: []Endpoint{
			{
				HostAndPort: "localhost:8085",
				UserName:    "foo",
				Password:    "apassword",
			},
			{
				HostAndPort: "http://mysite.com:8087",
			},
		},
	}
	if !reflect.DeepEqual(expected, config) {
		t.Errorf("Expected %v, got %v", expected, config)
	}
}

func TestInfluxConfigError1(t *testing.T) {
	configFile := `
database: aDatabase
endpoints:
- username: foo
  password: apassword
- hostAndPort: http://mysite.com:8087
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err == nil {
		t.Error("Expected error reading config file.")
	}
}

func TestInfluxConfigError2(t *testing.T) {
	configFile := `
endpoints:
- hostAndPort: localhost:8085
  username: foo
  password: apassword
- hostAndPort: http://mysite.com:8087
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err == nil {
		t.Error("Expected error reading config file.")
	}
}

func TestInfluxConfigError3(t *testing.T) {
	configFile := `
database: somedb
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var config Config
	if err := config.Read(buffer); err == nil {
		t.Error("Expected error reading config file.")
	}
}
