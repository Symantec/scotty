package yamlutil_test

import (
	"bytes"
	"github.com/Symantec/scotty/lib/yamlutil"
	"gopkg.in/yaml.v2"
	"io"
	"testing"
)

type mixedType struct {
	PUBLIC  int
	private int
	Yaml    int `yaml:"boo"`
}

func (m *mixedType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type mixedFieldsType mixedType
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*mixedFieldsType)(m))
}

func TestUnmarshal(t *testing.T) {
	configFile := `
public: 1
private: 2
boo: 3
`
	buffer := bytes.NewBuffer(([]byte)(configFile))
	var m mixedType
	if err := readFrom(buffer, &m); err == nil {
		t.Error("Expected error here.")
	}

	configFile = `
public: 1
boo: 3
`
	m = mixedType{}
	buffer = bytes.NewBuffer(([]byte)(configFile))
	if err := readFrom(buffer, &m); err != nil {
		t.Error("Expected this to unmarshal.")
	}
	assertValueEquals(
		t,
		mixedType{
			PUBLIC: 1,
			Yaml:   3,
		},
		m)
}

func readFrom(r io.Reader, result *mixedType) error {
	var content bytes.Buffer
	if _, err := content.ReadFrom(r); err != nil {
		return err
	}
	if err := yaml.Unmarshal(content.Bytes(), result); err != nil {
		return err
	}
	return nil
}

func assertValueEquals(
	t *testing.T,
	expected, actual interface{}) {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
