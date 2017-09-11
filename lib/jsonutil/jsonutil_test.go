package jsonutil_test

import (
	"bytes"
	"encoding/json"
	"github.com/Symantec/scotty/lib/jsonutil"
	"io"
	"strings"
	"testing"
)

type mixedType struct {
	PUBLIC  int
	private int
	Yaml    int `json:"boo"`
}

func (m *mixedType) UnmarshalJSON(b []byte) error {
	type mixedFieldsType mixedType
	return jsonutil.StrictUnmarshalJSON(b, (*mixedFieldsType)(m))
}

func TestUnmarshal(t *testing.T) {
	configFile := `{"public": 1, "private": 2, "boo": 3}`
	var m mixedType
	if err := readFrom(strings.NewReader(configFile), &m); err == nil {
		t.Error("Expected error here.")
	}

	configFile = `{"public": 1, "boo": 3}`
	m = mixedType{}
	if err := readFrom(strings.NewReader(configFile), &m); err != nil {
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
	if err := json.Unmarshal(content.Bytes(), result); err != nil {
		return err
	}
	return nil
}

func assertValueEquals(
	t *testing.T,
	expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}
