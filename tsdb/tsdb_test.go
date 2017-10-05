package tsdb_test

import (
	"encoding/json"
	"github.com/Symantec/scotty/tsdb"
	"testing"
)

func TestMarshal(t *testing.T) {
	ts := tsdb.TimeSeries{
		{1400500600.0, 39.25},
		{1400500700.0, 40.75},
		{1400500800.0, 41.0},
	}
	b, _ := json.Marshal(ts)
	assertValueEquals(
		t,
		"{\"1400500600\":39.25,\"1400500700\":40.75,\"1400500800\":41}",
		string(b))
}

func TestEarliest(t *testing.T) {
	ts := tsdb.TimeSeries{
		{1400500600.0, 39.25},
		{1400500700.0, 40.75},
		{1400500800.0, 41.0},
	}
	truncated := ts.EarlyTruncate(1400500700.0)
	if len(truncated) != 2 {
		t.Error("Expected truncated to be of length 2")
	}
	if truncated[0].Ts != 1400500700.0 {
		t.Error("Expected first ts to be 1400500700.0")
	}
}

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	t.Helper()
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}
