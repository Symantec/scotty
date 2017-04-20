package metrics_test

import (
	"github.com/Symantec/scotty/metrics"
	"reflect"
	"testing"
	"time"
)

var (
	kNow = time.Date(2016, 6, 6, 13, 24, 0, 0, time.Local)
)

func TestOk(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:  "Ok",
			Value: int64(10),
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected ok. Got error")
	}
}

func TestMissingData(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path: "Missing",
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: missing value.")
	}
}

func TestBadData(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:  "Bad",
			Value: 35, // plain int. Not accepted.
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: bad value.")
	}
}

func TestDuplicateData(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:  "Duplicate",
			Value: int64(35),
		},
		{
			Path:  "Duplicate",
			Value: int64(36),
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: duplicate value.")
	}
}

func TestSomeTimeStampsMissing(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:  "Missing timestamp",
			Value: int64(35),
		},
		{
			Path:      "Present timestamp",
			Value:     int64(36),
			TimeStamp: kNow,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error: Should tolerate some timestamps missing")
	}
}

func TestDifferentTimeStampsSameGroup(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "Another timestamp",
			Value:     int64(36),
			TimeStamp: kNow.Add(time.Minute),
			GroupId:   0,
		},
		{
			Path:      "One timestamp",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: different timestamps same group.")
	}
}

func TestPathNamesSorted(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "One timestamp",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
		{
			Path:      "Another timestamp",
			Value:     int64(36),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: paths not sorted.")
	}
}

func TestPathNamesSortedCorrectly(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "/netstat/foo",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
		{
			Path:      "/netstat.0/foo",
			Value:     int64(36),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("No error expected. Already sorted correctly.")
	}
}

func TestSameTimeStampSameGroupOk(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "Another timestamp",
			Value:     int64(36),
			TimeStamp: kNow,
			GroupId:   0,
		},
		{
			Path:      "One timestamp",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for same timestamps same group.")
	}
}

func TestSameTimeStampDiffGroupOk(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "Another timestamp",
			Value:     int64(36),
			TimeStamp: kNow,
			GroupId:   3,
		},
		{
			Path:      "One timestamp",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for same timestamps different groups.")
	}
}

func TestDiffTimeStampDiffGroupOk(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path:      "Another timestamp",
			Value:     int64(36),
			TimeStamp: kNow.Add(time.Minute),
			GroupId:   1,
		},
		{
			Path:      "One timestamp",
			Value:     int64(35),
			TimeStamp: kNow,
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for different timestamps and groups.")
	}
}

func TestFind(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path: "bar",
		},
		{
			Path: "baz",
		},
		{
			Path: "foo",
		},
		{
			Path: "yyyy",
		},
	}
	if out := metrics.Find(list, "a"); out != 0 {
		t.Errorf("Expected 0, got %d", out)
	}
	if out := metrics.Find(list, "foo"); out != 2 {
		t.Errorf("Expected 2, got %d", out)
	}
	if out := metrics.Find(list, "zzz"); out != 4 {
		t.Errorf("Expected 4, got %d", out)
	}
}

func TestChildren(t *testing.T) {
	list := metrics.SimpleList{
		{
			Path: "/proc/fs/x/METRICS/free",
		},
		{
			Path: "/sys/fs/METRICS/free",
		},
		{
			Path: "/sys/fs/METRICS/size",
		},
		{
			Path: "/sys/fs/a",
		},
		{
			Path: "/sys/fs/boot/METRICS/free",
		},
		{
			Path: "/sys/fs/boot/a/METRICS/b",
		},
		{
			Path: "/sys/fs/boot/a/METRICS/free",
		},
		{
			Path: "/sys/fs/boot/a/METRICS/size",
		},
		{
			Path: "/sys/fs/boot/b/METRICS/size",
		},
		{
			Path: "/sys/fs/boot/a/y",
		},
		{
			Path: "/sys/fs/main/METRICS/free",
		},
		{
			Path: "/sys/fs/main/METRICS/size/x",
		},
		{
			Path: "/sys/net/blow/METRICS/size",
		},
	}
	actual := metrics.FileSystems(list)
	expected := []string{
		"/",
		"/boot",
		"/boot/a",
		"/boot/b",
		"/main",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v; got %v", expected, actual)
	}
}
