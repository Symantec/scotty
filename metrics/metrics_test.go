package metrics_test

import (
	"github.com/Symantec/scotty/metrics"
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
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: some missing timestamps.")
	}
}

func TestDifferentTimeStampsSameGroup(t *testing.T) {
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
			TimeStamp: kNow.Add(time.Minute),
			GroupId:   0,
		},
	}
	if err := metrics.VerifyList(list); err == nil {
		t.Error("Expected error: different timestamps same group.")
	}
}

func TestSameTimeStampSameGroupOk(t *testing.T) {
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
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for same timestamps same group.")
	}
}

func TestSameTimeStampDiffGroupOk(t *testing.T) {
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
			GroupId:   3,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for same timestamps different groups.")
	}
}

func TestDiffTimeStampDiffGroupOk(t *testing.T) {
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
			TimeStamp: kNow.Add(time.Minute),
			GroupId:   1,
		},
	}
	if err := metrics.VerifyList(list); err != nil {
		t.Error("Expected no error for different timestamps and groups.")
	}
}
