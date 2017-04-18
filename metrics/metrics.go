package metrics

import (
	"errors"
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"sort"
	"strings"
	"time"
)

var (
	errGroupId = errors.New("metrics: Conflicting Timestamps for group Id.")
)

func comparePaths(first, second string) int {
	firstSplits := strings.Split(first, "/")
	secondSplits := strings.Split(second, "/")
	flen := len(firstSplits)
	slen := len(secondSplits)
	for i := 0; i < flen && i < slen; i++ {
		if firstSplits[i] < secondSplits[i] {
			return -1
		}
		if firstSplits[i] > secondSplits[i] {
			return 1
		}
	}
	if flen < slen {
		return -1
	}
	if flen > slen {
		return 1
	}
	return 0
}

func find(list List, path string) int {
	return sort.Search(list.Len(), func(index int) bool {
		var value Value
		list.Index(index, &value)
		return comparePaths(value.Path, path) >= 0
	})
}

func get(list List, path string) (interface{}, bool) {
	index := Find(list, path)
	if index == list.Len() {
		return nil, false
	}
	var value Value
	list.Index(index, &value)
	if value.Path != path {
		return nil, false
	}
	return value.Value, true
}

func getFloat64(list List, path string) (float64, bool) {
	val, ok := get(list, path)
	if !ok {
		return 0.0, false
	}
	v, k := val.(float64)
	return v, k
}

func getUint64(list List, path string) (uint64, bool) {
	val, ok := get(list, path)
	if !ok {
		return 0, false
	}
	v, k := val.(uint64)
	return v, k
}

func verifyList(list List) error {
	length := list.Len()
	pathSet := make(map[string]bool, length)
	groupIdToTimeStamp := make(map[int]time.Time)
	var lastPathName string
	for i := 0; i < length; i++ {
		var value Value
		list.Index(i, &value)
		if comparePaths(value.Path, lastPathName) < 0 {
			return errors.New(
				fmt.Sprintf(
					"Paths not sorted: '%s' should come before '%s",
					value.Path,
					lastPathName))
		}
		lastPathName = value.Path
		if pathSet[value.Path] {
			return errors.New(
				fmt.Sprintf("Duplicate path: %s", value.Path))
		}
		pathSet[value.Path] = true
		if types.FromGoValue(value.Value) == types.Unknown {
			return errors.New(
				fmt.Sprintf("Bad value: %v", value.Value))
		}
		if !value.TimeStamp.IsZero() {
			lastTs, ok := groupIdToTimeStamp[value.GroupId]
			if !ok {
				groupIdToTimeStamp[value.GroupId] = value.TimeStamp
			} else if value.TimeStamp != lastTs {
				return errGroupId
			}
		}
	}
	return nil
}
