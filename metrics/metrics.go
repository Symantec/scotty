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

var (
	kSysFs = pathType{"sys", "fs"}
)

type pathType []string

func newPath(s string) pathType {
	if strings.HasPrefix(s, "/") {
		s = s[1:]
	}
	return strings.Split(s, "/")
}

func (p pathType) Truncate(x int) pathType {
	if len(p) <= x {
		return p
	}
	return p[:x]
}

func (p pathType) Find(start int, s string) int {
	l := len(p)
	for i := start; i < l; i++ {
		if p[i] == s {
			return i
		}
	}
	return -1
}

func (p pathType) Sub(start, end int) pathType {
	return p[start:end]
}

func (p pathType) String() string {
	return "/" + strings.Join(p, "/")
}

func comparePaths(first, second pathType) int {
	flen := len(first)
	slen := len(second)
	for i := 0; i < flen && i < slen; i++ {
		if first[i] < second[i] {
			return -1
		}
		if first[i] > second[i] {
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

func getPathTypeByIndex(list List, index int) pathType {
	var value Value
	list.Index(index, &value)
	return newPath(value.Path)
}

func fileSystems(list List) (result []string) {
	sysFsLen := len(kSysFs)
	beginning := find(list, kSysFs)
	ending := findNext(list, kSysFs)
	for beginning < ending {
		current := getPathTypeByIndex(list, beginning)
		metricsPos := current.Find(sysFsLen, "METRICS")
		if metricsPos == -1 {
			beginning++
			continue
		}
		result = append(result, current.Sub(sysFsLen, metricsPos).String())
		beginning = findNext(list, current.Truncate(metricsPos+1))
	}
	return
}

func find(list List, target pathType) int {
	targetLen := len(target)
	return sort.Search(list.Len(), func(index int) bool {
		var value Value
		list.Index(index, &value)
		return comparePaths(newPath(value.Path).Truncate(targetLen), target) >= 0
	})
}

func findNext(list List, target pathType) int {
	targetLen := len(target)
	return sort.Search(list.Len(), func(index int) bool {
		var value Value
		list.Index(index, &value)
		return comparePaths(newPath(value.Path).Truncate(targetLen), target) > 0
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

func getTime(list List, path string) (time.Time, bool) {
	val, ok := get(list, path)
	if !ok {
		return time.Time{}, false
	}
	v, k := val.(time.Time)
	return v, k
}

func verifyList(list List) error {
	length := list.Len()
	pathSet := make(map[string]bool, length)
	groupIdToTimeStamp := make(map[int]time.Time)
	var lastPath pathType
	var lastPathName string
	for i := 0; i < length; i++ {
		var value Value
		list.Index(i, &value)
		thisPath := newPath(value.Path)
		if comparePaths(thisPath, lastPath) < 0 {
			return errors.New(
				fmt.Sprintf(
					"Paths not sorted: '%s' should come before '%s",
					value.Path,
					lastPathName))
		}
		lastPath = thisPath
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
