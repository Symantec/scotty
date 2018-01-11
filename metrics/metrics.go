package metrics

import (
	"errors"
	"fmt"
	"github.com/Symantec/scotty/namesandports"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"sort"
	"strings"
	"time"
)

var (
	kSysFs        = pathType{"sys", "fs"}
	kHealthChecks = pathType{"health-checks"}
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

func endpoints(list List) (result namesandports.NamesAndPorts) {
	healthChecksLen := len(kHealthChecks)
	beginning := find(list, kHealthChecks)
	ending := findNext(list, kHealthChecks)
	for beginning < ending {
		current := getPathTypeByIndex(list, beginning)
		if len(current) == healthChecksLen {
			beginning++
			continue
		}
		name := current[healthChecksLen]
		base := current.Truncate(healthChecksLen + 1)
		if istri, _ := getBool(list, base.String()+"/has-tricorder-metrics"); istri {
			isTls, _ := getBool(list, base.String()+"/use-tls")
			port, ok := getAsUint(list, base.String()+"/port-number")
			if ok {
				result.Add(name, port, isTls)
			}
		}
		beginning = findNext(list, base)
	}
	return
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
	sort.Strings(result)
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

func getBool(list List, path string) (bool, bool) {
	val, ok := get(list, path)
	if !ok {
		return false, false
	}
	v, k := val.(bool)
	return v, k
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

func getAsUint(list List, path string) (uint, bool) {
	val, ok := get(list, path)
	if !ok {
		return 0, false
	}
	switch i := val.(type) {
	case int64:
		return uint(i), true
	case int32:
		return uint(i), true
	case int16:
		return uint(i), true
	case int8:
		return uint(i), true
	case int:
		return uint(i), true
	case uint64:
		return uint(i), true
	case uint32:
		return uint(i), true
	case uint16:
		return uint(i), true
	case uint8:
		return uint(i), true
	case uint:
		return i, true
	default:
		return 0, false
	}
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
			} else if !value.TimeStamp.Equal(lastTs) {
				return fmt.Errorf(
					"Conflicting timestamps for group %d: %s and %s; %v and %v",
					value.GroupId,
					duration.SinceEpoch(lastTs).String(),
					duration.SinceEpoch(value.TimeStamp).String(),
					lastTs,
					value.TimeStamp,
				)
			}
		}
	}
	return nil
}
