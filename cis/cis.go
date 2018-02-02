package cis

import (
	"github.com/Symantec/scotty/metrics"
	"strings"
	"time"
)

const (
	kSysPackages   = "/sys/packages/"
	kSysInstanceId = "/sys/cloud/aws/instance-id"
)

func getPackageInfo(list metrics.List) (
	result PackageInfo, ts time.Time, ok bool) {
	start := metrics.Find(list, kSysPackages)
	length := list.Len()
	var currentPackageEntry *PackageEntry
	for i := start; i < length; i++ {
		var value metrics.Value
		list.Index(i, &value)
		if !strings.HasPrefix(value.Path, kSysPackages) {
			return
		}
		// parts[0] is management type
		// parts[1] is name
		// parts[2] is either "size" or "version"
		parts := strings.Split(value.Path[len(kSysPackages):], "/")
		if len(parts) != 3 {
			continue
		}
		if !ok {
			ok = true
			result.ManagementType = parts[0]
			ts = value.TimeStamp
			if ts.IsZero() {
				ts = time.Now()
			}
			result.Packages = append(
				result.Packages, PackageEntry{Name: parts[1]})
			currentPackageEntry = &result.Packages[len(result.Packages)-1]
		} else if parts[0] != result.ManagementType {
			return
		} else if parts[1] != currentPackageEntry.Name {
			result.Packages = append(
				result.Packages, PackageEntry{Name: parts[1]})
			currentPackageEntry = &result.Packages[len(result.Packages)-1]
		}
		if parts[2] == "size" {
			sz, ok := value.Value.(uint64)
			if ok {
				currentPackageEntry.Size = sz
			}
		} else if parts[2] == "version" {
			ver, ok := value.Value.(string)
			if ok {
				currentPackageEntry.Version = ver
			}
		}
	}
	return
}

func getInstanceId(list metrics.List) (result string, ok bool) {
	index := metrics.Find(list, kSysInstanceId)
	if index == list.Len() {
		return
	}
	var value metrics.Value
	list.Index(index, &value)
	if value.Path != kSysInstanceId {
		return
	}
	result, ok = value.Value.(string)
	return
}

func getStats(list metrics.List, optInstanceId string) *Stats {
	var result Stats
	if optInstanceId != "" {
		result.InstanceId = optInstanceId
	} else {
		var ok bool
		result.InstanceId, ok = getInstanceId(list)
		if !ok {
			return nil
		}
	}
	var ok bool
	result.Packages, result.TimeStamp, ok = getPackageInfo(list)
	if !ok {
		return nil
	}
	return &result
}

func (p *PackageInfo) equals(rhs *PackageInfo) bool {
	if p.ManagementType != rhs.ManagementType {
		return false
	}
	if len(p.Packages) != len(rhs.Packages) {
		return false
	}
	for i := range p.Packages {
		if p.Packages[i] != rhs.Packages[i] {
			return false
		}
	}
	return true
}
