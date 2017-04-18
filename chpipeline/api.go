package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"time"
)

type FsStats struct {
	MountPoint string
	Size       uint64
	Free       uint64
}

func (f *FsStats) Used() uint64 {
	if f.Free > f.Size {
		return 0
	}
	return f.Size - f.Free
}

func (f *FsStats) UsedPercent() float64 {
	return float64(f.Used()) / float64(f.Size) * 100.0
}

type InstanceStats struct {
	Ts               time.Time
	UserTimeFraction float64
	MemoryFree       uint64
	MemoryTotal      uint64
	Fss              []FsStats
}

func (s *InstanceStats) CPUUsedPercent() float64 {
	// TODO: Make safe: Must return between 0 and 100.0
	return s.UserTimeFraction * 100.0
}

func (s *InstanceStats) MemoryUsedPercent() (float64, bool) {
	if s.MemoryTotal == 0 {
		return 0.0, false
	}
	var memoryUsed uint64
	if s.MemoryFree < s.MemoryTotal {
		memoryUsed = s.MemoryTotal - s.MemoryFree
	}
	return float64(memoryUsed) / float64(s.MemoryTotal) * 100.0, true
}

func GetStats(list metrics.List) InstanceStats {
	return InstanceStats{}
}

type CloudHealthCall struct {
	Instances []cloudhealth.InstanceData
	Fss       []cloudhealth.FsData
}

type RollUpStats struct {
	instanceId        string
	ts                time.Time
	notEmpty          bool
	cpuUsedPercent    cloudhealth.FVariable
	memoryFreeBytes   cloudhealth.IVariable
	memorySizeBytes   cloudhealth.IVariable
	memoryUsedPercent cloudhealth.FVariable
	// fsSizeBytes, fsUsedBytes, fsUsedPercent
	// fss map[string]*rollUpFsStatsType
}

func NewRollUpStats(
	instanceId string) *RollUpStats {
	return nil
}

func (r *RollUpStats) TimeOk(t time.Time) bool {
	return true
}

func (r *RollUpStats) Add(s InstanceStats) {
}

func (r *RollUpStats) Clear() CloudHealthCall {
	return CloudHealthCall{}
}
