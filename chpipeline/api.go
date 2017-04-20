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

func (f *FsStats) UsedPercent() (float64, bool) {
	if f.Size == 0 {
		return 0.0, false
	}
	return float64(f.Used()) / float64(f.Size) * 100.0, true
}

type InstanceStats struct {
	Ts               time.Time
	UserTimeFraction float64
	MemoryFree       uint64
	MemoryTotal      uint64
	Fss              []FsStats
}

func (s *InstanceStats) CPUUsedPercent() float64 {
	result := s.UserTimeFraction * 100.0
	if result < 0.0 {
		return 0.0
	}
	if result > 100.0 {
		return 100.0
	}
	return result
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
	return getStats(list)
}

type CloudHealthInstanceCall struct {
	Instance cloudhealth.InstanceData
	Fss      []cloudhealth.FsData
}

func (c CloudHealthInstanceCall) Split() (
	CloudHealthInstanceCall, [][]cloudhealth.FsData) {
	return c.split()
}

type RollUpStats struct {
	instanceId        string
	ts                time.Time
	notEmpty          bool
	cpuUsedPercent    cloudhealth.FVariable
	memoryFreeBytes   cloudhealth.IVariable
	memorySizeBytes   cloudhealth.IVariable
	memoryUsedPercent cloudhealth.FVariable
	fss               map[string]*rollUpFsStatsType
}

func NewRollUpStats(
	instanceId string) *RollUpStats {
	return &RollUpStats{
		instanceId: instanceId,
		fss:        make(map[string]*rollUpFsStatsType)}
}

func (r *RollUpStats) TimeOk(t time.Time) bool {
	return r.timeOk(t)
}

func (r *RollUpStats) Add(s InstanceStats) {
	r.add(s)
}

func (r *RollUpStats) Clear() CloudHealthInstanceCall {
	return r.clear()
}

func (r *RollUpStats) IsEmpty() bool {
	return !r.notEmpty
}
