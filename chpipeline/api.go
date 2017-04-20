package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"time"
)

// FsStats contains stats for a particular file system for some point in time
type FsStats struct {
	MountPoint string
	Size       uint64
	Free       uint64
}

// Used returns how many bytes are used.
func (f *FsStats) Used() uint64 {
	if f.Free > f.Size {
		return 0
	}
	return f.Size - f.Free
}

// UsedPercent returns the percentage of the file system that is used.
// UsedPercent returns false if the size of the file system is zero.
func (f *FsStats) UsedPercent() (float64, bool) {
	if f.Size == 0 {
		return 0.0, false
	}
	return float64(f.Used()) / float64(f.Size) * 100.0, true
}

// InstanceStats contains cloudhealth statistics for an aws instance at
// some point in time
type InstanceStats struct {
	Ts               time.Time // Timestamp of statistics.
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

// MemoryUsedPercent returns the percentage of memory used.
// MemoryUsedPercent returns false if total memory is 0.
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

// GetStats reads cloudhealth statistics from a group of collected metrics
// for some instance.
func GetStats(list metrics.List) InstanceStats {
	return getStats(list)
}

// CloudHealthInstanceCall represents a call to write cloudhealth data
// for one instance.
type CloudHealthInstanceCall struct {
	Instance cloudhealth.InstanceData // The instance data to write
	Fss      []cloudhealth.FsData     // Data for each file system in instance
}

// Split splits this call into smaller calls that are below the maximum size
// for writing to cloudhealth.
// If c is small enough, then split returns c, nil. If c is too big,
// Split returns a version of c that is small enough plus additional groups
// of file system data. In this case, caller must write each additional group
// of file system data separately.
func (c CloudHealthInstanceCall) Split() (
	CloudHealthInstanceCall, [][]cloudhealth.FsData) {
	return c.split()
}

// RollUpStats represents rolled up statistics for a machine by some time
// period.
type RollUpStats struct {
	instanceId        string
	ts                time.Time
	roundDuration     time.Duration
	notEmpty          bool
	cpuUsedPercent    cloudhealth.FVariable
	memoryFreeBytes   cloudhealth.IVariable
	memorySizeBytes   cloudhealth.IVariable
	memoryUsedPercent cloudhealth.FVariable
	fss               map[string]*rollUpFsStatsType
}

// NewRollUpStats creates a new RollUpStats for given instanceId that
// rolls up data every roundDuration. Hint: to do hourly rollups, pass
// time.Hour for roundDuration.
func NewRollUpStats(
	instanceId string, roundDuration time.Duration) *RollUpStats {
	return &RollUpStats{
		instanceId:    instanceId,
		roundDuration: roundDuration,
		fss:           make(map[string]*rollUpFsStatsType)}
}

// TimeOk returns true if time t is for the same time period as the other times
// in this instancce.
func (r *RollUpStats) TimeOk(t time.Time) bool {
	return r.timeOk(t)
}

// Add adds s to this instance. Add panics if the timestamp for s is not
// for the same time period as times already in this instance.
func (r *RollUpStats) Add(s InstanceStats) {
	r.add(s)
}

// Clear clears this instance and returns the call needed to write the data
// that was cleared to cloud health. After clear is called, Add will accept
// data with any timestamp. Clear panics if IsEmpty() returns true.
func (r *RollUpStats) Clear() CloudHealthInstanceCall {
	return r.clear()
}

// IsEmpty returns true if this instance has not data.
func (r *RollUpStats) IsEmpty() bool {
	return !r.notEmpty
}
