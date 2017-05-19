// Package chpipeline manages collecting data for cloudhealth and other systems
package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"time"
)

// MaybeFloat64 values are either nothing or a float64.
type MaybeFloat64 struct {
	Value float64
	Ok    bool // True if Value is set; false if nothing.
}

// MaybeUint64 values are either nothing or a uint64.
type MaybeUint64 struct {
	Value uint64
	Ok    bool // True if Value is set; false if nothing.
}

// FsStats contains stats for a particular file system for some point in time
type FsStats struct {
	MountPoint string
	Size       MaybeUint64
	Free       MaybeUint64
}

// CombineFsStats returns a collection of FsStats as a single FsStats with
// mount point of '/'. The returned instance contains the combined size and
// free space of all file systems. When combining, CombineFsStats ignores
// any FsStats instance with missing data.
func CombineFsStats(stats []FsStats) FsStats {
	return combineFsStats(stats)
}

// Used returns how many bytes are used or false if information is missing.
func (f *FsStats) Used() (uint64, bool) {
	return f.used()
}

// UsedPercent returns the percentage of the file system that is used.
// UsedPercent returns false if the size of the file system is zero or if
// needed information is missing.
func (f *FsStats) UsedPercent() (float64, bool) {
	return f.usedPercent()
}

// InstanceStats contains cloudhealth statistics for an aws instance at
// some point in time
type InstanceStats struct {
	Ts               time.Time // Timestamp of statistics.
	UserTimeFraction MaybeFloat64
	MemoryFree       MaybeUint64
	MemoryTotal      MaybeUint64
	Fss              []FsStats
}

// WithCombinedFsStats returns an instance like this one but with the file
// system stats combined using the CombineFsStats() function.
func (s InstanceStats) WithCombinedFsStats() InstanceStats {
	s.Fss = []FsStats{CombineFsStats(s.Fss)}
	return s
}

// CPUUsedPercent returns CPU usage between 0.0 and 100.0. Returns false
// if needed information is missing.
func (s *InstanceStats) CPUUsedPercent() (float64, bool) {
	return s.cpuUsedPercent()
}

// MemoryUsedPercent returns the percentage of memory used.
// MemoryUsedPercent returns false if total memory is 0 or if needed
// information is missing.
func (s *InstanceStats) MemoryUsedPercent() (float64, bool) {
	return s.memoryUsedPercent()
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

// NewCloudHealthInstanceCall creates a new CloudHealthInstanceCall from a
// Snapshot.
func NewCloudHealthInstanceCall(s *Snapshot) CloudHealthInstanceCall {
	return newCloudHealthInstanceCall(s)
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

// Snapshot represents a snapshot of a RollUpStats instance.
// Snapshot instances should be treated as immutable.
type Snapshot struct {
	AccountNumber     string
	InstanceId        string
	Ts                time.Time
	CpuUsedPercent    cloudhealth.FVariable
	MemoryFreeBytes   cloudhealth.IVariable
	MemorySizeBytes   cloudhealth.IVariable
	MemoryUsedPercent cloudhealth.FVariable
	Fss               []FsSnapshot
}

// FsSnaapshot depicts a snapshot of a file system
type FsSnapshot struct {
	MountPoint  string
	Size        cloudhealth.IVariable
	Used        cloudhealth.IVariable
	UsedPercent cloudhealth.FVariable
}

// RollUpStats represents rolled up statistics for a machine by some time
// period.
type RollUpStats struct {
	accountNumber     string
	instanceId        string
	ts                time.Time
	tsOk              bool
	roundDuration     time.Duration
	cpuUsedPercent    cloudhealth.FVariable
	memoryFreeBytes   cloudhealth.IVariable
	memorySizeBytes   cloudhealth.IVariable
	memoryUsedPercent cloudhealth.FVariable
	fss               map[string]*rollUpFsStatsType
}

// NewRollUpStats creates a new RollUpStats for given accountNumber and
// instanceId that rolls up data every roundDuration. Hint: to do hourly
// rollups, pass time.Hour for roundDuration.
func NewRollUpStats(
	accountNumber string,
	instanceId string,
	roundDuration time.Duration) *RollUpStats {
	return &RollUpStats{
		accountNumber: accountNumber,
		instanceId:    instanceId,
		roundDuration: roundDuration,
		fss:           make(map[string]*rollUpFsStatsType)}
}

func (r *RollUpStats) RoundDuration() time.Duration { return r.roundDuration }

func (r *RollUpStats) AccountNumber() string { return r.accountNumber }

func (r *RollUpStats) InstanceId() string { return r.instanceId }

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

// TakeSnapshot grabs a snapshot of this instance
func (r *RollUpStats) TakeSnapshot() *Snapshot {
	return r.takeSnapshot()
}

// Clear clears this instance
// After clear is called, Add will accept data with any timestamp.
func (r *RollUpStats) Clear() {
	r.clear()
}
