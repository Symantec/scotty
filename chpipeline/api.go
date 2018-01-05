// Package chpipeline manages collecting data for cloudhealth and other systems
package chpipeline

import (
	"container/list"
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"sync"
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
	s.CombineFsStats()
	return s
}

// ClearNonMemory clears the non memory related stats in place.
func (s *InstanceStats) ClearNonMemory() {
	s.UserTimeFraction.Ok = false
	s.Fss = nil
}

// CombineFsStats combines the file system stats of this instance in
// place using the CombineFsStats() function.
func (s *InstanceStats) CombineFsStats() {
	s.Fss = []FsStats{CombineFsStats(s.Fss)}
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

// AgedSnapshot is a Snapshot that knows its age.
// When creating, set Ts to time.Now()
type AgedSnapshot struct {
	Ts       time.Time
	Snapshot *Snapshot
}

// Age returns the age of the snapshot.
func (a AgedSnapshot) Age() time.Duration {
	return time.Since(a.Ts)
}

// AgeSnapshotList is a slice of snapshots with an age.
// When creating, set Ts to time.Now()
type AgedSnapshotList struct {
	Ts           time.Time
	SnapshotList []*Snapshot
}

// Age returns the age of this slice of snapshots.
func (a AgedSnapshotList) Age() time.Duration {
	return time.Since(a.Ts)
}

// AgedSnapshotChannel is a channel of AgedSnapshot that keeps track of how
// many times the channel overflows.
type AgedSnapshotChannel struct {
	Channel   chan AgedSnapshot
	mu        sync.Mutex
	overflows uint64
}

// NewAgedSnapshotChannel creates an AgedSnapshotChannel with specified
// length.
func NewAgedSnapshotChannel(length int) *AgedSnapshotChannel {
	return &AgedSnapshotChannel{
		Channel: make(chan AgedSnapshot, length),
	}
}

// Send sends the specified AgedSnapshot on the channel. If sending on the
// channel would block, Send returns immediately incrementing the overflow
// count.
func (a *AgedSnapshotChannel) Send(s AgedSnapshot) {
	a.send(s)
}

// Overflows returns the overflow count.
func (a *AgedSnapshotChannel) Overflows() uint64 {
	return a._overflows()
}

// AgedSnapshotListChannel is a channel of AgedSnapshotList that keeps track
// of how many times the channel overflows.
type AgedSnapshotListChannel struct {
	Channel   chan AgedSnapshotList
	mu        sync.Mutex
	overflows uint64
}

// NewAgedSnapshotListChannel creates an AgedSnapshotListChannel with specified
// length.
func NewAgedSnapshotListChannel(length int) *AgedSnapshotListChannel {
	return &AgedSnapshotListChannel{
		Channel: make(chan AgedSnapshotList, length),
	}
}

// Send sends the specified AgedSnapshotList on the channel. If sending on the
// channel would block, Send returns immediately incrementing the overflow
// count.
func (a *AgedSnapshotListChannel) Send(s AgedSnapshotList) {
	a.send(s)
}

// Overflows returns the overflow count.
func (a *AgedSnapshotListChannel) Overflows() uint64 {
	return a._overflows()
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
func (r *RollUpStats) Add(s *InstanceStats) {
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

// SnapshotStore stores snapshots from least to most recent. SnapshotStore
// evicts snapshots with older timestamps automatically as new snapshots
// are added. SnapshotStore instances also know how to persist themselves
// to the local file system.
type SnapshotStore struct {
	dirPath   string
	hostName  string
	appName   string
	span      time.Duration
	id        string
	snapshots list.List
}

// NewSnapshotStore creates a new SnapshotStore instance. dirPath is the
// full path of the directory where the new instance will store its data.
// Mutliple instances at once may store their data within the same directory.
// hostName and port are the hostname and port of the endpoint for which the
// new instance will store snapshots. span controls how long snapshots stay
// in newly created instance. The difference between the oldest and newest
// snapshot will never exceed span.
func NewSnapshotStore(
	dirPath string, hostName string, appName string, span time.Duration) *SnapshotStore {
	result := &SnapshotStore{
		dirPath:  dirPath,
		hostName: hostName,
		appName:  appName,
		span:     span,
		id:       computeId(hostName, appName)}
	result.snapshots.Init()
	return result
}

func (s *SnapshotStore) HostName() string { return s.hostName }

func (s *SnapshotStore) AppName() string { return s.appName }

// Load loads this instance's data from the file system into this instance.
func (s *SnapshotStore) Load() error {
	return s.load()
}

// Save saves this instance's data to the file system
func (s *SnapshotStore) Save() error {
	return s.save()
}

// Add adds a new snapshot to this instance
func (s *SnapshotStore) Add(snapshot *Snapshot) {
	s.add(snapshot)
}

// GetAll retrieves all the snapshots in this instance from oldest to newest.
func (s *SnapshotStore) GetAll() []*Snapshot {
	return s.getAll()
}
