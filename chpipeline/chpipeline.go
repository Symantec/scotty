package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"sort"
	"time"
)

const (
	kMaxFileSystemsWithInstance = (cloudhealth.MaxDataPoints - cloudhealth.InstanceDataPointCount) / cloudhealth.FsDataPointCount
	kMaxFileSystems             = cloudhealth.MaxDataPoints / cloudhealth.FsDataPointCount
)

func newMaybeFloat64(value float64, ok bool) MaybeFloat64 {
	return MaybeFloat64{Value: value, Ok: ok}
}

func newMaybeUint64(value uint64, ok bool) MaybeUint64 {
	return MaybeUint64{Value: value, Ok: ok}
}

func (f *FsStats) used() (result uint64, ok bool) {
	if !f.Size.Ok || !f.Free.Ok {
		return
	}
	if f.Free.Value > f.Size.Value {
		return 0, true
	}
	return f.Size.Value - f.Free.Value, true
}

func (f *FsStats) usedPercent() (result float64, ok bool) {
	used, usedOk := f.used()
	if !usedOk || !f.Size.Ok || f.Size.Value == 0 {
		return
	}
	return (float64(used) / float64(f.Size.Value) * 100.0), true
}

func (s *InstanceStats) cpuUsedPercent() (result float64, ok bool) {
	if !s.UserTimeFraction.Ok {
		return
	}
	percent := s.UserTimeFraction.Value * 100.0
	if percent < 0.0 {
		return 0.0, true
	}
	if percent > 100.0 {
		return 100.0, true
	}
	return percent, true
}

func (s *InstanceStats) memoryUsedPercent() (result float64, ok bool) {
	if !s.MemoryTotal.Ok || !s.MemoryFree.Ok || s.MemoryTotal.Value == 0 {
		return
	}
	var memoryUsed uint64
	if s.MemoryFree.Value < s.MemoryTotal.Value {
		memoryUsed = s.MemoryTotal.Value - s.MemoryFree.Value
	}
	return float64(memoryUsed) / float64(s.MemoryTotal.Value) * 100.0, true
}

func getStats(list metrics.List) InstanceStats {
	var result InstanceStats
	// TODO: Maybe use time from remote host instead?
	result.Ts = time.Now().UTC()

	result.UserTimeFraction = newMaybeFloat64(metrics.GetFloat64(
		list, "/sys/sched/cpu/user-time-fraction"))
	result.MemoryFree = newMaybeUint64(metrics.GetUint64(
		list, "/sys/memory/available"))
	result.MemoryTotal = newMaybeUint64(metrics.GetUint64(
		list, "/sys/memory/total"))

	fileSystems := metrics.FileSystems(list)
	for _, fileSystem := range fileSystems {
		prefix := "/sys/fs"
		if fileSystem != "/" {
			prefix += fileSystem
		}
		size := newMaybeUint64(metrics.GetUint64(list, prefix+"/METRICS/size"))
		free := newMaybeUint64(metrics.GetUint64(list, prefix+"/METRICS/free"))
		result.Fss = append(
			result.Fss,
			FsStats{MountPoint: fileSystem, Size: size, Free: free})
	}
	return result
}

type rollUpFsStatsType struct {
	fsSizeBytes   cloudhealth.IVariable
	fsUsedBytes   cloudhealth.IVariable
	fsUsedPercent cloudhealth.FVariable
}

func (r *rollUpFsStatsType) IsEmpty() bool {
	return r.fsSizeBytes.IsEmpty() && r.fsUsedBytes.IsEmpty() && r.fsUsedPercent.IsEmpty()
}

func (r *RollUpStats) timeOk(t time.Time) bool {
	// Time is ok if no roll up time is yet established
	if !r.tsOk {
		return true
	}
	return r.roundTime(t).Equal(r.ts)
}

func (r *RollUpStats) add(s InstanceStats) {
	if !r.timeOk(s.Ts) {
		panic("Time not ok")
	}
	r.ts = r.roundTime(s.Ts)
	r.tsOk = true

	if cpuUsedPercent, ok := s.CPUUsedPercent(); ok {
		r.cpuUsedPercent.Add(cpuUsedPercent)
	}
	if s.MemoryFree.Ok {
		r.memoryFreeBytes.Add(s.MemoryFree.Value)
	}
	if s.MemoryTotal.Ok {
		r.memorySizeBytes.Add(s.MemoryTotal.Value)
	}
	if used, ok := s.MemoryUsedPercent(); ok {
		r.memoryUsedPercent.Add(used)
	}
	for _, fss := range s.Fss {
		rollupFs := r.fss[fss.MountPoint]
		if rollupFs == nil {
			rollupFs = &rollUpFsStatsType{}
			r.fss[fss.MountPoint] = rollupFs
		}
		if fss.Size.Ok {
			rollupFs.fsSizeBytes.Add(fss.Size.Value)
		}
		if used, ok := fss.Used(); ok {
			rollupFs.fsUsedBytes.Add(used)
		}
		if usedPercent, ok := fss.UsedPercent(); ok {
			rollupFs.fsUsedPercent.Add(usedPercent)
		}
	}
}

func (r *RollUpStats) cloudHealth() CloudHealthInstanceCall {
	if !r.tsOk {
		panic("No timestamp")
	}
	instance := cloudhealth.InstanceData{
		AccountNumber:     r.accountNumber,
		InstanceId:        r.instanceId,
		Ts:                r.ts,
		CpuUsedPercent:    r.cpuUsedPercent,
		MemoryFreeBytes:   r.memoryFreeBytes,
		MemorySizeBytes:   r.memorySizeBytes,
		MemoryUsedPercent: r.memoryUsedPercent,
	}
	var fss []cloudhealth.FsData
	for mountPoint, rollupFs := range r.fss {
		if rollupFs.IsEmpty() {
			continue
		}
		fsData := cloudhealth.FsData{
			AccountNumber: r.accountNumber,
			InstanceId:    r.instanceId,
			MountPoint:    mountPoint,
			Ts:            r.ts,
			FsSizeBytes:   rollupFs.fsSizeBytes,
			FsUsedBytes:   rollupFs.fsUsedBytes,
			FsUsedPercent: rollupFs.fsUsedPercent,
		}
		fss = append(fss, fsData)
	}
	// Needed to make testing possible
	sort.Sort(byMountPointType(fss))
	return CloudHealthInstanceCall{
		Instance: instance,
		Fss:      fss,
	}
}

func (r *RollUpStats) clear() {
	r.tsOk = false
	r.cpuUsedPercent.Clear()
	r.memoryFreeBytes.Clear()
	r.memorySizeBytes.Clear()
	r.memoryUsedPercent.Clear()
	for _, rollupFs := range r.fss {
		rollupFs.fsSizeBytes.Clear()
		rollupFs.fsUsedBytes.Clear()
		rollupFs.fsUsedPercent.Clear()
	}
}

func (r *RollUpStats) roundTime(t time.Time) time.Time {
	return t.UTC().Round(r.roundDuration)
}

func (c CloudHealthInstanceCall) split() (
	newCall CloudHealthInstanceCall,
	extraFs [][]cloudhealth.FsData) {
	if len(c.Fss) <= kMaxFileSystemsWithInstance {
		return c, nil
	}
	fssLeft := c.Fss
	newCall = CloudHealthInstanceCall{
		Instance: c.Instance,
		Fss:      fssLeft[:kMaxFileSystemsWithInstance],
	}
	fssLeft = fssLeft[kMaxFileSystemsWithInstance:]
	for len(fssLeft) > kMaxFileSystems {
		extraFs = append(extraFs, fssLeft[:kMaxFileSystems])
		fssLeft = fssLeft[kMaxFileSystems:]
	}
	extraFs = append(extraFs, fssLeft)
	return
}

type byMountPointType []cloudhealth.FsData

func (b byMountPointType) Len() int { return len(b) }

func (b byMountPointType) Swap(i, j int) { b[j], b[i] = b[i], b[j] }

func (b byMountPointType) Less(i, j int) bool {
	return b[i].MountPoint < b[j].MountPoint
}
