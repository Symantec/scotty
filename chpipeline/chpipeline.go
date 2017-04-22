package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"time"
)

const (
	kMaxFileSystemsWithInstance = (cloudhealth.MaxDataPoints - cloudhealth.InstanceDataPointCount) / cloudhealth.FsDataPointCount
	kMaxFileSystems             = cloudhealth.MaxDataPoints / cloudhealth.FsDataPointCount
)

func getStats(list metrics.List) InstanceStats {
	var result InstanceStats
	// TODO: Maybe use time from remote host instead?
	result.Ts = time.Now().UTC()

	result.UserTimeFraction, _ = metrics.GetFloat64(
		list, "/sys/sched/cpu/user-time-fraction")
	result.MemoryFree, _ = metrics.GetUint64(list, "/sys/memory/free")
	result.MemoryTotal, _ = metrics.GetUint64(list, "/sys/memory/total")

	fileSystems := metrics.FileSystems(list)
	for _, fileSystem := range fileSystems {
		prefix := "/sys/fs" + fileSystem
		size, _ := metrics.GetUint64(list, prefix+"/METRICS/size")
		free, _ := metrics.GetUint64(list, prefix+"/METRICS/free")
		result.Fss = append(
			result.Fss,
			FsStats{MountPoint: fileSystem, Size: size, Free: free})
	}
	return result
}

type rollUpFsStatsType struct {
	isNotEmpty    bool
	fsSizeBytes   cloudhealth.IVariable
	fsUsedBytes   cloudhealth.IVariable
	fsUsedPercent cloudhealth.FVariable
}

func (r *rollUpFsStatsType) IsEmpty() bool {
	return !r.isNotEmpty
}

func (r *RollUpStats) timeOk(t time.Time) bool {
	if r.IsEmpty() {
		return true
	}
	return r.roundTime(t).Equal(r.ts)
}

func (r *RollUpStats) add(s InstanceStats) {
	if !r.timeOk(s.Ts) {
		panic("Time not ok")
	}
	r.ts = r.roundTime(s.Ts)
	r.notEmpty = true
	r.cpuUsedPercent.Add(s.CPUUsedPercent())
	r.memoryFreeBytes.Add(s.MemoryFree)
	r.memorySizeBytes.Add(s.MemoryTotal)
	used, ok := s.MemoryUsedPercent()
	if ok {
		r.memoryUsedPercent.Add(used)
	}
	for _, fss := range s.Fss {
		rollupFs := r.fss[fss.MountPoint]
		if rollupFs == nil {
			rollupFs = &rollUpFsStatsType{}
			r.fss[fss.MountPoint] = rollupFs
		}
		rollupFs.isNotEmpty = true
		rollupFs.fsSizeBytes.Add(fss.Size)
		rollupFs.fsUsedBytes.Add(fss.Used())
		usedPercent, ok := fss.UsedPercent()
		if ok {
			rollupFs.fsUsedPercent.Add(usedPercent)
		}
	}
}

func (r *RollUpStats) clear() CloudHealthInstanceCall {
	if r.IsEmpty() {
		panic("Already empty")
	}
	instance := cloudhealth.InstanceData{
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
			InstanceId:    r.instanceId,
			MountPoint:    mountPoint,
			Ts:            r.ts,
			FsSizeBytes:   rollupFs.fsSizeBytes,
			FsUsedBytes:   rollupFs.fsUsedBytes,
			FsUsedPercent: rollupFs.fsUsedPercent,
		}
		fss = append(fss, fsData)
	}

	// Now clear everything
	r.notEmpty = false
	r.cpuUsedPercent.Clear()
	r.memoryFreeBytes.Clear()
	r.memorySizeBytes.Clear()
	r.memoryUsedPercent.Clear()
	for _, rollupFs := range r.fss {
		rollupFs.fsSizeBytes.Clear()
		rollupFs.fsUsedBytes.Clear()
		rollupFs.fsUsedPercent.Clear()
		rollupFs.isNotEmpty = false
	}
	return CloudHealthInstanceCall{
		Instance: instance,
		Fss:      fss,
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
