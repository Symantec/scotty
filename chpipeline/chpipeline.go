package chpipeline

import (
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	"time"
)

func getStats(list metrics.List) InstanceStats {
	var result InstanceStats
	// TODO: Maybe use time from remote host instead?
	result.Ts = time.Now().UTC()

	result.UserTimeFraction, _ = metrics.GetFloat64(
		list, "/sys/sched/cpu/user-time-fraction")
	result.MemoryFree, _ = metrics.GetUint64(list, "/sys/memory/free")
	result.MemoryTotal, _ = metrics.GetUint64(list, "/sys/memory/total")

	// TODO: This just gets the root file system. We need to report
	// all filesystems.
	size, _ := metrics.GetUint64(list, "/sys/fs/METRICS/size")
	free, _ := metrics.GetUint64(list, "/sys/fs/METRICS/free")
	if size > 0 {
		result.Fss = []FsStats{{MountPoint: "/", Size: size, Free: free}}
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
	return roundToHour(t).Equal(r.ts)
}

func (r *RollUpStats) add(s InstanceStats) {
	if !r.timeOk(s.Ts) {
		panic("Time not ok")
	}
	r.ts = roundToHour(s.Ts)
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

func (r *RollUpStats) clear() CloudHealthCall {
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
	return CloudHealthCall{
		Instances: []cloudhealth.InstanceData{instance},
		Fss:       fss,
	}
}

func roundToHour(t time.Time) time.Time {
	return t.UTC().Round(time.Hour)
}
