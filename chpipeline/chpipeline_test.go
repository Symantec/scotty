package chpipeline_test

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/metrics"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
	"time"
)

var (
	kToday = time.Date(2017, 5, 1, 0, 0, 0, 0, time.UTC)
)

func TestNoStats(t *testing.T) {
	Convey("With no stats", t, func() {
		var list metrics.SimpleList
		instanceStats := chpipeline.GetStats(list)
		Convey("CPUUsedPercent returns false", func() {
			_, ok := instanceStats.CPUUsedPercent()
			So(ok, ShouldBeFalse)
		})
		Convey("MemoryUsedPercent returns false", func() {
			_, ok := instanceStats.MemoryUsedPercent()
			So(ok, ShouldBeFalse)
		})
		Convey("No filesystems", func() {
			So(instanceStats.Fss, ShouldBeEmpty)
		})
	})

}

func TestMemoryCpuStats(t *testing.T) {
	Convey("With memory cpu stats", t, func() {
		list := metrics.SimpleList{
			{
				Path:  "/sys/memory/available",
				Value: uint64(3210000),
			},
			{
				Path:  "/sys/memory/free",
				Value: uint64(3330000),
			},
			{
				Path:  "/sys/memory/total",
				Value: uint64(4000000),
			},
			{
				Path:  "/sys/sched/cpu/user-time-fraction",
				Value: 0.0625,
			},
		}
		instanceStats := chpipeline.GetStats(list)
		Convey("CPUUsedPercent", func() {
			cpu, ok := instanceStats.CPUUsedPercent()
			So(cpu, ShouldEqual, 6.25)
			So(ok, ShouldBeTrue)
		})
		Convey("MemoryUsedPercent", func() {
			memory, ok := instanceStats.MemoryUsedPercent()
			So(memory, ShouldEqual, 19.75)
			So(ok, ShouldBeTrue)
		})
		Convey("No filesystems", func() {
			So(instanceStats.Fss, ShouldBeEmpty)
		})
	})
}

func TestFileSystemStats(t *testing.T) {
	Convey("With file system stats", t, func() {
		list := metrics.SimpleList{
			{
				Path:  "/sys/fs/METRICS/available",
				Value: uint64(6000000),
			},
			{
				Path:  "/sys/fs/METRICS/free",
				Value: uint64(8000000),
			},
			{
				Path:  "/sys/fs/METRICS/size",
				Value: uint64(10000000),
			},
			{
				Path:  "/sys/fs/boot/METRICS/available",
				Value: uint64(190000),
			},
			{
				Path:  "/sys/fs/boot/METRICS/free",
				Value: uint64(210000),
			},
			{
				Path:  "/sys/fs/mnt/METRICS/free",
				Value: uint64(5000000),
			},
			{
				Path:  "/sys/fs/mnt/METRICS/size",
				Value: uint64(6000000),
			},
		}
		Convey("Non combined file system stats", func() {
			stats := chpipeline.GetStats(list)
			expected := []chpipeline.FsStats{
				{
					MountPoint: "/",
					Size: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(10000000),
					},
					Free: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(8000000),
					},
				},
				{
					MountPoint: "/boot",
					Free: chpipeline.MaybeUint64{
						Ok:    true,
						Value: 210000,
					},
				},
				{
					MountPoint: "/mnt",
					Size: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(6000000),
					},
					Free: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(5000000),
					},
				},
			}
			So(expected, ShouldResemble, stats.Fss)
		})
		Convey("combined file system stats", func() {
			stats := chpipeline.GetStats(list).WithCombinedFsStats()
			expected := []chpipeline.FsStats{
				{
					MountPoint: "/",
					Size: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(16000000),
					},
					Free: chpipeline.MaybeUint64{
						Ok:    true,
						Value: uint64(13000000),
					},
				},
			}
			So(expected, ShouldResemble, stats.Fss)
		})
	})
}

func TestRollUp(t *testing.T) {
	Convey("With file system stats", t, func() {
		rollup := chpipeline.NewRollUpStats(
			"accountNumber", "instanceId", "us-east-1", time.Hour)
		stats1 := chpipeline.InstanceStats{
			Ts: kToday.Add(13 * time.Hour).Add(31 * time.Minute),
			UserTimeFraction: chpipeline.MaybeFloat64{
				Value: 0.375,
				Ok:    true,
			},
			MemoryFree: chpipeline.MaybeUint64{
				Value: 3500,
				Ok:    true,
			},
			MemoryTotal: chpipeline.MaybeUint64{
				Value: 5000,
				Ok:    true,
			},
			Fss: []chpipeline.FsStats{
				{
					MountPoint: "/",
					Size: chpipeline.MaybeUint64{
						Value: 7200,
						Ok:    true,
					},
					Free: chpipeline.MaybeUint64{
						Value: 4500,
						Ok:    true,
					},
				},
				{
					MountPoint: "/boot",
					Size: chpipeline.MaybeUint64{
						Value: 10000,
						Ok:    true,
					},
					Free: chpipeline.MaybeUint64{
						Value: 8125,
						Ok:    true,
					},
				},
			},
		}
		stats2 := chpipeline.InstanceStats{
			Ts: kToday.Add(14 * time.Hour),
			UserTimeFraction: chpipeline.MaybeFloat64{
				Value: 0.25,
				Ok:    true,
			},
			MemoryFree: chpipeline.MaybeUint64{
				Value: 3000,
				Ok:    true,
			},
			MemoryTotal: chpipeline.MaybeUint64{
				Value: 5000,
				Ok:    true,
			},
			Fss: []chpipeline.FsStats{
				{
					MountPoint: "/",
					Size: chpipeline.MaybeUint64{
						Value: 7200,
						Ok:    true,
					},
					Free: chpipeline.MaybeUint64{
						Value: 3600,
						Ok:    true,
					},
				},
				{
					MountPoint: "/boot",
					Size: chpipeline.MaybeUint64{
						Value: 10000,
						Ok:    true,
					},
					Free: chpipeline.MaybeUint64{
						Value: 7500,
						Ok:    true,
					},
				},
			},
		}
		stats3 := chpipeline.InstanceStats{
			Ts: kToday.Add(14 * time.Hour).Add(29 * time.Minute),
			MemoryTotal: chpipeline.MaybeUint64{
				Value: 5300,
				Ok:    true,
			},
			Fss: []chpipeline.FsStats{
				{
					MountPoint: "/",
					Size: chpipeline.MaybeUint64{
						Value: 7500,
						Ok:    true,
					},
				},
				{
					MountPoint: "/boot",
					Size: chpipeline.MaybeUint64{
						Value: 0,
						Ok:    true,
					},
					Free: chpipeline.MaybeUint64{
						Value: 7500,
						Ok:    true,
					},
				},
			},
		}
		stats4 := chpipeline.InstanceStats{
			Ts: kToday.Add(15 * time.Hour),
			MemoryTotal: chpipeline.MaybeUint64{
				Value: 8192,
				Ok:    true,
			},
		}
		So(rollup.TimeOk(stats1.Ts), ShouldBeTrue)
		rollup.Add(&stats1)
		So(rollup.TimeOk(stats2.Ts), ShouldBeTrue)
		rollup.Add(&stats2)
		So(rollup.TimeOk(stats3.Ts), ShouldBeTrue)
		rollup.Add(&stats3)
		So(rollup.TimeOk(kToday.Add(15*time.Hour)), ShouldBeFalse)
		So(func() { rollup.Add(&stats4) }, ShouldPanic)
		Convey("Rollup successful", func() {
			chInstanceCall := chpipeline.NewCloudHealthInstanceCall(
				rollup.TakeSnapshot())
			instance := chInstanceCall.Instance
			So(
				instance.AccountNumber,
				ShouldEqual,
				"accountNumber",
			)
			So(
				instance.InstanceId,
				ShouldEqual,
				"instanceId",
			)
			So(
				instance.Ts,
				ShouldResemble,
				kToday.Add(14*time.Hour))
			So(
				instance.CpuUsedPercent.Avg(),
				ShouldEqual,
				31.25)
			So(
				instance.MemoryFreeBytes.Avg(),
				ShouldEqual,
				3250)
			So(
				instance.MemorySizeBytes.Avg(),
				ShouldEqual,
				5100)
			So(
				instance.MemoryUsedPercent.Avg(),
				ShouldEqual,
				35.0)

			fss := chInstanceCall.Fss
			So(fss, ShouldHaveLength, 2)
			So(fss[0].AccountNumber, ShouldEqual, "accountNumber")
			So(fss[0].InstanceId, ShouldEqual, "instanceId")
			So(fss[0].MountPoint, ShouldEqual, "/")
			So(fss[0].Ts, ShouldResemble, kToday.Add(14*time.Hour))
			So(fss[0].FsSizeBytes.Avg(), ShouldEqual, 7300)
			So(fss[0].FsUsedBytes.Avg(), ShouldEqual, 3150)
			So(fss[0].FsUsedPercent.Avg(), ShouldEqual, 43.75)

			So(fss[1].AccountNumber, ShouldEqual, "accountNumber")
			So(fss[1].InstanceId, ShouldEqual, "instanceId")
			So(fss[1].MountPoint, ShouldEqual, "/boot")
			So(fss[1].Ts, ShouldResemble, kToday.Add(14*time.Hour))
			So(fss[1].FsSizeBytes.Avg(), ShouldAlmostEqual, 6667.0, 1.0)
			So(fss[1].FsUsedBytes.Avg(), ShouldEqual, 1458.0)
			So(fss[1].FsUsedPercent.Avg(), ShouldEqual, 21.875)
		})
		Convey("Clear works", func() {
			rollup.Clear()
			So(func() { rollup.TakeSnapshot() }, ShouldPanic)
			stats4 := chpipeline.InstanceStats{
				Ts: kToday.Add(15 * time.Hour),
				MemoryTotal: chpipeline.MaybeUint64{
					Value: 8192,
					Ok:    true,
				},
			}
			rollup.Add(&stats4)
			chInstanceCall := chpipeline.NewCloudHealthInstanceCall(
				rollup.TakeSnapshot())
			instance := chInstanceCall.Instance
			So(
				instance.MemorySizeBytes.Avg(),
				ShouldEqual,
				8192)
		})
	})
}

func TestSplit(t *testing.T) {
	Convey("split", t, func() {
		// enough to ensure overlfow
		fss := make([]cloudhealth.FsData, cloudhealth.MaxDataPoints)
		for i := range fss {
			fss[i].MountPoint = strconv.Itoa(i)
		}
		call := chpipeline.CloudHealthInstanceCall{
			Fss: fss,
		}
		splitCall, splitFss := call.Split()
		So(len(splitCall.Fss)*cloudhealth.FsDataPointCount+cloudhealth.InstanceDataPointCount,
			ShouldBeLessThanOrEqualTo,
			cloudhealth.MaxDataPoints)
		for _, splitFs := range splitFss {
			So(len(splitFs)*cloudhealth.FsDataPointCount,
				ShouldBeLessThanOrEqualTo,
				cloudhealth.MaxDataPoints)
		}
		var concat []cloudhealth.FsData
		// concatenate all the splits.
		concat = append(concat, splitCall.Fss...)
		for _, splitFs := range splitFss {
			concat = append(concat, splitFs...)
		}
		// concatenated splits should equal original
		So(concat, ShouldResemble, call.Fss)
	})

	Convey("No split", t, func() {
		// no overlfow
		fss := make([]cloudhealth.FsData, 1)
		for i := range fss {
			fss[i].MountPoint = strconv.Itoa(i)
		}
		call := chpipeline.CloudHealthInstanceCall{
			Fss: fss,
		}
		splitCall, splitFss := call.Split()
		So(splitCall, ShouldResemble, call)
		So(splitFss, ShouldBeEmpty)
	})
}
