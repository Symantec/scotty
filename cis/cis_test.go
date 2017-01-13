package cis_test

import (
	"github.com/Symantec/scotty/cis"
	"github.com/Symantec/scotty/metrics"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var (
	someTime = time.Date(2017, 1, 3, 23, 10, 56, 0, time.UTC)
)

func checkNoInstanceId(list metrics.SimpleList) {
	Convey("No instance ID gives nil", func() {
		list = list.Sorted()
		index := metrics.Find(list, "/sys/cloud/aws/instance-id")
		list[index].Path = "/sys/cloud/aws/instance-id_"
		So(cis.GetStats(list.Sorted()), ShouldBeNil)
	})
}

func checkExtraManagementType(
	list metrics.SimpleList, lastResult *cis.Stats) {
	Convey("Extra management types ignored", func() {
		// defensive copy
		list = list.Sorted()
		newPackage("/sys/packages/ubuntu/xyz", 199, "0.1", &list)
		So(cis.GetStats(list.Sorted()), ShouldResemble, lastResult)
	})
}

func checkExtraStuffAtEnd(
	list metrics.SimpleList, lastResult *cis.Stats) {
	Convey("Extra stuff after packages ignored", func() {
		// defensive copy
		list = list.Sorted()
		list = append(
			list,
			metrics.Value{
				Path:  "/sys/xyz",
				Value: "abcd",
			})
		So(cis.GetStats(list.Sorted()), ShouldResemble, lastResult)
	})
}

func TestGetStats(t *testing.T) {

	Convey("No metrics gives nil", t, func() {
		var list metrics.SimpleList
		So(cis.GetStats(list), ShouldBeNil)
	})

	Convey("InstanceId with no packages gives nil", t, func() {
		var list metrics.SimpleList
		list = append(list, newInstanceId("i-487"))
		So(cis.GetStats(list.Sorted()), ShouldBeNil)
	})

	Convey("With packages", t, func() {
		var list metrics.SimpleList
		var lastResult *cis.Stats
		list = append(list, newInstanceId("i-487"))

		Convey("With one package", func() {
			newPackage("/sys/packages/debs/apt", 623, "1.0", &list)
			lastResult = cis.GetStats(list.Sorted())
			So(
				lastResult,
				ShouldResemble,
				&cis.Stats{
					TimeStamp:  someTime,
					InstanceId: "i-487",
					Packages: cis.PackageInfo{
						ManagementType: "debs",
						Packages: []cis.PackageEntry{
							{
								Name:    "apt",
								Version: "1.0",
								Size:    623,
							},
						},
					},
				})
			checkNoInstanceId(list)
			checkExtraManagementType(list, lastResult)
			checkExtraStuffAtEnd(list, lastResult)
		})

		Convey("With many packages", func() {
			newPackage("/sys/packages/debs/apt", 623, "1.0", &list)
			newPackage("/sys/packages/debs/bash", 924, "2.0", &list)
			newPackage("/sys/packages/debs/collectd", 187, "0.5", &list)
			lastResult = cis.GetStats(list.Sorted())
			So(
				lastResult,
				ShouldResemble,
				&cis.Stats{
					TimeStamp:  someTime,
					InstanceId: "i-487",
					Packages: cis.PackageInfo{
						ManagementType: "debs",
						Packages: []cis.PackageEntry{
							{
								Name:    "apt",
								Version: "1.0",
								Size:    623,
							},
							{
								Name:    "bash",
								Version: "2.0",
								Size:    924,
							},
							{
								Name:    "collectd",
								Version: "0.5",
								Size:    187,
							},
						},
					},
				})
			checkNoInstanceId(list)
			checkExtraManagementType(list, lastResult)
			checkExtraStuffAtEnd(list, lastResult)
		})

		Convey("With incomplete packages", func() {
			list = append(
				list,
				metrics.Value{
					Path:      "/sys/packages/debs/apt/nothing",
					Value:     int64(123),
					TimeStamp: someTime,
				},
				metrics.Value{
					Path:      "/sys/packages/debs/bash/version",
					Value:     "3.1",
					TimeStamp: someTime,
				},
				metrics.Value{
					Path:      "/sys/packages/debs/nothing",
					Value:     "123",
					TimeStamp: someTime,
				},
				metrics.Value{
					Path:      "/sys/packages/debs/something/size",
					Value:     int64(998),
					TimeStamp: someTime,
				},
			)
			lastResult = cis.GetStats(list.Sorted())
			So(
				lastResult,
				ShouldResemble,
				&cis.Stats{
					TimeStamp:  someTime,
					InstanceId: "i-487",
					Packages: cis.PackageInfo{
						ManagementType: "debs",
						Packages: []cis.PackageEntry{
							{
								Name: "apt",
							},
							{
								Name:    "bash",
								Version: "3.1",
							},
							{
								Name: "something",
							},
						},
					},
				})
			checkNoInstanceId(list)
			checkExtraManagementType(list, lastResult)
			checkExtraStuffAtEnd(list, lastResult)
		})

	})

}

func newInstanceId(instanceId string) metrics.Value {
	return metrics.Value{
		Path:  "/sys/cloud/aws/instance-id",
		Value: instanceId,
	}
}

func newPackage(
	path string,
	size uint64,
	version string,
	list *metrics.SimpleList) {
	*list = append(
		*list,
		metrics.Value{
			Path:      path + "/size",
			Value:     size,
			TimeStamp: someTime,
		},
		metrics.Value{
			Path:      path + "/version",
			Value:     version,
			TimeStamp: someTime,
		},
	)
}
