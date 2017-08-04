package chpipeline_test

import (
	"github.com/Symantec/scotty/chpipeline"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	"time"
)

func TestPersistence(t *testing.T) {
	today := time.Date(2017, 5, 1, 0, 0, 0, 0, time.UTC)
	baseDir := "/tmp/scotty"
	firstS := chpipeline.NewSnapshotStore(baseDir, "host1", "app1", 5*time.Hour)
	secondS := chpipeline.NewSnapshotStore(baseDir, "host1", "app2", 5*time.Hour)
	thirdS := chpipeline.NewSnapshotStore(baseDir, "host2", "app1", 5*time.Hour)
	fourthS := chpipeline.NewSnapshotStore(baseDir, "host2", "app2", 5*time.Hour)
	Convey("With empty directory", t, func() {
		So(os.RemoveAll("/tmp/scotty"), ShouldBeNil)
		So(os.Mkdir("/tmp/scotty", 0755), ShouldBeNil)
		Convey("Loading from empty directory gives error.", func() {
			So(firstS.Load(), ShouldNotBeNil)
		})
		Convey("Data shall persist", func() {
			firstS.Add(&chpipeline.Snapshot{Ts: today})
			firstS.Add(&chpipeline.Snapshot{Ts: today.Add(time.Hour)})
			secondS.Add(&chpipeline.Snapshot{Ts: today.Add(10 * time.Hour)})
			secondS.Add(&chpipeline.Snapshot{Ts: today.Add(11 * time.Hour)})
			secondS.Add(&chpipeline.Snapshot{Ts: today.Add(12 * time.Hour)})
			thirdS.Add(&chpipeline.Snapshot{Ts: today.Add(20 * time.Hour)})
			So(firstS.Save(), ShouldBeNil)
			So(secondS.Save(), ShouldBeNil)
			So(thirdS.Save(), ShouldBeNil)
			So(fourthS.Save(), ShouldBeNil)
			firstList := firstS.GetAll()
			secondList := secondS.GetAll()
			thirdList := thirdS.GetAll()
			fourthList := fourthS.GetAll()

			Convey("Loading data should preserve persistence", func() {
				firstS := chpipeline.NewSnapshotStore(baseDir, "host1", "app1", 5*time.Hour)
				secondS := chpipeline.NewSnapshotStore(baseDir, "host1", "app2", 5*time.Hour)
				thirdS := chpipeline.NewSnapshotStore(baseDir, "host2", "app1", 5*time.Hour)
				fourthS := chpipeline.NewSnapshotStore(baseDir, "host2", "app2", 5*time.Hour)
				So(firstS.Load(), ShouldBeNil)
				So(secondS.Load(), ShouldBeNil)
				So(thirdS.Load(), ShouldBeNil)
				So(fourthS.Load(), ShouldBeNil)

				So(firstS.GetAll(), ShouldResemble, firstList)
				So(secondS.GetAll(), ShouldResemble, secondList)
				So(thirdS.GetAll(), ShouldResemble, thirdList)
				So(fourthS.GetAll(), ShouldResemble, fourthList)

			})
		})
	})
}

func TestCircularBuffer(t *testing.T) {
	today := time.Date(2017, 5, 1, 0, 0, 0, 0, time.UTC)
	Convey("With snapshot store", t, func() {
		store := chpipeline.NewSnapshotStore(
			"unusedDirPath", "unusedHostName", "unusedAppName", 5*time.Hour)
		So(store.GetAll(), ShouldHaveLength, 0)
		store.Add(&chpipeline.Snapshot{Ts: today})
		So(store.GetAll(), ShouldHaveLength, 1)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(4 * time.Hour)})
		So(store.GetAll(), ShouldHaveLength, 2)
		So(func() {
			store.Add(&chpipeline.Snapshot{Ts: today.Add(3 * time.Hour)})
		}, ShouldPanic)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(4 * time.Hour)})
		So(store.GetAll(), ShouldHaveLength, 3)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(5 * time.Hour)})
		So(store.GetAll(), ShouldHaveLength, 4)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(6 * time.Hour)})
		// Today snapshot gets evicted as it is > 5 hours past
		So(store.GetAll(), ShouldHaveLength, 4)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(9 * time.Hour)})
		So(store.GetAll(), ShouldHaveLength, 5)
		store.Add(&chpipeline.Snapshot{Ts: today.Add(10 * time.Hour)})
		// both instances of Today + 4h get evicted.
		So(store.GetAll(), ShouldHaveLength, 4)
		snapshots := store.GetAll()
		So(snapshots[0].Ts, ShouldResemble, today.Add(5*time.Hour))
		So(snapshots[1].Ts, ShouldResemble, today.Add(6*time.Hour))
		So(snapshots[2].Ts, ShouldResemble, today.Add(9*time.Hour))
		So(snapshots[3].Ts, ShouldResemble, today.Add(10*time.Hour))

		// More than 5 hours past today+10 hours
		store.Add(&chpipeline.Snapshot{Ts: today.Add(16 * time.Hour)})
		So(store.GetAll(), ShouldHaveLength, 1)
	})
}
