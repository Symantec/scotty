package pool_test

import (
	"github.com/Symantec/scotty/lib/pool"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"testing"
	"time"
)

type fakeCloserType struct {
	mu         sync.Mutex
	closeCount int
}

func (f *fakeCloserType) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCount++
	return nil
}

func (f *fakeCloserType) NumClosed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCount
}

type fakeCloserResourceType struct {
	*pool.SingleResource
}

func newFakeCloserResource(f *fakeCloserType) *fakeCloserResourceType {
	return &fakeCloserResourceType{
		SingleResource: pool.NewSingleResource(f),
	}
}

func (f *fakeCloserResourceType) Get() (uint64, *fakeCloserType) {
	id, val := f.SingleResource.Get()
	return id, val.(*fakeCloserType)
}

func (f *fakeCloserResourceType) Set(closer *fakeCloserType) {
	f.SingleResource.Set(closer)
}

func TestSingleResource(t *testing.T) {
	Convey("With New Resource and closers", t, func() {
		first := &fakeCloserType{}
		second := &fakeCloserType{}
		third := &fakeCloserType{}
		resource := newFakeCloserResource(first)
		Convey("Getting and putting a closer shouldn't close it.", func() {
			id, fake := resource.Get()
			resource.Put(id)
			time.Sleep(time.Millisecond)
			So(fake.NumClosed(), ShouldEqual, 0)
			Convey("But changing the closer should close the first one.", func() {
				resource.Set(second)
				time.Sleep(time.Millisecond)
				So(fake.NumClosed(), ShouldEqual, 1)
			})
		})
		Convey("The closer gotten should be the last one set.", func() {
			id, fake := resource.Get()
			So(fake, ShouldEqual, first)
			resource.Put(id)
			resource.Set(second)
			id, fake = resource.Get()
			So(fake, ShouldEqual, second)
			resource.Put(id)
			resource.Set(third)
			id, fake = resource.Get()
			So(fake, ShouldEqual, third)
			resource.Put(id)
		})
		Convey("Changing the closer alone won't close previous ones", func() {
			id1, fake1 := resource.Get()
			resource.Set(second)
			id2, fake2 := resource.Get()
			resource.Set(third)
			id3, fake3 := resource.Get()
			So(fake1, ShouldEqual, first)
			So(fake2, ShouldEqual, second)
			So(fake3, ShouldEqual, third)
			time.Sleep(time.Millisecond)
			So(fake1.NumClosed(), ShouldEqual, 0)
			So(fake2.NumClosed(), ShouldEqual, 0)
			So(fake3.NumClosed(), ShouldEqual, 0)
			Convey("Putting closers will close all but current", func() {
				resource.Put(id1)
				resource.Put(id2)
				resource.Put(id3)
				time.Sleep(time.Millisecond)
				So(fake1.NumClosed(), ShouldEqual, 1)
				So(fake2.NumClosed(), ShouldEqual, 1)
				So(fake3.NumClosed(), ShouldEqual, 0)
			})

		})
	})
}
