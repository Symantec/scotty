package keyedqueue_test

import (
	"github.com/Symantec/scotty/lib/keyedqueue"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"testing"
	"time"
)

type element struct {
	K string
	V int
}

func (e *element) Key() interface{} {
	return e.K
}

func TestQueue(t *testing.T) {

	Convey("With new queue", t, func() {
		queue := keyedqueue.New()

		Convey("Remove blocks until element added", func() {
			var wg sync.WaitGroup
			var result *element
			wg.Add(1)
			go func() {
				result = queue.Remove().(*element)
				wg.Done()
			}()
			// Wait a bit to help ensure that the call to Remove is blocking.
			time.Sleep(100 * time.Millisecond)
			queue.Add(&element{K: "A"})
			wg.Wait()
			So(result.K, ShouldEqual, "A")
		})

		Convey("With A,B,C,D added", func() {

			queue.Add(&element{K: "A"})
			queue.Add(&element{K: "B"})
			queue.Add(&element{K: "C"})
			queue.Add(&element{K: "D"})

			Convey("A,B,C,D removed", func() {
				So(queue.Len(), ShouldEqual, 4)
				So(queue.Remove().(*element).K, ShouldEqual, "A")
				So(queue.Remove().(*element).K, ShouldEqual, "B")
				So(queue.Remove().(*element).K, ShouldEqual, "C")
				So(queue.Remove().(*element).K, ShouldEqual, "D")
				So(queue.Len(), ShouldEqual, 0)
			})

			Convey("With D' and B' added", func() {
				queue.Add(&element{K: "D", V: 1})
				queue.Add(&element{K: "B", V: 1})

				Convey("A, B', C, D' removed", func() {
					So(queue.Len(), ShouldEqual, 4)
					res := queue.Remove().(*element)
					So(res.K, ShouldEqual, "A")
					So(res.V, ShouldEqual, 0)
					res = queue.Remove().(*element)
					So(res.K, ShouldEqual, "B")
					So(res.V, ShouldEqual, 1)
					res = queue.Remove().(*element)
					So(res.K, ShouldEqual, "C")
					So(res.V, ShouldEqual, 0)
					res = queue.Remove().(*element)
					So(res.K, ShouldEqual, "D")
					So(res.V, ShouldEqual, 1)
					So(queue.Len(), ShouldEqual, 0)
				})
			})
		})
	})
}
