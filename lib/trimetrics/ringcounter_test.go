package trimetrics

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAPI(t *testing.T) {
	Convey("API", t, func() {
		var total ringCounterType
		total.Init(5)
		total.Inc(3, 6)
		total.Inc(4, 2)
		total.Inc(4, 5)
		So(total.Total(4), ShouldEqual, 13)
		// Going back in time has no affect
		total.Inc(2, 102)
		So(total.Total(2), ShouldEqual, 13)
		// Starting at present time again has affect
		total.Inc(4, 1)
		So(total.Total(4), ShouldEqual, 14)
		total.Inc(5, 10)
		total.Inc(7, 4)
		So(total.Total(7), ShouldEqual, 28)
		// ring counter now full
		// Advancing to 9 evicts 3=6 and 4=8
		So(total.Total(9), ShouldEqual, 14)
		total.Inc(10, 3)
		// 7=4, 10=3 total 7
		So(total.Total(10), ShouldEqual, 7)
		// Test advancing more than 5
		total.Inc(17, 5)
		total.Inc(17, 3)
		So(total.Total(17), ShouldEqual, 8)
	})
}
