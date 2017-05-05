package cloudhealth_test

import (
	"github.com/Symantec/scotty/cloudhealth"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestFVariable(t *testing.T) {
	Convey("FVariable", t, func() {
		var v cloudhealth.FVariable
		So(v.IsEmpty(), ShouldBeTrue)
		v.Add(7.25)
		So(v.Avg(), ShouldEqual, 7.25)
		So(v.Min, ShouldEqual, 7.25)
		So(v.Max, ShouldEqual, 7.25)
		v.Add(-2.25)
		v.Add(1.75)
		So(v.Avg(), ShouldEqual, 2.25)
		So(v.Min, ShouldEqual, -2.25)
		So(v.Max, ShouldEqual, 7.25)
		v.Clear()
		So(v.IsEmpty(), ShouldBeTrue)
	})
}

func TestIVariable(t *testing.T) {
	Convey("IVariable", t, func() {
		var v cloudhealth.IVariable
		So(v.IsEmpty(), ShouldBeTrue)
		v.Add(7)
		So(v.Avg(), ShouldEqual, 7)
		So(v.Min, ShouldEqual, 7)
		So(v.Max, ShouldEqual, 7)
		v.Add(19)
		v.Add(10)
		So(v.Avg(), ShouldEqual, 12)
		So(v.Min, ShouldEqual, 7)
		So(v.Max, ShouldEqual, 19)
		v.Clear()
		So(v.IsEmpty(), ShouldBeTrue)
	})
}
