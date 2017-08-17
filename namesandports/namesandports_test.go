package namesandports_test

import (
	"github.com/Symantec/scotty/namesandports"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestAPI(t *testing.T) {
	Convey("With API", t, func() {
		var n namesandports.NamesAndPorts
		So(n.Copy(), ShouldBeNil)
		n.Add("health agent", 6910)
		n2 := n.Copy()
		n2.Add("scotty", 6980)
		So(n.HasPort(6980), ShouldBeFalse)
		So(n2.HasPort(6980), ShouldBeTrue)
	})
}
