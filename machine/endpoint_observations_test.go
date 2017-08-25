package machine_test

import (
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/namesandports"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestEndpointObservations(t *testing.T) {
	Convey("Test API", t, func() {
		eos := machine.NewEndpointObservations()
		eos.Save(
			"host1",
			namesandports.NamesAndPorts{
				"app1": 6911,
				"app2": 6912,
			})
		eos.Save("host2", nil)
		eos.Save("host2", nil)
		So(eos.GetAll(), ShouldResemble, map[string]machine.EndpointObservation{
			"host1": machine.EndpointObservation{
				SeqNo: 1,
				Endpoints: namesandports.NamesAndPorts{
					"app1": 6911,
					"app2": 6912,
				},
			},
			"host2": machine.EndpointObservation{
				SeqNo:     2,
				Endpoints: nil,
			},
		})
		eos.MaybeAddApp("host2", "scotty", 6980)
		So(eos.GetAll(), ShouldResemble, map[string]machine.EndpointObservation{
			"host1": machine.EndpointObservation{
				SeqNo: 1,
				Endpoints: namesandports.NamesAndPorts{
					"app1": 6911,
					"app2": 6912,
				},
			},
			"host2": machine.EndpointObservation{
				SeqNo:     2,
				Endpoints: namesandports.NamesAndPorts{"scotty": 6980},
			},
		})
		eos.MaybeAddApp("host1", "scotty", 6980)
		So(eos.GetAll(), ShouldResemble, map[string]machine.EndpointObservation{
			"host1": machine.EndpointObservation{
				SeqNo: 1,
				Endpoints: namesandports.NamesAndPorts{
					"app1":   6911,
					"app2":   6912,
					"scotty": 6980,
				},
			},
			"host2": machine.EndpointObservation{
				SeqNo:     2,
				Endpoints: namesandports.NamesAndPorts{"scotty": 6980},
			},
		})
		eos.MaybeAddApp("host1", "do", 6980)
		eos.MaybeAddApp("host2", "re", 6980)
		eos.MaybeAddApp("host3", "mi", 6980)
		So(eos.GetAll(), ShouldResemble, map[string]machine.EndpointObservation{
			"host1": machine.EndpointObservation{
				SeqNo: 1,
				Endpoints: namesandports.NamesAndPorts{
					"app1":   6911,
					"app2":   6912,
					"scotty": 6980,
				},
			},
			"host2": machine.EndpointObservation{
				SeqNo:     2,
				Endpoints: namesandports.NamesAndPorts{"scotty": 6980},
			},
		})
	})
}
