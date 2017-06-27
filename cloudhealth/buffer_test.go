package cloudhealth_test

import (
	"github.com/Symantec/scotty/cloudhealth"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func TestBuffer(t *testing.T) {
	Convey("With new buffer", t, func() {
		buffer := cloudhealth.NewBuffer()
		So(buffer.IsEmpty(), ShouldBeTrue)
		instance := cloudhealth.InstanceData{AccountNumber: "0"}
		fs := cloudhealth.FsData{AccountNumber: "0"}
		idx := 0
		var allInstances []cloudhealth.InstanceData
		var allFs []cloudhealth.FsData
		for buffer.Add(instance, []cloudhealth.FsData{fs}) {
			allInstances = append(allInstances, instance)
			allFs = append(allFs, fs)
			idx++
			instance = cloudhealth.InstanceData{
				AccountNumber: strconv.Itoa(idx)}
			fs = cloudhealth.FsData{
				AccountNumber: strconv.Itoa(idx)}
		}
		Convey("With full buffer", func() {
			So(buffer.IsEmpty(), ShouldBeFalse)
			instances, fss := buffer.Get()
			datapointCount := len(instances)*cloudhealth.InstanceDataPointCount + len(fss)*cloudhealth.FsDataPointCount

			// data point count in buffer should be just less than max
			// adding again will exceed.
			So(datapointCount, ShouldBeLessThanOrEqualTo, cloudhealth.MaxDataPoints)
			So(datapointCount+cloudhealth.InstanceDataPointCount+cloudhealth.FsDataPointCount, ShouldBeGreaterThan, cloudhealth.MaxDataPoints)

			// What is in buffer should be what was added
			So(instances, ShouldResemble, allInstances)
			So(fss, ShouldResemble, allFs)
		})
		buffer.Clear()
		Convey("With empty buffer", func() {
			So(buffer.IsEmpty(), ShouldBeTrue)
		})
	})
}
