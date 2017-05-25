package cloudhealthlmm

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

type nameValueType struct {
	Name  string
	Value float64
}

func (w *Writer) writeToLmm(
	accountNumber,
	instanceId string,
	ts time.Time,
	nameValues []nameValueType) error {
	records := make([]pstore.Record, len(nameValues))
	for i := range nameValues {
		records[i] = pstore.Record{
			HostName: instanceId,
			Path:     nameValues[i].Name,
			Tags: pstore.TagGroup{
				"region":        w.region,
				"accountNumber": accountNumber,
				"instanceId":    instanceId,
			},
			Kind:      types.Float64,
			Unit:      units.None,
			Value:     nameValues[i].Value,
			Timestamp: ts,
		}
	}
	return w.writer.Write(records)
}

func (w *Writer) write(s *chpipeline.Snapshot) error {
	nv := []nameValueType{
		{Name: CpuUsedPercentAvg, Value: s.CpuUsedPercent.Avg()},
		{Name: CpuUsedPercentMax, Value: s.CpuUsedPercent.Max},
		{Name: CpuUsedPercentMin, Value: s.CpuUsedPercent.Min},
		{Name: MemoryFreeBytesAvg, Value: float64(s.MemoryFreeBytes.Avg())},
		{Name: MemoryFreeBytesMax, Value: float64(s.MemoryFreeBytes.Max)},
		{Name: MemoryFreeBytesMin, Value: float64(s.MemoryFreeBytes.Min)},
		{Name: MemorySizeBytesAvg, Value: float64(s.MemorySizeBytes.Avg())},
		{Name: MemorySizeBytesMax, Value: float64(s.MemorySizeBytes.Max)},
		{Name: MemorySizeBytesMin, Value: float64(s.MemorySizeBytes.Min)},
		{Name: MemoryUsedPercentAvg, Value: s.MemoryUsedPercent.Avg()},
		{Name: MemoryUsedPercentMax, Value: s.MemoryUsedPercent.Max},
		{Name: MemoryUsedPercentMin, Value: s.MemoryUsedPercent.Min}}
	for i := range s.Fss {
		if s.Fss[i].MountPoint != "/" {
			continue
		}
		fsnv := []nameValueType{
			{Name: FsSizeBytesAvg, Value: float64(s.Fss[i].Size.Avg())},
			{Name: FsSizeBytesMax, Value: float64(s.Fss[i].Size.Max)},
			{Name: FsSizeBytesMin, Value: float64(s.Fss[i].Size.Min)},
			{Name: FsUsedBytesAvg, Value: float64(s.Fss[i].Used.Avg())},
			{Name: FsUsedBytesMax, Value: float64(s.Fss[i].Used.Max)},
			{Name: FsUsedBytesMin, Value: float64(s.Fss[i].Used.Min)},
			{Name: FsUsedPercentAvg, Value: s.Fss[i].UsedPercent.Avg()},
			{Name: FsUsedPercentMax, Value: s.Fss[i].UsedPercent.Max},
			{Name: FsUsedPercentMin, Value: s.Fss[i].UsedPercent.Min}}
		nv = append(nv, fsnv...)
		break
	}
	return w.writeToLmm(s.AccountNumber, s.InstanceId, s.Ts, nv)
}
