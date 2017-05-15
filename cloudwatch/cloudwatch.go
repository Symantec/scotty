package cloudwatch

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

var (
	kBytes                 = aws.String("Bytes")
	kCpuUsedPercent        = aws.String("CpuUsedPercent")
	kFileSystemSize        = aws.String("FileSystemSize")
	kFileSystemUsed        = aws.String("FileSystemUsed")
	kFileSystemUsedPercent = aws.String("FileSystemUsedPercent")
	kInstanceId            = aws.String("InstanceId")
	kMemoryFreeBytes       = aws.String("MemoryFreeBytes")
	kMemorySizeBytes       = aws.String("MemorySizeBytes")
	kMemoryUsedPercent     = aws.String("MemoryUsedPercent")
	kPercent               = aws.String("Percent")
	kScotty                = aws.String("Scotty")
)

func toPutMetricData(
	snapshot *chpipeline.Snapshot) *cloudwatch.PutMetricDataInput {
	instanceIdDim := &cloudwatch.Dimension{
		Name:  kInstanceId,
		Value: aws.String(snapshot.InstanceId),
	}
	instanceIdDims := []*cloudwatch.Dimension{
		instanceIdDim,
	}
	timestamp := aws.Time(snapshot.Ts)
	var metricData []*cloudwatch.MetricDatum
	if !snapshot.CpuUsedPercent.IsEmpty() {
		cpuUsedPercent := &cloudwatch.MetricDatum{
			MetricName: kCpuUsedPercent,
			Dimensions: instanceIdDims,
			Timestamp:  timestamp,
			Unit:       kPercent,
			Value:      aws.Float64(snapshot.CpuUsedPercent.Avg()),
		}
		metricData = append(metricData, cpuUsedPercent)
	}
	if !snapshot.MemoryUsedPercent.IsEmpty() {
		memoryUsedPercent := &cloudwatch.MetricDatum{
			MetricName: kMemoryUsedPercent,
			Dimensions: instanceIdDims,
			Timestamp:  timestamp,
			Unit:       kPercent,
			Value:      aws.Float64(snapshot.MemoryUsedPercent.Avg()),
		}
		metricData = append(metricData, memoryUsedPercent)
	}
	if len(snapshot.Fss) > 1 {
		panic("Can't have multiple file systems")
	}
	if len(snapshot.Fss) == 1 {
		fss := snapshot.Fss[0]
		if !fss.UsedPercent.IsEmpty() {
			usedPercentMetric := &cloudwatch.MetricDatum{
				MetricName: kFileSystemUsedPercent,
				Dimensions: instanceIdDims,
				Timestamp:  timestamp,
				Unit:       kPercent,
				Value:      aws.Float64(fss.UsedPercent.Avg()),
			}
			metricData = append(metricData, usedPercentMetric)
		}
	}
	return &cloudwatch.PutMetricDataInput{
		MetricData: metricData,
		Namespace:  kScotty,
	}
}
