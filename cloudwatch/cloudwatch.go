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
	kMountPoint            = aws.String("MountPoint")
	kPercent               = aws.String("Percent")
	kScotty                = aws.String("scotty")
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
	if !snapshot.MemoryFreeBytes.IsEmpty() {
		memoryFreeBytes := &cloudwatch.MetricDatum{
			MetricName: kMemoryFreeBytes,
			Dimensions: instanceIdDims,
			Timestamp:  timestamp,
			Unit:       kBytes,
			Value:      aws.Float64(float64(snapshot.MemoryFreeBytes.Avg())),
		}
		metricData = append(metricData, memoryFreeBytes)
	}
	if !snapshot.MemorySizeBytes.IsEmpty() {
		memorySizeBytes := &cloudwatch.MetricDatum{
			MetricName: kMemorySizeBytes,
			Dimensions: instanceIdDims,
			Timestamp:  timestamp,
			Unit:       kBytes,
			Value:      aws.Float64(float64(snapshot.MemorySizeBytes.Avg())),
		}
		metricData = append(metricData, memorySizeBytes)
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
	for _, fss := range snapshot.Fss {
		instanceIdMountPointDims := []*cloudwatch.Dimension{
			instanceIdDim,
			{
				Name:  kMountPoint,
				Value: aws.String(fss.MountPoint),
			},
		}
		if !fss.Size.IsEmpty() {
			sizeMetric := &cloudwatch.MetricDatum{
				MetricName: kFileSystemSize,
				Dimensions: instanceIdMountPointDims,
				Timestamp:  timestamp,
				Unit:       kBytes,
				Value:      aws.Float64(float64(fss.Size.Avg())),
			}
			metricData = append(metricData, sizeMetric)
		}
		if !fss.Used.IsEmpty() {
			usedMetric := &cloudwatch.MetricDatum{
				MetricName: kFileSystemUsed,
				Dimensions: instanceIdMountPointDims,
				Timestamp:  timestamp,
				Unit:       kBytes,
				Value:      aws.Float64(float64(fss.Used.Avg())),
			}
			metricData = append(metricData, usedMetric)
		}
		if !fss.UsedPercent.IsEmpty() {
			usedPercentMetric := &cloudwatch.MetricDatum{
				MetricName: kFileSystemUsedPercent,
				Dimensions: instanceIdMountPointDims,
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
