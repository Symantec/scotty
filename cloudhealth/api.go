package cloudhealth

import (
	"time"
)

// Variable represents a cloudfire variable rolled up over some amount of time
type Variable struct {
	Count uint64
	Min   float64
	Max   float64
	Sum   float64
}

// IsEmpty returns true if no values have been added to this variable
func (v Variable) IsEmpty() bool {
	return v.Count == 0
}

// Add returns this variable with a new value added
func (v Variable) Add(x float64) Variable {
	return Variable{}
}

// Avg returns the average of this variable
func (v Variable) Avg() float64 {
	return v.Sum / float64(v.Count)
}

const InstanceDataPointCount = 12 // 4 variables * (min,max,avg)

// InstanceData contains rolled up data for a particular instance
type InstanceData struct {
	InstanceId        string    // The aws instance ID
	Ts                time.Time // The timestamp at one hour granularity
	CpuUsedPercent    Variable
	MemoryFreeBytes   Variable
	MemorySizeBytes   Variable
	MemoryUsedPercent Variable
}

const FSDataPointCount = 9 // 3 variables * (min,max,avg)

// FsData contains rolled up data for a particular file system
type FsData struct {
	InstanceId    string    // the aws instance ID
	MountPoint    string    // The mount point of file system
	Ts            time.Time // The timestamp at one hour granularity
	FsSizeBytes   Variable
	FsUsedBytes   Variable
	FsUsedPercent Variable
}

// Config configures the writer
type Config struct {
	ApiKey        string
	DataCenter    string // e.g us-east-1
	AccountNumber string // 8 digit AWS account number
	DryRun        bool   // If true, runs in dry run mode
}

type Writer struct {
	config Config
}

// Write writes the provided data in a single request.
// Write panics if the number of data points exceeds 1000. That is,
// len(instances)*InstanceDataPointCount + len(fss)*FsDataPointCount >= 1000.
// Write returns the http response code along with an error if applicable.
// If the write is parital success, Write returns 200 along with the error.
// Write will never return a 429 response. If Write receives a 429, it
// retries using exponential backoff internally until it receives a non
// 429 response and returns that.
func (w *Writer) Write(
	instances []InstanceData, fss []FsData) (
	responseCode int, err error) {
	return
}
