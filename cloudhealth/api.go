package cloudhealth

import (
	"github.com/Symantec/scotty/lib/yamlutil"
	"time"
)

const (
	// Default endpoint for cloudhealth service.
	DefaultEndpoint = "https://chapi.cloudhealthtech.com/metrics/v1"
)

// FVariable represents a floating point cloudfire variable rolled up over
// some amount of time
type FVariable struct {
	Count uint64
	Min   float64
	Max   float64
	Sum   float64
}

// IsEmpty returns true if no values have been added to this variable
func (v FVariable) IsEmpty() bool {
	return v.Count == 0
}

// Add adds x to this variable
func (v *FVariable) Add(x float64) {
	v.add(x)
}

// Clear clears out this variable.
func (v *FVariable) Clear() {
	*v = FVariable{}
}

// Avg returns the average of this variable
func (v FVariable) Avg() float64 {
	return v.Sum / float64(v.Count)
}

// IVariable represents an integer cloudfire variable rolled up over
// some amount of time
type IVariable struct {
	Count uint64
	Min   uint64
	Max   uint64
	Sum   uint64
}

// IsEmpty returns true if no values have been added to this variable
func (v IVariable) IsEmpty() bool {
	return v.Count == 0
}

// Add adds x to this variable
func (v *IVariable) Add(x uint64) {
	v.add(x)
}

// Clear clears out this variable.
func (v *IVariable) Clear() {
	*v = IVariable{}
}

// Avg returns the average of this variable
func (v IVariable) Avg() uint64 {
	return v.Sum / v.Count
}

const InstanceDataPointCount = 12 // 4 variables * (min,max,avg)

// InstanceData contains rolled up data for a particular instance
type InstanceData struct {
	AccountNumber     string    // account number if different from default
	InstanceId        string    // The aws instance ID
	Ts                time.Time // The timestamp at one hour granularity
	CpuUsedPercent    FVariable
	MemoryFreeBytes   IVariable
	MemorySizeBytes   IVariable
	MemoryUsedPercent FVariable
}

const FsDataPointCount = 9 // 3 variables * (min,max,avg)

// Maximum datapoints that can be written to cloudhealth at once
const MaxDataPoints = 1000

// FsData contains rolled up data for a particular file system
type FsData struct {
	AccountNumber string    // account number if different from default
	InstanceId    string    // the aws instance ID
	MountPoint    string    // The mount point of file system
	Ts            time.Time // The timestamp at one hour granularity
	FsSizeBytes   IVariable
	FsUsedBytes   IVariable
	FsUsedPercent FVariable
}

// Config configures the writer
type Config struct {
	ApiKey        string `yaml:"apiKey"`
	DataCenter    string `yaml:"dataCenter"`    // e.g us-east-1
	AccountNumber string `yaml:"accountNumber"` // default account number
	DryRun        bool   `yaml:"dryRun"`        // If true, runs in dry run mode

	// like "http://somehost.com:1234/endpoint" If omitted, defaults to
	// standard endpoint for cloudhealth.
	Endpoint string `yaml:"endpoint"`
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configFields Config
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*configFields)(c))
}

func (c *Config) Reset() {
	*c = Config{}
}

type Writer struct {
	config Config
}

// NewWriter returns a new Writer instance.
func NewWriter(config Config) *Writer {
	return newWriter(config)
}

// Write writes the provided data in a single request.
// Write returns an error if the number of data points exceeds 1000. That is,
// len(instances)*InstanceDataPointCount + len(fss)*FsDataPointCount >= 1000.
// Write returns the http response code along with an error if applicable.
// If the write is parital success, Write returns 200 along with the error.
// Write will never return a 429 response. If Write receives a 429, it
// retries using exponential backoff internally until it receives a non
// 429 response and returns that.
func (w *Writer) Write(
	instances []InstanceData, fss []FsData) (
	responseCode int, err error) {
	return w.write(instances, fss)
}
