// cloudhealthlmm contains routines for writing cloudhealth data to lmm.
package cloudhealthlmm

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/kafka"
)

// Metric names used when writing to lmm
const (
	CpuUsedPercentAvg = "CpuUsedPercentAvg"
	CpuUsedPercentMax = "CpuUsedPercentMax"
	CpuUsedPercentMin = "CpuUsedPercentMin"

	MemoryFreeBytesAvg = "MemoryFreeBytesAvg"
	MemoryFreeBytesMax = "MemoryFreeBytesMax"
	MemoryFreeBytesMin = "MemoryFreeBytesMin"

	MemorySizeBytesAvg = "MemorySizeBytesAvg"
	MemorySizeBytesMax = "MemorySizeBytesMax"
	MemorySizeBytesMin = "MemorySizeBytesMin"

	MemoryUsedPercentAvg = "MemoryUsedPercentAvg"
	MemoryUsedPercentMax = "MemoryUsedPercentMax"
	MemoryUsedPercentMin = "MemoryUsedPercentMin"

	FsSizeBytesAvg = "FsSizeBytesAvg"
	FsSizeBytesMax = "FsSizeBytesMax"
	FsSizeBytesMin = "FsSizeBytesMin"

	FsUsedBytesAvg = "FsUsedBytesAvg"
	FsUsedBytesMax = "FsUsedBytesMax"
	FsUsedBytesMin = "FsUsedBytesMin"

	FsUsedPercentAvg = "FsUsedPercentAvg"
	FsUsedPercentMax = "FsUsedPercentMax"
	FsUsedPercentMin = "FsUsedPercentMin"
)

// Config is the configuration for writing to cloudhealth data to lmm
type Config struct {
	Kafka  kafka.Config `yaml:"kafka"`  // standard lmm configuration
	Region string       `yaml:"region"` // like "us-east-1"
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configFields Config
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*configFields)(c))
}

func (c *Config) Reset() {
	*c = Config{}
}

// Writer writes cloudhealth data to lmm
type Writer struct {
	region string
	writer pstore.RecordWriter
}

// NewWriter returns a new Writer instance.
func NewWriter(c Config) (*Writer, error) {
	writer, err := c.Kafka.NewWriter()
	if err != nil {
		return nil, err
	}
	return &Writer{region: c.Region, writer: writer}, nil
}

// Write writes a snapshot to LMM
func (w *Writer) Write(snapshot *chpipeline.Snapshot) error {
	return w.write(snapshot)
}
