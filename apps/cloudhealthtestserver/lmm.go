package main

import (
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/scotty/lib/dynconfig"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"io"
	"path"
)

type lmmWriterType struct {
	writer pstore.RecordWriter
}

func (w *lmmWriterType) Write(metrics []lmmMetricType) error {
	records := make([]pstore.Record, len(metrics))
	for i := range metrics {
		records[i] = pstore.Record{
			HostName: metrics[i].InstanceId,
			Path:     metrics[i].MetricName,
			Tags: pstore.TagGroup{
				"region":        metrics[i].Region,
				"accountNumber": metrics[i].AccountNumber,
				"instanceId":    metrics[i].InstanceId,
			},
			Kind:      types.Float64,
			Unit:      units.None,
			Value:     metrics[i].Value,
			Timestamp: metrics[i].Date,
		}
	}
	return w.writer.Write(records)
}

func buildLmmWriterType(reader io.Reader) (interface{}, error) {
	var c kafka.Config
	if err := yamlutil.Read(reader, &c); err != nil {
		return nil, err
	}
	writer, err := c.NewWriter()
	if err != nil {
		return nil, err
	}
	return &lmmWriterType{writer: writer}, nil
}

func newLmmConfig(
	configDir string, logger log.Logger) (*dynconfig.DynConfig, error) {
	return dynconfig.NewInitialized(
		path.Join(configDir, "lmm.yaml"),
		buildLmmWriterType,
		"lmm",
		logger)
}
