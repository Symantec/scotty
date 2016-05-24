package influx

import (
	"errors"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/kafka"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/influxdata/influxdb/client/v2"
)

const (
	kTagVersion = "@version"
	kTagHost    = "host"
	kFieldName  = "name"
	kFieldValue = "value"
	kVersionNum = "1"
)

type writer struct {
	client      client.Client
	batchConfig client.BatchPointsConfig
}

func newWriter(c Config) (
	result pstore.LimitedRecordWriter, err error) {
	if c.HostAndPort == "" || c.Database == "" {
		err = errors.New(
			"HostAndPort and Database fields required.")
		return
	}
	var config client.HTTPConfig
	config.Addr = c.HostAndPort
	config.Username = c.UserName
	config.Password = c.Password
	aClient, err := client.NewHTTPClient(config)
	if err != nil {
		return
	}
	w := &writer{
		client: aClient,
		batchConfig: client.BatchPointsConfig{
			Database:         c.Database,
			Precision:        c.Precision,
			RetentionPolicy:  c.RetentionPolicy,
			WriteConsistency: c.WriteConsistency,
		},
	}
	result = w
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return kafka.IsTypeSupported(t)
}

func (w *writer) Write(records []pstore.Record) (err error) {
	batchPoints, err := client.NewBatchPoints(w.batchConfig)
	if err != nil {
		return
	}
	if err = addPoints(records, batchPoints); err != nil {
		return
	}
	if err = w.client.Write(batchPoints); err != nil {
		return
	}
	return
}

func addPoints(records []pstore.Record, batchPoints client.BatchPoints) error {
	for i := range records {
		point, err := createPoint(&records[i])
		if err != nil {
			return err
		}
		batchPoints.AddPoint(point)
	}
	return nil
}

func createPoint(r *pstore.Record) (*client.Point, error) {
	tags := map[string]string{
		kTagVersion: kVersionNum,
		kTagHost:    r.HostName,
	}
	for k, v := range r.Tags {
		tags[k] = v
	}
	fields := map[string]interface{}{
		kFieldName:  r.Path,
		kFieldValue: kafka.ToFloat64(r),
	}
	return client.NewPoint(r.Path, tags, fields, r.Timestamp)
}
