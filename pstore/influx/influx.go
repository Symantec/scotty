package influx

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/lmm"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/influxdata/influxdb/client/v2"
)

const (
	kTagVersion = "@version"
	kTagHost    = "host"
	kFieldName  = "name"
	kFieldValue = "value"
)

type writer struct {
	clients     []client.Client
	batchConfig client.BatchPointsConfig
}

func newWriter(c *Config) (
	result pstore.LimitedRecordWriter, err error) {
	if err = c.checkRequiredFields(); err != nil {
		panic(err)
	}
	clients := make([]client.Client, len(c.Endpoints))
	var config client.HTTPConfig
	for i := range c.Endpoints {
		config.Addr = c.Endpoints[i].HostAndPort
		config.Username = c.Endpoints[i].UserName
		config.Password = c.Endpoints[i].Password
		clients[i], err = client.NewHTTPClient(config)
		if err != nil {
			return
		}
	}
	w := &writer{
		clients: clients,
		batchConfig: client.BatchPointsConfig{
			Database: c.Database,
		},
	}
	result = w
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return lmm.IsTypeSupported(t)
}

func (w *writer) Write(records []pstore.Record) (err error) {
	batchPoints, err := client.NewBatchPoints(w.batchConfig)
	if err != nil {
		return
	}
	if err = addPoints(records, batchPoints); err != nil {
		return
	}
	for i := range w.clients {
		if err = w.clients[i].Write(batchPoints); err != nil {
			return
		}
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
		kTagVersion: lmm.Version,
		kTagHost:    r.HostName,
	}
	for k, v := range r.Tags {
		tags[k] = v
	}
	fields := map[string]interface{}{
		kFieldName:  r.Path,
		kFieldValue: lmm.ToFloat64(r),
	}
	return client.NewPoint(r.Path, tags, fields, r.Timestamp)
}
