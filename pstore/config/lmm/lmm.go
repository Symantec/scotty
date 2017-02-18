package lmm

import (
	"errors"
	"github.com/Symantec/scotty/lib/queuesender"
	"github.com/Symantec/scotty/lib/synchttp"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/tricorder/go/tricorder/types"
)

type writer struct {
	endpoint string
	tenantId string
	apiKey   string
	sync     synchttp.JSONWriter
	async    *queuesender.Sender
}

func newWriter(c Config) (
	result pstore.LimitedRecordWriter, err error) {
	if c.Endpoint == "" || c.TenantId == "" || c.ApiKey == "" {
		err = errors.New(
			"endpoint, tenantId, and apiKey keys required")
		return
	}
	var awriter writer
	awriter.endpoint = c.Endpoint
	awriter.tenantId = c.TenantId
	awriter.apiKey = c.ApiKey
	if c.Name != "" {
		awriter.async, err = queuesender.New(
			awriter.endpoint, 2000, "", nil)
		if err != nil {
			return
		}
		awriter.async.Register(c.Name)
	} else {
		awriter.sync, err = synchttp.NewSyncJSONWriter()
		if err != nil {
			return
		}
	}
	result = &awriter
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return kafka.IsTypeSupported(t)
}

func (w *writer) Write(records []pstore.Record) (err error) {
	if len(records) == 0 {
		return nil
	}
	if w.sync != nil {
		for i := range records {
			if err = w.sync.Write(
				w.endpoint,
				kafka.LMMJSONPayload(
					&records[i], w.tenantId, w.apiKey, false)); err != nil {
				return
			}
		}
		return
	} else {
		for i := range records {
			w.async.Send(
				w.endpoint,
				kafka.LMMJSONPayload(&records[i], w.tenantId, w.apiKey, false))
		}
	}
	return
}
