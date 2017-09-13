package lmm

import (
	"errors"
	"github.com/Symantec/scotty/lib/queuesender"
	"github.com/Symantec/scotty/lib/synchttp"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"net/http"
)

const (
	kRegion = "region"
)

type writer struct {
	endpoint string
	tenantId string
	apiKey   string
	headers  http.Header
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
	awriter.headers = make(http.Header)
	awriter.headers.Set("tenantid", awriter.tenantId)
	awriter.headers.Set("apikey", awriter.apiKey)
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
	return kafka.IsTypeSupported(t) || t == types.String
}

func (w *writer) Write(records []pstore.Record) (err error) {
	if len(records) == 0 {
		return nil
	}
	if w.sync != nil {
		var jsonStuff []interface{}
		for i := range records {
			payload := kafka.LMMJSONPayload(
				&records[i], w.tenantId, w.apiKey, false)
			jsonStuff = append(jsonStuff, payload)
		}
		if err = w.sync.Write(w.endpoint, w.headers, jsonStuff); err != nil {
			return
		}
		return
	} else {
		// TODO: If we go back to async, be sure we can send custom
		// headers.
		for i := range records {
			payload := kafka.LMMJSONPayload(
				&records[i], w.tenantId, w.apiKey, false)
			w.async.Send(w.endpoint, payload)
		}
	}
	return
}
