package tsdb

import (
	//"bytes"
	//"encoding/json"
	"errors"
	//"fmt"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/pstore/config/kafka"
	"github.com/Symantec/tricorder/go/tricorder/types"
	//	"github.com/prometheus/prometheus/documentation/examples/remote_storage/remote_storage_bridge/opentsdb"
	// "github.com/prometheus/prometheus/util/httputil"
	//"io/ioutil"
	//"math"
	"net/http"
	//"net/url"
	"time"
)

const (
	// TODO: Verify this is correct with LMM.
	kHostName = "HostName"
)

const (
	putEndpoint     = "/api/put"
	contentTypeJSON = "application/json"
)

type writer struct {
	url        string
	httpClient *http.Client
}

func newWriter(c Config) (
	result pstore.LimitedRecordWriter, err error) {
	if c.HostAndPort == "" {
		err = errors.New(
			"HostAndPort fields required.")
		return
	}
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	w := &writer{
		url: c.HostAndPort,
		// httpClient: httputil.NewDeadlineClient(c.Timeout, nil),
	}
	result = w
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return kafka.IsTypeSupported(t)
}

func (w *writer) Write(records []pstore.Record) (err error) {
	/*
		reqs := make([]opentsdb.StoreSamplesRequest, len(records))
		for i := range records {
			v := kafka.ToFloat64(&records[i])
			// We can't allow Infinity or NaN, so we do the best we can.
			if math.IsInf(v, 1) {
				v = math.MaxFloat64
			} else if math.IsInf(v, -1) {
				v = -math.MaxFloat64
			} else if math.IsNaN(v) {
				v = 0
			}
			reqs[i] = opentsdb.StoreSamplesRequest{
				Metric: opentsdb.TagValue(records[i].Path),
				// TODO: Milliseconds?
				Timestamp: records[i].Timestamp.Unix(),
				Value:     v,
				Tags:      allTagValues(&records[i]),
			}
		}

		u, err := url.Parse(w.url)
		if err != nil {
			return err
		}

		u.Path = putEndpoint

		buf, err := json.Marshal(reqs)
		if err != nil {
			return err
		}

		resp, err := w.httpClient.Post(
			u.String(),
			contentTypeJSON,
			bytes.NewBuffer(buf),
		)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// API returns status code 204 for successful writes.
		// http://opentsdb.net/docs/build/html/api_http/put.html
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}

		// API returns status code 400 on error, encoding error details in the
		// response content in JSON.
		buf, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var r map[string]int
		if err := json.Unmarshal(buf, &r); err != nil {
			return err
		}
		return fmt.Errorf("failed to write %d samples to OpenTSDB, %d succeeded", r["failed"], r["success"])
	*/
	// Just do nothing for now
	return nil

}

/*
func allTagValues(r *pstore.Record) map[string]opentsdb.TagValue {
	result := map[string]opentsdb.TagValue{
		kHostName: opentsdb.TagValue(r.HostName),
	}
	for k, v := range r.Tags {
		result[k] = opentsdb.TagValue(v)
	}
	return result
}
*/
