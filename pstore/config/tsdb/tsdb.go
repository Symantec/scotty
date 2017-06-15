package tsdb

import (
	"errors"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/bluebreezecf/opentsdb-goclient/client"
	"github.com/bluebreezecf/opentsdb-goclient/config"
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

var (
	supportedTypes = map[types.Type]bool{
		types.Bool:       true,
		types.Int8:       true,
		types.Int16:      true,
		types.Int32:      true,
		types.Int64:      true,
		types.Uint8:      true,
		types.Uint16:     true,
		types.Uint32:     true,
		types.Uint64:     true,
		types.Float32:    true,
		types.Float64:    true,
		types.GoTime:     true,
		types.GoDuration: true,
		types.String:     true,
	}
)

type writer struct {
	client client.Client
}

func newWriter(c Config) (
	result pstore.LimitedRecordWriter, err error) {
	if c.HostAndPort == "" {
		err = errors.New(
			"HostAndPort fields required.")
		return
	}
	var cl client.Client
	cl, err = client.NewClient(
		config.OpenTSDBConfig{OpentsdbHost: c.HostAndPort})
	if err != nil {
		return
	}
	result = &writer{client: cl}
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func asInterface(r *pstore.Record) interface{} {
	switch r.Kind {
	case types.Bool:
		if r.Value.(bool) {
			return 1.0
		}
		return 0.0
	case types.Int8:
		return int64(r.Value.(int8))
	case types.Int16:
		return int64(r.Value.(int16))
	case types.Int32:
		return int64(r.Value.(int32))
	case types.Int64:
		return r.Value.(int64)
	case types.Uint8:
		return int64(r.Value.(uint8))
	case types.Uint16:
		return int64(r.Value.(uint16))
	case types.Uint32:
		return int64(r.Value.(uint32))
	case types.Uint64:
		return int64(r.Value.(uint64))
	case types.Float32:
		return float64(r.Value.(float32))
	case types.Float64:
		return r.Value.(float64)
	case types.GoTime:
		return duration.TimeToFloat(r.Value.(time.Time))
	case types.GoDuration:
		return duration.ToFloat(
			r.Value.(time.Duration)) * units.FromSeconds(
			r.Unit)
	case types.String:
		return r.Value.(string)
	default:
		panic("Unsupported type")

	}

}

func (w *writer) Write(records []pstore.Record) (err error) {
	datas := make([]client.DataPoint, len(records))
	for i := range records {
		datas[i].Metric = records[i].Path
		datas[i].Timestamp = records[i].Timestamp.Unix()
		datas[i].Value = asInterface(&records[i])
		datas[i].Tags = allTagValues(&records[i])
	}
	// We have to pass details as the second arg or else the call will return
	// an error. Internally, this call parses the JSON in the response, but
	// it expects the response to be "details" style. Arguably this is a bug
	// in the bluebreezecf/opentsdb-goclient library because according to the
	// open tsdb spec, it should be acceptable to pass the empty string as
	// the second param and receive no details.
	//
	// Also, the go runtime uses Transfer-Encoding = chunked when it deems
	// appropriate. So to use this go library, the open tsdb server must be
	// set up to accept Transfer-Encoding = chunked. We do this by changing
	// the opentsdb.conf file. Chunk size in go appears to be 8192 but we set
	// to 65536 for good measure.
	//
	// Add these lines to opentsdb.conf:
	//
	// tsd.http.request.enable_chunked = true
	// tsd.http.request.max_chunk = 65536
	_, err = w.client.Put(datas, "details")
	return
}

func allTagValues(r *pstore.Record) map[string]string {
	result := map[string]string{kHostName: r.HostName}
	for k, v := range r.Tags {
		result[k] = v
	}
	return result
}
