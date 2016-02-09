package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"time"
)

const (
	kVersion   = "@version"
	kTimestamp = "@timestamp"
	kValue     = "value"
	kName      = "name"
	kHost      = "host"
	kTenantId  = "tenant_id"
	kApiKey    = "apikey"
)

const (
	kTimeFormat = "2006-01-02T15:04:05.000Z"
)

const (
	kVersionNum = "1"
)

var (
	supportedTypes = map[types.Type]bool{
		types.Bool:       true,
		types.Int:        true,
		types.Uint:       true,
		types.Float:      true,
		types.GoTime:     true,
		types.GoDuration: true,
	}
)

var (
	kFakeWriter = &fakeWriter{
		recordSerializerType{
			TenantId: "aTenantId",
			ApiKey:   "anApiKey"}}
)

type pathAndMillisType struct {
	Path   string
	Millis int64
}

func newPathAndMillisType(record *pstore.Record) pathAndMillisType {
	return pathAndMillisType{
		Path:   record.Path,
		Millis: int64(messages.TimeToFloat(record.Timestamp) * 1000.0)}
}

type uniqueMetricsWriter struct {
	pstore.Writer
}

// fixDuplicates is a workaround for a Grafana bug.
// In Grafana, two metric values cannot have the same name and timestamp even
// if they are for different endpoints. Otherwise, race conditions cause
// one of the metric values to stomp out the other yielding an incomplete
// data set.
// fixDuplicates returns a slice like records except that if two records
// have the same metric name and timestamp, fixDuplicates adds 1ms to one
// of the timestamps to avoid race conditions in Grafana.
// If records requires no modifications, fixDuplicates returns it unchanged.
// Otherwise, fixDuplicates returns a copy of the records slice with the
// needed modifications leaving the original records slice unchanged.
func fixDuplicates(records []pstore.Record) (result []pstore.Record) {
	result = records
	copied := false
	pathAndTimeExists := make(map[pathAndMillisType]bool)
	for i := range records {
		pathAndMillis := newPathAndMillisType(&result[i])
		for pathAndTimeExists[pathAndMillis] {
			if !copied {
				result = make([]pstore.Record, len(records))
				copy(result, records)
				copied = true
			}
			result[i].Timestamp = result[i].Timestamp.Add(time.Millisecond)
			pathAndMillis = newPathAndMillisType(&result[i])
		}
		pathAndTimeExists[pathAndMillis] = true
	}
	return
}

func (u uniqueMetricsWriter) Write(records []pstore.Record) (err error) {
	return u.Writer.Write(fixDuplicates(records))
}

type fakeWriter struct {
	serializer recordSerializerType
}

func newFakeWriter() pstore.Writer {
	return uniqueMetricsWriter{kFakeWriter}
}

func (f *fakeWriter) IsTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func (f *fakeWriter) Write(records []pstore.Record) (err error) {
	for i := range records {
		var payload []byte
		payload, err = f.serializer.Serialize(&records[i])
		if err != nil {
			return
		}
		fmt.Println(string(payload))
		fmt.Println()
	}
	fmt.Println()
	return
}

type writer struct {
	broker     *kafka.Broker
	producer   kafka.DistributingProducer
	topic      string
	serializer recordSerializerType
}

func newWriter(c *Config) (
	result pstore.Writer, err error) {
	var awriter writer
	awriter.topic = c.Topic
	awriter.serializer.TenantId = c.TenantId
	awriter.serializer.ApiKey = c.ApiKey
	awriter.broker, err = kafka.Dial(c.Endpoints, kafka.NewBrokerConf(c.ClientId))
	if err != nil {
		return
	}
	var count int32
	count, err = awriter.broker.PartitionCount(c.Topic)
	if err != nil {
		return
	}
	conf := kafka.NewProducerConf()
	conf.RequiredAcks = proto.RequiredAcksLocal
	producer := awriter.broker.Producer(conf)
	awriter.producer = kafka.NewRoundRobinProducer(producer, count)
	if c.AllowDuplicates {
		result = &awriter
	} else {
		result = uniqueMetricsWriter{&awriter}
	}
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func (w *writer) Write(records []pstore.Record) (err error) {
	msgs := make([]*proto.Message, len(records))
	for i := range records {
		var payload []byte
		payload, err = w.serializer.Serialize(&records[i])
		if err != nil {
			return
		}
		msgs[i] = &proto.Message{Value: payload}
	}
	_, err = w.producer.Distribute(w.topic, msgs...)
	return
}

// recordSerializerType serializes a record to bytes for kafka.
type recordSerializerType struct {
	TenantId string
	ApiKey   string
}

func (s *recordSerializerType) Serialize(r *pstore.Record) ([]byte, error) {
	if !supportedTypes[r.Kind] {
		panic("Cannot record given kind.")
	}
	record := map[string]interface{}{
		kVersion:   kVersionNum,
		kTenantId:  s.TenantId,
		kApiKey:    s.ApiKey,
		kTimestamp: r.Timestamp.Format(kTimeFormat),
		kName:      r.Path,
		kHost:      r.HostName}
	switch r.Kind {
	case types.Bool:
		if r.Value.(bool) {
			record[kValue] = 1.0
		} else {
			record[kValue] = 0.0
		}
	case types.Int:
		record[kValue] = float64(r.Value.(int64))
	case types.Uint:
		record[kValue] = float64(r.Value.(uint64))
	case types.Float:
		record[kValue] = r.Value
	case types.GoTime:
		record[kValue] = messages.TimeToFloat(r.Value.(time.Time))
	case types.GoDuration:
		record[kValue] = messages.DurationToFloat(
			r.Value.(time.Duration)) * units.FromSeconds(
			r.Unit)
	default:
		panic("Unsupported type")

	}
	for k, v := range r.Tags {
		record[k] = v
	}
	return json.Marshal(record)
}
