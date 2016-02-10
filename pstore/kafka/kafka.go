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
	kAppName   = "appname"
	kTenantId  = "tenant_id"
	kApiKey    = "apikey"
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
	kFakeWriter = &fakeWriter{tenantId: "aTenantId", apiKey: "anApiKey"}
)

// TODO: Remove once we know the grafana bug involving duplicate timestamps
// is fixed.
type pathAndMillisType struct {
	Path   string
	Millis int64
}

// TODO: Remove once we know the grafana bug involving duplicate timestamps
// is fixed.
func newPathAndMillisType(record *pstore.Record) pathAndMillisType {
	return pathAndMillisType{
		Path:   record.Path,
		Millis: int64(record.Timestamp * 1000.0)}
}

// TODO: Remove once we know the grafana bug involving duplicate timestamps
// is fixed.
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
	// TODO: Remove once we know the grafana bug involving duplicate timestamps
	// is fixed.
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
			result[i].Timestamp += 0.001
			pathAndMillis = newPathAndMillisType(&result[i])
		}
		pathAndTimeExists[pathAndMillis] = true
	}
	return
}

// TODO: Remove once we know the grafana bug involving duplicate timestamps
// is fixed.
func (u uniqueMetricsWriter) Write(records []pstore.Record) (err error) {
	return u.Writer.Write(fixDuplicates(records))
}

type fakeWriter struct {
	tenantId string
	apiKey   string
}

func newFakeWriter() pstore.Writer {
	return kFakeWriter
}

func (f *fakeWriter) IsTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func (f *fakeWriter) Write(records []pstore.Record) (err error) {
	serializer := newRecordSerializer(f.tenantId, f.apiKey)
	for i := range records {
		var payload []byte
		payload, err = serializer.Serialize(&records[i])
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
	broker   *kafka.Broker
	producer kafka.DistributingProducer
	tenantId string
	apiKey   string
	topic    string
}

func newWriter(c *Config) (
	result pstore.Writer, err error) {
	var awriter writer
	awriter.topic = c.Topic
	awriter.tenantId = c.TenantId
	awriter.apiKey = c.ApiKey
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
	result = &awriter
	return
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func (w *writer) Write(records []pstore.Record) (err error) {
	serializer := newRecordSerializer(w.tenantId, w.apiKey)
	msgs := make([]*proto.Message, len(records))
	for i := range records {
		var payload []byte
		payload, err = serializer.Serialize(&records[i])
		if err != nil {
			return
		}
		msgs[i] = &proto.Message{Value: payload}
	}
	_, err = w.producer.Distribute(w.topic, msgs...)
	return
}

// recordSerializerType serializes a record to bytes for kafka.
// Warning, instances of this type are not thread safe.
type recordSerializerType struct {
	record       map[string]interface{}
	formatString string
}

func newRecordSerializer(tenantId, apiKey string) *recordSerializerType {
	return &recordSerializerType{
		record: map[string]interface{}{
			kVersion:  "1",
			kTenantId: tenantId,
			kApiKey:   apiKey},
		formatString: "2006-01-02T15:04:05.000Z"}
}

func (s *recordSerializerType) Serialize(r *pstore.Record) ([]byte, error) {
	if !supportedTypes[r.Kind] {
		panic("Cannot record given kind.")
	}
	s.record[kTimestamp] = messages.FloatToTime(
		r.Timestamp).Format(s.formatString)
	switch r.Kind {
	case types.Bool:
		if r.Value.(bool) {
			s.record[kValue] = 1.0
		} else {
			s.record[kValue] = 0.0
		}
	case types.Int:
		s.record[kValue] = float64(r.Value.(int64))
	case types.Uint:
		s.record[kValue] = float64(r.Value.(uint64))
	case types.Float:
		s.record[kValue] = r.Value
	case types.GoTime:
		s.record[kValue] = messages.TimeToFloat(r.Value.(time.Time))
	case types.GoDuration:
		s.record[kValue] = messages.DurationToFloat(
			r.Value.(time.Duration)) * units.FromSeconds(
			r.Unit)
	default:
		panic("Unsupported type")

	}
	s.record[kName] = r.Path
	s.record[kHost] = r.HostName
	s.record[kAppName] = r.AppName
	return json.Marshal(s.record)
}
