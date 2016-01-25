package lmm

import (
	"encoding/json"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"strconv"
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
)

func isTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func newWriter(topic string, tenantId, apiKey string, addresses []string) (
	result *Writer, err error) {
	var writer Writer
	writer.topic = topic
	writer.tenantId = tenantId
	writer.apiKey = apiKey
	writer.broker, err = kafka.Dial(addresses, kafka.NewBrokerConf("test"))
	if err != nil {
		return
	}
	var count int32
	count, err = writer.broker.PartitionCount(topic)
	if err != nil {
		return
	}
	conf := kafka.NewProducerConf()
	conf.RequiredAcks = proto.RequiredAcksLocal
	producer := writer.broker.Producer(conf)
	writer.producer = kafka.NewRoundRobinProducer(producer, count)
	result = &writer
	return
}

func (w *Writer) write(records []Record) (err error) {
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

// recordSerializerType serializes a record to bytes for LMM.
// Warning, instances of this type are not thread safe.
type recordSerializerType struct {
	record       map[string]string
	formatString string
}

func newRecordSerializer(tenantId, apiKey string) *recordSerializerType {
	return &recordSerializerType{
		record: map[string]string{
			kVersion:  "1",
			kTenantId: tenantId,
			kApiKey:   apiKey},
		formatString: "2006-01-02T15:04:05.000Z"}
}

func (s *recordSerializerType) Serialize(r *Record) ([]byte, error) {
	if !isTypeSupported(r.Kind) {
		panic("Cannot record given kind.")
	}
	s.record[kTimestamp] = messages.FloatToTime(
		r.Timestamp).Format(s.formatString)
	switch r.Kind {
	case types.Bool:
		if r.Value.(bool) {
			s.record[kValue] = "1"
		} else {
			s.record[kValue] = "0"
		}
	case types.Int:
		s.record[kValue] = strconv.FormatInt(r.Value.(int64), 10)
	case types.Uint:
		s.record[kValue] = strconv.FormatUint(r.Value.(uint64), 10)
	case types.Float:
		s.record[kValue] = strconv.FormatFloat(
			r.Value.(float64), 'f', -1, 64)
	case types.GoTime:
		s.record[kValue] = strconv.FormatFloat(
			messages.TimeToFloat(r.Value.(time.Time)),
			'f', -1, 64)
	case types.GoDuration:
		s.record[kValue] = strconv.FormatFloat(
			messages.DurationToFloat(
				r.Value.(time.Duration))*units.FromSeconds(r.Unit),
			'f', -1, 64)
	default:
		panic("Unsupported type")

	}
	s.record[kName] = r.Path
	s.record[kHost] = r.HostName
	s.record[kAppName] = r.AppName
	return json.Marshal(s.record)
}
