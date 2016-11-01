package kafka

// TODO: If optiopay/kafka accepts pull request, remove this file.

import (
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"sync"
	"time"
)

type backOffType struct {
	initialWaitTime time.Duration
	exp             float64
	nextWaitTime    time.Duration
}

func newBackOffType(initial time.Duration, exp float64) *backOffType {
	return &backOffType{
		initialWaitTime: initial,
		exp:             exp,
		nextWaitTime:    initial,
	}
}

func (b *backOffType) WaitTime(err error) (waitTime time.Duration) {
	if err == nil {
		b.nextWaitTime = b.initialWaitTime
		return
	}
	waitTime = b.nextWaitTime
	b.nextWaitTime = time.Duration(float64(b.nextWaitTime) * b.exp)
	return
}

type channelTopicProducerResponseType struct {
	Offset int64
	Error  error
}

type channelTopicProducerRequestType struct {
	Messages []*proto.Message
	Response chan channelTopicProducerResponseType
}

type channelTopicProducerType struct {
	topic    string
	producer kafka.Producer
	ch       chan channelTopicProducerRequestType
}

func newChannelTopicProducer(
	topic string,
	p kafka.Producer,
	numPartitions int32) *channelTopicProducerType {
	result := &channelTopicProducerType{
		topic:    topic,
		producer: p,
		ch: make(
			chan channelTopicProducerRequestType, numPartitions),
	}
	partitionCount := int(numPartitions)
	for i := 0; i < partitionCount; i++ {
		go result.consumePartition(i)
	}
	return result
}

func (p *channelTopicProducerType) consumePartition(partition int) {
	backoff := newBackOffType(10*time.Second, 1.5)
	for {
		request := <-p.ch
		var response channelTopicProducerResponseType
		response.Offset, response.Error = p.producer.Produce(
			p.topic, int32(partition), request.Messages...)
		request.Response <- response
		close(request.Response)
		waitTime := backoff.WaitTime(response.Error)
		if waitTime > 0 {
			time.Sleep(waitTime)
		}
	}
}

func (p *channelTopicProducerType) Distribute(messages ...*proto.Message) (
	offset int64, err error) {
	responseCh := make(chan channelTopicProducerResponseType)
	p.ch <- channelTopicProducerRequestType{
		Messages: messages,
		Response: responseCh,
	}
	response := <-responseCh
	return response.Offset, response.Error
}

type channelProducer struct {
	producer   kafka.Producer
	partitions int32
	mu         sync.Mutex
	topicMap   map[string]*channelTopicProducerType
}

// newChannelProducer wraps given producer and returns a
// DistributingProducer that works like a random producer plus it
// guarantees that if only X% of KAFKA endpoints are up, writing throughput
// will eventually approach X%.
// The returned DistributingProducer accomplishes this by favoring destinations
// that accept writes over ones that continually produce errors.
func newChannelProducer(
	p kafka.Producer, numPartitions int32) kafka.DistributingProducer {
	return &channelProducer{
		producer:   p,
		partitions: numPartitions,
		topicMap:   make(map[string]*channelTopicProducerType),
	}
}

func (p *channelProducer) fetchTopicProducer(topic string) (
	result *channelTopicProducerType) {
	p.mu.Lock()
	defer p.mu.Unlock()
	result = p.topicMap[topic]
	if result == nil {
		result = newChannelTopicProducer(
			topic, p.producer, p.partitions)
		p.topicMap[topic] = result
	}
	return
}

// Distribute writes messages to given kafka topic, choosing
// a destination at random but favoring destinations that accept writes
// rather than producing errors.
// All messages written within single Produce call are atomically
// written to the same destination.
func (p *channelProducer) Distribute(
	topic string, messages ...*proto.Message) (offset int64, err error) {
	return p.fetchTopicProducer(topic).Distribute(messages...)
}
