package jsonsource

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"net/http"
	"reflect"
	"time"
)

type connectorType int

var (
	kConnector = connectorType(0)
)

func (c connectorType) Connect(host string, port uint) (sources.Poller, error) {
	url := fmt.Sprintf("http://%s:%d/metricsapi", host, port)
	var client http.Client
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	return &pollerType{response: response}, nil
}

func (c connectorType) Name() string {
	return "json"
}

type pollerType struct {
	response *http.Response
}

type genericMetric struct {
	Value json.RawMessage `json:"value"`
	messages.Metric
}

func (g *genericMetric) decodeValueAs(prototype interface{}) error {
	valuePtr := reflect.New(reflect.TypeOf(prototype))
	if err := json.Unmarshal(g.Value, valuePtr.Interface()); err != nil {
		return err
	}
	g.Metric.Value = valuePtr.Elem().Interface()
	return nil
}

func (g *genericMetric) decodeValue() error {
	switch g.Kind {
	case types.List:
		nilSlice, err := g.SubType.SafeNilSlice()
		if err != nil {
			return err
		}
		return g.decodeValueAs(nilSlice)
	default:
		zeroVal, err := messages.ZeroValue(g.Kind)
		if err != nil {
			return err
		}
		return g.decodeValueAs(zeroVal)
	}
}

func (g *genericMetric) ConvertToGoRPC() (err error) {
	if err = g.decodeValue(); err != nil {
		return err
	}
	if err = g.Metric.ConvertToGoRPC(); err != nil {
		g.Metric.Value = nil
	} else {
		g.Value = nil
	}
	return
}

func (p *pollerType) Poll() (result metrics.List, err error) {
	if p.response.StatusCode != 200 {
		return nil, errors.New(p.response.Status)
	}
	decoder := json.NewDecoder(p.response.Body)
	var values genericMetricList
	if err = decoder.Decode(&values); err != nil {
		return
	}
	for _, v := range values {
		if err = v.ConvertToGoRPC(); err != nil {
			return
		}
	}
	result = values
	return
}

func (p *pollerType) Close() error {
	return p.response.Body.Close()
}

type genericMetricList []*genericMetric

func (l genericMetricList) Len() int {
	return len(l)
}

func (l genericMetricList) Index(i int, result *metrics.Value) {
	result.Path = l[i].Path
	result.Description = l[i].Description
	result.Unit = l[i].Unit
	result.Value = l[i].Metric.Value
	if l[i].TimeStamp == nil {
		result.TimeStamp = time.Time{}
	} else {
		result.TimeStamp = l[i].TimeStamp.(time.Time)
	}
	result.GroupId = l[i].GroupId
}
