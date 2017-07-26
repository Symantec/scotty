package selfsource

import (
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"time"
)

type connectorType int

var (
	kConnector = connectorType(0)
)

var (
	kPoller = pollerType(0)
)

func (c connectorType) Connect(host string, port uint) (sources.Poller, error) {
	return kPoller, nil
}

func (c connectorType) Name() string {
	return "self"
}

type pollerType int

func (p pollerType) Poll() (result metrics.List, err error) {
	values := tricorder.ReadMyMetrics("")
	return listType(values), nil
}

func (p pollerType) Close() error {
	return nil
}

type listType messages.MetricList

func (l listType) Len() int {
	return len(l)
}

func (l listType) Index(i int, result *metrics.Value) {
	result.Path = l[i].Path
	result.Description = l[i].Description
	result.Unit = l[i].Unit
	result.Value = l[i].Value
	if l[i].TimeStamp == nil {
		result.TimeStamp = time.Time{}
	} else {
		result.TimeStamp = l[i].TimeStamp.(time.Time)
	}
	result.GroupId = l[i].GroupId
}
