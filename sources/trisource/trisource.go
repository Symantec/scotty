package trisource

import (
	"fmt"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"net/rpc"
	"time"
)

type connectorType int

var (
	kConnector = connectorType(0)
)

func (c connectorType) Connect(host string, port int) (sources.Poller, error) {
	conn, err := rpc.DialHTTP(
		"tcp",
		fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}
	return pollerType{conn}, nil
}

func (c connectorType) Name() string {
	return "tricorder"
}

type pollerType struct {
	*rpc.Client
}

func (p pollerType) Poll() (result metrics.List, err error) {
	var values messages.MetricList
	err = p.Call("MetricsServer.ListMetrics", "", &values)
	if err != nil {
		return
	}
	for i := range values {
		values[i].Kind = fixupKind(values[i].Kind)
	}
	return listType(values), nil
}

type listType messages.MetricList

func (l listType) Len() int {
	return len(l)
}

func (l listType) Index(i int, result *metrics.Value) {
	result.Path = l[i].Path
	result.Description = l[i].Description
	result.Unit = l[i].Unit
	result.Kind = l[i].Kind
	result.Bits = l[i].Bits
	result.Value = l[i].Value
	if l[i].TimeStamp == nil {
		result.TimeStamp = time.Time{}
	} else {
		result.TimeStamp = l[i].TimeStamp.(time.Time)
	}
	result.GroupId = l[i].GroupId
}

// TODO: Delete once all apps link with new tricorder
func fixupKind(k types.Type) types.Type {
	switch k {
	case "int":
		return types.Int64
	case "uint":
		return types.Uint64
	case "float":
		return types.Float64
	default:
		return k
	}
}
