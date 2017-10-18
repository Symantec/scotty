package trisource

import (
	"crypto/tls"
	"fmt"
	libtls "github.com/Symantec/Dominator/lib/net/tls"
	"github.com/Symantec/Dominator/lib/rpcclientpool"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"time"
)

type connectorType int

var (
	kConnector = connectorType(0)
)

func (c connectorType) NewResource(
	host string, port uint, config sources.Config) sources.Resource {
	if config.IsTls {
		return rpcclientpool.NewWithDialer(
			"tcp",
			fmt.Sprintf("%s:%d", host, port),
			true,
			"",
			libtls.NewDialer(nil, &tls.Config{InsecureSkipVerify: true}))
	}
	return rpcclientpool.New(
		"tcp",
		fmt.Sprintf("%s:%d", host, port),
		true,
		"")
}

func (c connectorType) Connect(host string, port uint, config sources.Config) (
	sources.Poller, error) {
	return c.ResourceConnect(c.NewResource(host, port, config))
}

func (c connectorType) ResourceConnect(resource sources.Resource) (
	sources.Poller, error) {
	clientResource := resource.(*rpcclientpool.ClientResource)
	conn, err := clientResource.Get(nil)
	if err != nil {
		return nil, err
	}
	return pollerType{conn}, nil
}

func (c connectorType) Name() string {
	return "tricorder"
}

type pollerType struct {
	client *rpcclientpool.Client
}

func (p pollerType) Poll() (result metrics.List, err error) {
	var values messages.MetricList
	err = p.client.Call("MetricsServer.ListMetrics", "", &values)
	if err != nil {
		p.client.Close()
		return
	}
	for i := range values {
		values[i].Kind = fixupKind(values[i].Kind)
	}
	return listType(values), nil
}

func (p pollerType) Close() error {
	p.client.Put()
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
