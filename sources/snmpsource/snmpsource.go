package snmpsource

import (
	"errors"
	"github.com/Symantec/scotty/sources"
)

type connectorType string

func newConnector(community string) sources.Connector {
	return connectorType(community)
}

func (c connectorType) Connect(host string, port uint, config sources.Config) (
	sources.Poller, error) {
	return nil, errors.New("SNMP not supported.")
}

func (c connectorType) Name() string {
	return "snmp"
}
