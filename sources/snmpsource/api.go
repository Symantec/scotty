// Package snmpsource connects to sources using snmp
package snmpsource

import (
	"github.com/Symantec/scotty/sources"
)

// NewConnector returns a new connector for the given community.
func NewConnector(community string) sources.Connector {
	return newConnector(community)
}
