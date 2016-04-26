// Package trisource connects to sources using tricorder.
package trisource

import (
	"github.com/Symantec/scotty/sources"
)

func GetConnector() sources.Connector {
	return kConnector
}
