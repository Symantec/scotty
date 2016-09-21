// Package jsonsource connects to sources using tricorder json.
package jsonsource

import (
	"github.com/Symantec/scotty/sources"
)

func GetConnector() sources.Connector {
	return kConnector
}
