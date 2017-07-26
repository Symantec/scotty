// Package selfsource get metrics for current process
package selfsource

import (
	"github.com/Symantec/scotty/sources"
)

func GetConnector() sources.Connector {
	return kConnector
}
