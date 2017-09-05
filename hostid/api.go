// Package hostid contains types to identify hosts
package hostid

import (
	"strings"
)

// HostID identifies a host. HostID instances should not be changed in place
// once created.
type HostID struct {
	HostName  string
	IPAddress string
}

func (h *HostID) ConnectID() string {
	if h.IPAddress != "" {
		return h.IPAddress
	}
	return ignorePastStar(h.HostName)
}

func ignorePastStar(hostName string) string {
	idx := strings.Index(hostName, "*")
	if idx == -1 {
		return hostName
	}
	return hostName[:idx]
}
