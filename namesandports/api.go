// Package namesandports contains the NamesAndPorts datastructure.
package namesandports

type Record struct {
	Port  uint
	IsTLS bool
}

// NamesAndPorts represents a set of application names and corresponding
// ports.  nil means the empty set. The key is the application name; the
// value is the port and whether or not it is TLS.
type NamesAndPorts map[string]Record

// Copy returns a copy of this instance
func (n NamesAndPorts) Copy() NamesAndPorts {
	return n._copy()
}

// Add adds a name and port to this instance in place.
func (n *NamesAndPorts) Add(name string, port uint, isTLS bool) {
	n.add(name, port, isTLS)
}

// HasPort returns true if port is included in set
func (n NamesAndPorts) HasPort(port uint) bool {
	return n.hasPort(port)
}
