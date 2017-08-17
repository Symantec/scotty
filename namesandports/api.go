// Package namesandports contains the NamesAndPorts datastructure.
package namesandports

// NamesAndPorts represents a set of application names and corresponding
// ports.  nil means the empty set. The key is the application name; the
// value is the port.
type NamesAndPorts map[string]uint

// Copy returns a copy of this instance
func (n NamesAndPorts) Copy() NamesAndPorts {
	return n._copy()
}

// Add adds a name and port to this instance in place.
func (n *NamesAndPorts) Add(name string, port uint) {
	n.add(name, port)
}

// HasPort returns true if port is included in set
func (n NamesAndPorts) HasPort(port uint) bool {
	return n.hasPort(port)
}
