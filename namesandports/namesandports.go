package namesandports

func (n NamesAndPorts) _copy() NamesAndPorts {
	if n == nil {
		return nil
	}
	result := make(NamesAndPorts, len(n))
	for k, v := range n {
		result[k] = v
	}
	return result
}

func (n *NamesAndPorts) add(name string, port uint) {
	if *n == nil {
		*n = make(NamesAndPorts)
	}
	(*n)[name] = port
}

func (n NamesAndPorts) hasPort(port uint) bool {
	for _, v := range n {
		if v == port {
			return true
		}
	}
	return false
}
