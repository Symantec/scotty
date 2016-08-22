package tsdb

import (
	"bytes"
	"fmt"
)

func (t TimeSeries) marshalJSON() ([]byte, error) {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "{")
	for i := range t {
		if i > 0 {
			fmt.Fprintf(b, ",")
		}
		fmt.Fprintf(b, "\"%d\":%g", int64(t[i].Ts), t[i].Value)
	}
	fmt.Fprintf(b, "}")
	return b.Bytes(), nil
}
