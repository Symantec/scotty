package httputil

import (
	"net/url"
)

func newUrl(path string, nameValues []string) *url.URL {
	length := len(nameValues)
	if length%2 != 0 {
		panic("nameValues must have even length.")
	}
	values := make(url.Values)
	for i := 0; i < length; i += 2 {
		values.Add(nameValues[i], nameValues[i+1])
	}
	return &url.URL{
		Path:     path,
		RawQuery: values.Encode()}
}
