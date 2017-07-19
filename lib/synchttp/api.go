package synchttp

import (
	"net/http"
)

// JSONWriter posts json to a rest API.
type JSONWriter interface {
	// url is the endpoint of the rest API. headers are any additional
	// content headers to be written besides "Content-Type: application/json";
	// payload is marshalled into JSON.
	Write(url string, headers http.Header, payload interface{}) error
}

// NewSyncJSONWriter creates a synchronous json writer.
func NewSyncJSONWriter() (JSONWriter, error) {
	wc, err := newSyncJSONWriter()
	return wc, err
}
