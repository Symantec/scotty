package synchttp

// JSONWriter posts json to a rest API.
type JSONWriter interface {
	// url is the endpoint of the rest API. payload is marshalled into JSON.
	Write(url string, payload interface{}) error
}

// NewSyncJSONWriter creates a synchronous json writer.
func NewSyncJSONWriter() (JSONWriter, error) {
	wc, err := newSyncJSONWriter()
	return wc, err
}
