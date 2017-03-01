package synchttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

type syncJSONWriterType struct {
	client http.Client
}

var (
	kSyncJSONWriter = &syncJSONWriterType{}
)

func newSyncJSONWriter() (JSONWriter, error) {
	return kSyncJSONWriter, nil
}

func (w *syncJSONWriterType) Write(url string, payload interface{}) error {
	buffer, err := encodeJSON(payload)
	if err != nil {
		return err
	}
	payloadStr := buffer.String()
	response, err := w.client.Post(url, "application/json", buffer)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return errors.New(response.Status + ": " + payloadStr)
	}
	return nil
}

func encodeJSON(payload interface{}) (*bytes.Buffer, error) {
	result := &bytes.Buffer{}
	encoder := json.NewEncoder(result)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return result, nil
}
