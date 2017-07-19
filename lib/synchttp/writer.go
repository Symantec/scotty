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

func (w *syncJSONWriterType) Write(
	url string, headers http.Header, payload interface{}) error {
	buffer, err := encodeJSON(payload)
	if err != nil {
		return err
	}
	payloadStr := buffer.String()
	req, err := http.NewRequest("POST", url, buffer)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	response, err := w.client.Do(req)
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
