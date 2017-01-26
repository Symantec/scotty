package cis

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	kCisPath = "aws/packages"
)

var (
	replacer = strings.NewReplacer(".", "_")
)

func (c *Client) newAsyncWriter(handler func(r *Response)) (
	*Writer, error) {
	conn, err := tls.Dial("tcp", c.endpoint, nil)
	if err != nil {
		return nil, err
	}
	result := &Writer{endpoint: c.endpoint, conn: conn, handler: handler}
	result.queueReady.L = &result.lock
	result.queue.Init()
	go result.loop()
	return result, nil
}

func (w *Writer) dequeue() *Request {
	w.lock.Lock()
	defer w.lock.Unlock()
	for w.queue.Len() == 0 {
		w.queueReady.Wait()
	}
	return w.queue.Remove(w.queue.Front()).(*Request)
}

func (w *Writer) enqueue(r *Request) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.queue.PushBack(r)
	w.queueReady.Broadcast()
}

func (w *Writer) loop() {
	br := bufio.NewReader(w.conn)
	for {
		var resp Response
		resp.Request = *w.dequeue()
		httpresp, err := http.ReadResponse(br, nil)
		if err != nil {
			resp.Err = err
		} else {
			defer httpresp.Body.Close()
			resp.Status = httpresp.Status
			resp.Code = httpresp.StatusCode
			io.Copy(ioutil.Discard, httpresp.Body)
		}
		w.handler(&resp)
	}
}

// Write writes data to CIS asynchronously
func (w *Writer) write(stats *Stats) error {

	type versionSizeType struct {
		Version string `json:"version"`
		Size    uint64 `json:"size"`
	}

	type packagesType struct {
		PkgMgmtType string                     `json:"pkgMgmtType"`
		Packages    map[string]versionSizeType `json:"packages"`
	}

	jsonToEncode := &packagesType{
		PkgMgmtType: stats.Packages.ManagementType,
		Packages: make(
			map[string]versionSizeType,
			len(stats.Packages.Packages)),
	}

	for _, entry := range stats.Packages.Packages {
		jsonToEncode.Packages[replacer.Replace(entry.Name)] = versionSizeType{
			Version: entry.Version, Size: entry.Size}
	}

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	if err := encoder.Encode(jsonToEncode); err != nil {
		return err
	}

	// TODO: Maybe get rid of all the diagnostics in the error messages
	url := fmt.Sprintf("https://%s/%s/%s", w.endpoint, kCisPath, stats.InstanceId)
	req := &Request{Url: url, Payload: buffer.String()}
	httpreq, err := http.NewRequest("POST", url, buffer)
	if err != nil {
		return err
	}
	httpreq.Header.Set("Content-Type", "application/json")
	if err := w.writeAndEnqueue(httpreq, req); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeAndEnqueue(
	httpreq *http.Request, req *Request) error {
	w.writeAndEnqueueSemaphore.Lock()
	defer w.writeAndEnqueueSemaphore.Unlock()
	if err := httpreq.Write(w.conn); err != nil {
		return err
	}
	w.enqueue(req)
	return nil
}
