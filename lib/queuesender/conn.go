package queuesender

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	kNetworkTimeout = 15 * time.Second
)

var (
	kErrNoConnection = errors.New("queuesender: No connection")
	kErrInterrupted  = errors.New("queuesender: interrupted")
)

// connType represents a single connection.
type connType struct {
	refresh func() (net.Conn, error)
	lock    sync.Mutex
	conn    net.Conn
	br      *bufio.Reader
}

func extractEndpoint(urlStr string) (scheme, endpoint string, err error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", err
	}
	host := u.Host
	if u.Scheme == "https" && !strings.Contains(u.Host, ":") {
		host = host + ":443"
	}
	if u.Scheme == "http" && !strings.Contains(u.Host, ":") {
		host = host + ":80"
	}
	return u.Scheme, host, nil
}

func newConn(urlStr string) (*connType, error) {
	scheme, endpoint, err := extractEndpoint(urlStr)
	if err != nil {
		return nil, err
	}
	var refresh func() (net.Conn, error)
	if scheme == "https" {
		refresh = func() (net.Conn, error) {
			return tls.Dial("tcp", endpoint, nil)
		}
	}
	if refresh == nil {
		return nil, errors.New("Unsupported scheme")
	}
	conn, err := refresh()
	if err != nil {
		return nil, err
	}
	return _newConn(conn, refresh), nil
}

func _newConn(conn net.Conn, refresh func() (net.Conn, error)) *connType {
	if conn == nil {
		return &connType{refresh: refresh}
	}
	return &connType{
		conn: conn, br: bufio.NewReader(conn), refresh: refresh}

}

func (c *connType) Close() {
	c.conn.Close()
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.conn != nil {
		c.conn = nil
		c.br = nil
	}
}

func (c *connType) _get() (net.Conn, *bufio.Reader) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.conn, c.br
}

func (c *connType) Refresh(logger *loggerType) *connType {
	newConn, err := c.refresh()
	if err != nil {
		logger.Log(err)
	}
	return _newConn(newConn, c.refresh)
}

func (c *connType) Send(req requestType, inter *interruptType) error {
	conn, _ := c._get()
	if conn == nil {
		return kErrNoConnection
	}
	buffer, err := encodeJSON(req.Json)
	if err != nil {
		return err
	}
	httpreq, err := http.NewRequest("POST", req.Endpoint, buffer)
	if err != nil {
		return err
	}
	httpreq.Header.Set("Content-Type", "application/json")
	conn.SetWriteDeadline(time.Now().Add(kNetworkTimeout))
	lastId := inter.Id()
	for {
		err := httpreq.Write(conn)
		// If interrupt requested, return
		if lastId != inter.Id() {
			return kErrInterrupted
		}
		// If its a timeout error, retry
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		}
		return err
	}
}

func (c *connType) Read(inter *interruptType) (bool, error) {
	conn, br := c._get()
	if br == nil {
		return false, kErrNoConnection
	}
	conn.SetReadDeadline(time.Now().Add(kNetworkTimeout))
	lastId := inter.Id()
	for {
		resp, err := http.ReadResponse(br, nil)
		// If interrupt requested, return
		if lastId != inter.Id() {
			return false, kErrInterrupted
		}
		// If its a timeout error, retry
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		}
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			var buffer bytes.Buffer
			io.Copy(&buffer, resp.Body)
			// 408 is a special error meaning that the connection timed out
			// because we didn't send any request. 408 errors don't go with
			// any particular request. When we get one of these, assume the
			// connection is bad.
			return resp.StatusCode != 408, errors.New(resp.Status + ": " + buffer.String())
		}
		return true, nil
	}
}

func encodeJSON(payload interface{}) (*bytes.Buffer, error) {
	result := &bytes.Buffer{}
	encoder := json.NewEncoder(result)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return result, nil
}
