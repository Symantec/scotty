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
)

var (
	kErrNoConnection = errors.New("queuesender: No connection")
)

// connType represents a single connection.
type connType struct {
	conn    net.Conn
	br      *bufio.Reader
	refresh func() (net.Conn, error)
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

func (c *connType) RefreshAndClose(logger *loggerType) *connType {
	if c.conn != nil {
		c.conn.Close()
	}
	newConn, err := c.refresh()
	if err != nil {
		logger.Log(err)
	}
	return _newConn(newConn, c.refresh)
}

func (c *connType) Send(req requestType) error {
	if c.conn == nil {
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
	return httpreq.Write(c.conn)
}

func (c *connType) Read() (bool, error) {
	if c.br == nil {
		return false, kErrNoConnection
	}
	resp, err := http.ReadResponse(c.br, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var buffer bytes.Buffer
		io.Copy(&buffer, resp.Body)
		return true, errors.New(resp.Status + ": " + buffer.String())
	}
	return true, nil
}

func encodeJSON(payload interface{}) (*bytes.Buffer, error) {
	result := &bytes.Buffer{}
	encoder := json.NewEncoder(result)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return result, nil
}
