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
)

type connManagerType struct {
	refresh       func() (net.Conn, error)
	lock          sync.Mutex
	active        *connType
	connRefreshes uint64
}

// newConnManager returns a connection manager that creates connections to
// urlStr.
func newConnManager(urlStr string) (*connManagerType, error) {
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
	return &connManagerType{refresh: refresh}, nil
}

// Get returns the current connection. If the current connection is bad,
// Get replaces it with a new connection.
func (c *connManagerType) Get() (*connType, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.active != nil && !c.active.isBad() {
		return c.active.get(), nil
	}
	conn, err := c.refresh()
	if err != nil {
		return nil, err
	}
	c.active = newConn(conn)
	c.connRefreshes++
	return c.active.get(), nil
}

// Refreshes returns the number of connection refreshes. That is, the
// number of times Get() created a new connection to replace a bad connection.
func (c *connManagerType) Refreshes() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.connRefreshes
}

// connType represents a single connection.
type connType struct {
	conn      net.Conn
	br        *bufio.Reader
	lock      sync.Mutex
	userCount int
	bad       bool
}

func newConn(conn net.Conn) *connType {
	return &connType{conn: conn, br: bufio.NewReader(conn)}
}

// Put marks this connection as unused by the caller. Each time the caller
// calls Get() on the connection manager to get a connection, it must call
// Put() on the returned connection when done with it. The caller must not
// use a connection after calling Put on it.
func (c *connType) Put() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.userCount--
	if c.userCount < 0 {
		panic("userCount cannot be negative")
	}
	if c.userCount < 1 && c.bad {
		c.conn.Close()
	}
}

// MarkBad marks this connection as bad. Caller must call MarkBad before
// calling Put.
func (c *connType) MarkBad() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.bad = true
}

func (c *connType) isBad() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.bad
}

func (c *connType) get() *connType {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.userCount++
	return c
}

// Send sends given request on this connection.
func (c *connType) Send(req requestType) error {
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

// Read reads response from the connection returning true if the connection
// is good or false if the connection is bad. If the connection is good,
// err will be non-nil if response read is something other than a 2XX
// response.
func (c *connType) Read() (bool, error) {
	resp, err := http.ReadResponse(c.br, nil)
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

func encodeJSON(payload interface{}) (*bytes.Buffer, error) {
	result := &bytes.Buffer{}
	encoder := json.NewEncoder(result)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return result, nil
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
