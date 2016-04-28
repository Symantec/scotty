// Package httputil contains various http utilities.
package httputil

import (
	"net/url"
)

// NewUrl returns a new URL with a given path and parameters.
// nameValues is parameter name, parameter value, parameter name, parameter
// value, etc. nameValues must have even length.
func NewUrl(path string, nameValues ...string) *url.URL {
	return newUrl(path, nameValues)
}
