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

// AppendParams returns a URL with new parameters appended. No existing
// parameter is replaced. u is the original URL; nameValues is
// parameter name, parameter value, parameter name, parameter
// value, etc. nameValues must have even length.
func AppendParams(u *url.URL, nameValues ...string) *url.URL {
	return appendParams(u, nameValues)
}

// WithParams returns a URL with new parameters. If the parameters
// already exist in the original URL, they are replaced.
// u is the original URL;
// nameValues is parameter name, parameter value, parameter name, parameter
// value, etc. nameValues must have even length.
func WithParams(u *url.URL, nameValues ...string) *url.URL {
	return withParams(u, nameValues)
}
