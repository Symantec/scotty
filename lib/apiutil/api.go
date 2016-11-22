package apiutil

import (
	"net/http"
)

type Options struct {
	// Returns pointer to structure for JSON encoded error message.
	// nil generates default json with a single 'error' field.
	ErrorGenerator func(status int, err error) interface{}
}

// HTTPError is an error that includes an HTTP status code.
type HTTPError interface {
	error
	// The HTTP status code
	Status() int
}

// NewHandler creates a handler to service a particular API endpoint.
//
// The parameter, handlerFunc, is a function that handles the API requests to
// the endpoint. handlerFunc must be a function that takes one parameter, the
// input to the API, and returns 2 values, the output and error.
//
// If the handlerFunc parameter is a slice, map, or pointer to a struct,
// the returned handler will translate the input stream into a value that can
// be passed to handlerFunc using the encoding/json package.
//
// If the handlerFunc parameter is a url.Values type, the returned handler will
// pass the URL parameters of the API request to handlerFunc.
//
// Caller may pass nil for options parameter if they want all defaults.
//
// The returned handler sends the value that handlerFunc returns as JSON by
// encoding it using the encoding/JSON package. If handlerFunc returns a
// non-nil error, the returned handler sends appropriate JSON for the error
// along with a 400 status code. If handlerFunc returns a non-nil HTTPError,
// the returned handler marshals the returned error as-is without using the
// ErrorGenerator function in options and sets the status code according to
// what Status() returns.
func NewHandler(handlerFunc interface{}, options *Options) http.Handler {
	return newHandler(handlerFunc, options)
}
