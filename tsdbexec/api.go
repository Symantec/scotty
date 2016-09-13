// Package tsdbexec is the top level package for serving tsdb requests.
// Each function in this package corresponds to a TSDB API call.
// Each function takes a request value along with scotty datastructures
// as parameters and returns a response value along with an error.
package tsdbexec

import (
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/tsdbjson"
	"net/http"
	"net/url"
	"time"
)

var (
	// NotFoundHandler is a net/http handler that reports a 404 error
	// the TSDB API way.
	NotFoundHandler = http.HandlerFunc(notFoundHandlerFunc)

	// LiteralOrDesc is the description of the "literal_or" filter
	LiteralOrDesc = &tsdbjson.FilterDescription{
		Examples:    "host=literal_or(web01),  host=literal_or(web01|web02|web03)  {\"type\":\"literal_or\",\"tagk\":\"host\",\"filter\":\"web01|web02|web03\",\"groupBy\":false}",
		Description: "Accepts one or more exact values and matches if the series contains any of them. Multiple values can be included and must be separated by the | (pipe) character. The filter is case sensitive and will not allow characters that TSDB does not allow at write time.",
	}
)

// Suggest corresponds to /api/suggest TSDB API call.
func Suggest(
	params url.Values,
	suggesterMap map[string]suggest.Suggester) (
	result []string, err error) {
	return _suggest(params, suggesterMap)
}

// Query corresponds to the /api/query TSDB API call.
func Query(
	request *tsdbjson.QueryRequest,
	endpoints *datastructs.ApplicationStatuses,
	minDownSampleTime time.Duration) (
	result []tsdbjson.TimeSeries, err error) {
	return query(request, endpoints, minDownSampleTime)
}

// NewHandler creates a handler to service a particular TSDB API endpoint.
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
// The returned handler sends the value that handlerFunc returns as JSON by
// encoding it using the encoding/JSON package. If handlerFunc returns a
// non-nil error, the returned handler sends appropriate JSON for the error
// along with a 400 status code.
func NewHandler(
	handlerFunc interface{}) http.Handler {
	return newHandler(handlerFunc)
}
