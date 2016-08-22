// Package tsdbexec is the top level package for serving tsdb requests.
// Each function in this package corresponds to a TSDB API call.
// Each function takes a request value along with scotty datastructures
// as parameters and returns a response value along with an error.
package tsdbexec

import (
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/tsdbjson"
)

// Query corresponds to the /api/query TSDB API call.
func Query(
	request *tsdbjson.QueryRequest,
	endpoints *datastructs.ApplicationStatuses) (
	result []tsdbjson.TimeSeries, err error) {
	return query(request, endpoints)
}
