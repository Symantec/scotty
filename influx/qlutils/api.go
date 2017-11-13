package qlutils

import (
	"errors"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/influxdata/influxql"
	"time"
)

var (
	// means the query contains a statement that is not a select statement.
	ErrNonSelectStatement = errors.New("qlutils: Non select statement")
	ErrUnsupported        = errors.New("qlutils: Unsupported")
)

// SingleQuery turns a statement into a query with that one statement.
func SingleQuery(stmt influxql.Statement) *influxql.Query {
	return &influxql.Query{Statements: influxql.Statements{stmt}}
}

// WithAggregationType returns a statement like stmt that uses aggregation
// as the aggregation function.
func WithAggregationType(stmt influxql.Statement, aggregation string) (
	influxql.Statement, error) {
	return withAggregationType(stmt, aggregation)
}

// AggregationType returns that aggregation function stmt uses in lowercase.
// AggregationType returns an error if stmt doesn't use an aggregation
// function.
func AggregationType(stmt influxql.Statement) (lowercase string, err error) {
	return aggregationType(stmt)
}

// NewQuery creates a new query instance from a string substituting currentTime
// for now().
func NewQuery(ql string, currentTime time.Time) (*influxql.Query, error) {
	return newQuery(ql, currentTime)
}

// QueryTimeRange returns the min and max time for a query. If no min time
// found, min is the zero value; if no max time found, max = now.
func QueryTimeRange(
	query *influxql.Query, now time.Time) (min, max time.Time, err error) {
	return queryTimeRange(query, now)
}

// QuerySetTimeRange returns a query just like query except that it is only
// for times falling between min inclusive and max exclusive. If none of the
// select statements in the query matches the given time range,
// returns nil, nil
func QuerySetTimeRange(
	query *influxql.Query, min, max time.Time) (*influxql.Query, error) {
	return querySetTimeRange(query, min, max)
}

// ParsedQuery converts a query to tsdb ParsedQuery instances for scotty.
// ParsedQuery creates one ParsedQuery instance for each select statement
// in the passed query.
// ParseQuery also returns the column names for each result set. The number
// of column name sets returned always equals the number of ParseQuery
// instances returned.
//
func ParseQuery(query *influxql.Query, now time.Time) (
	parsedQueries []tsdbjson.ParsedQuery,
	columnNameSets [][]string,
	err error) {
	return parseQuery(query, now)
}
