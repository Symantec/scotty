package qlutils

import (
	"github.com/influxdata/influxdb/influxql"
	"time"
)

func statementTimeRange(stmt influxql.Statement, currentTime time.Time) (
	min, max time.Time, err error) {
	sel, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		err = ErrNonSelectStatement
		return
	}
	min, max, err = influxql.TimeRange(sel.Condition)
	if err != nil {
		return
	}
	if max.IsZero() {
		max = currentTime.UTC()
	} else {
		max = max.Add(1)
	}
	return
}

func newQuery(sql string, currentTime time.Time) (*influxql.Query, error) {
	query, err := influxql.ParseQuery(sql)
	if err != nil {
		return nil, err
	}
	now := currentTime.UTC()
	nowValuer := influxql.NowValuer{Now: now}
	for _, stmt := range query.Statements {
		sel, ok := stmt.(*influxql.SelectStatement)
		if !ok {
			return nil, ErrNonSelectStatement
		}
		sel.Condition = influxql.Reduce(sel.Condition, &nowValuer)
	}
	return query, nil
}

func queryTimeRange(query *influxql.Query, currentTime time.Time) (
	min, max time.Time, err error) {
	if len(query.Statements) == 0 {
		return
	}
	tMin, tMax, err := statementTimeRange(query.Statements[0], currentTime)
	if err != nil {
		return
	}
	for _, stmt := range query.Statements[1:] {
		var cMin, cMax time.Time
		cMin, cMax, err = statementTimeRange(stmt, currentTime)
		if err != nil {
			return
		}
		if cMin.Before(tMin) {
			tMin = cMin
		}
		if cMax.After(tMax) {
			tMax = cMax
		}
	}
	min = tMin
	max = tMax
	return
}

func querySetTimeRange(
	query *influxql.Query, min, max time.Time) (*influxql.Query, error) {
	var result influxql.Query
	for _, stmt := range query.Statements {
		cMin, cMax, err := statementTimeRange(stmt, max)
		if err != nil {
			return nil, err
		}
		// If the select statement falls within desired min and max time
		if min.Before(cMax) && max.After(cMin) {
			if min.After(cMin) {
				cMin = min
			}
			if max.Before(cMax) {
				cMax = max
			}
			sel := stmt.(*influxql.SelectStatement)
			nsel, err := setTimeRange(sel, cMin, cMax)
			if err != nil {
				return nil, err
			}
			result.Statements = append(result.Statements, nsel)
		}
	}
	if len(result.Statements) == 0 {
		return nil, nil
	}
	return &result, nil
}

func setTimeRange(
	stmt *influxql.SelectStatement,
	min, max time.Time) (*influxql.SelectStatement, error) {
	stmtCopy := stmt.Clone()
	if err := stmtCopy.SetTimeRange(min, max); err != nil {
		return nil, err
	}
	return stmtCopy, nil
}
