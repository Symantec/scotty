package qlutils

import (
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/influxdata/influxdb/influxql"
	"time"
)

const (
	kInfluxHost    = "host"
	kInfluxAppName = "appname"
)

type tsdbAggSpecType struct {
	Agg        string
	Downsample string
}

var (
	kTsdbAggSpecsByInfluxName = map[string]*tsdbAggSpecType{
		"mean":  &tsdbAggSpecType{Agg: "avg", Downsample: "avg"},
		"sum":   &tsdbAggSpecType{Agg: "sum", Downsample: "sum"},
		"count": &tsdbAggSpecType{Agg: "sum", Downsample: "count"},
	}
)

func parseQuery(query *influxql.Query, now time.Time) (
	[]tsdbjson.ParsedQuery, [][]string, error) {
	result := make([]tsdbjson.ParsedQuery, len(query.Statements))
	colNames := make([][]string, len(query.Statements))
	for i := range result {
		var err error
		result[i], colNames[i], err = parseStatement(query.Statements[i], now)
		if err != nil {
			return nil, nil, err
		}
	}
	return result, colNames, nil
}

func parseStatement(stmt influxql.Statement, currentTime time.Time) (
	result tsdbjson.ParsedQuery, colNames []string, err error) {
	sel, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		err = ErrUnsupported
		return
	}
	// check for unsupported features
	if sel.Fill != influxql.NullFill {
		err = ErrUnsupported
		return
	}
	if sel.Limit != 0 || sel.Offset != 0 || sel.SLimit != 0 || sel.SOffset != 0 {
		err = ErrUnsupported
		return
	}
	if sel.TimeAlias != "" {
		err = ErrUnsupported
		return
	}
	if sel.OmitTime {
		err = ErrUnsupported
		return
	}
	if sel.IsRawQuery {
		err = ErrUnsupported
		return
	}
	if len(sel.SortFields) > 0 {
		err = ErrUnsupported
		return
	}

	// Get min and max time
	minTime, maxTime, err := influxql.TimeRange(sel.Condition, time.UTC)
	if err != nil {
		return
	}
	if maxTime.IsZero() {
		maxTime = currentTime.UTC()
	} else {
		maxTime = maxTime.Add(1)
	}
	start := duration.TimeToFloat(minTime)
	end := duration.TimeToFloat(maxTime)
	if start < 0.0 || end < 0.0 {
		err = ErrUnsupported
		return
	}

	// Get name of measurement
	if len(sel.Sources) != 1 {
		err = ErrUnsupported
		return
	}
	measurement, ok := sel.Sources[0].(*influxql.Measurement)
	if !ok {
		err = ErrUnsupported
		return
	}
	measurementName := measurement.Name

	// Get aggregator
	fields := sel.Fields
	if len(fields) != 1 {
		err = ErrUnsupported
		return
	}
	field := fields[0]
	call, ok := field.Expr.(*influxql.Call)
	if !ok {
		err = ErrUnsupported
		return
	}
	if len(call.Args) != 1 {
		err = ErrUnsupported
		return
	}
	varRef, ok := call.Args[0].(*influxql.VarRef)
	if !ok {
		err = ErrUnsupported
		return
	}
	if varRef.Val != "value" {
		err = ErrUnsupported
		return
	}
	aggSpec, ok := kTsdbAggSpecsByInfluxName[call.Name]
	if !ok {
		err = ErrUnsupported
		return
	}

	err = parseWhereClause(sel.Condition, &result.Options)
	if err != nil {
		return
	}

	var dur time.Duration
	dur, err = parseGroupByClause(sel.Dimensions, &result.Options)
	if err != nil {
		return
	}

	result.Metric = measurementName
	result.Aggregator.Type = aggSpec.Agg
	result.Aggregator.DownSample = &tsdbjson.DownSampleSpec{
		DurationInSeconds: float64(dur / time.Second),
		Type:              aggSpec.Downsample,
	}
	result.Start = duration.TimeToFloat(minTime)
	result.End = duration.TimeToFloat(maxTime)
	colNames = sel.ColumnNames()

	return
}

func parseGroupByClause(
	dimensions influxql.Dimensions,
	options *tsdbjson.ParsedQueryOptions) (time.Duration, error) {
	dur, tags := dimensions.Normalize()
	for _, tag := range tags {
		switch tag {
		case kInfluxHost:
			options.GroupByHostName = true
		case kInfluxAppName:
			options.GroupByAppName = true
		}
	}
	return dur, nil
}

func parseWhereClause(
	whereClause influxql.Expr, options *tsdbjson.ParsedQueryOptions) error {
	switch expr := whereClause.(type) {
	case *influxql.BinaryExpr:
		switch expr.Op {
		case influxql.AND:
			if err := parseWhereClause(expr.LHS, options); err != nil {
				return err
			}
			if err := parseWhereClause(expr.RHS, options); err != nil {
				return err
			}
			return nil
		default:
			return parseWCSingle(expr, options)
		}
	default:
		return ErrUnsupported
	}
}

func parseWCSingle(
	single *influxql.BinaryExpr, options *tsdbjson.ParsedQueryOptions) error {
	switch single.Op {
	case influxql.EQ:
		return parseWCEqual(single, options)
	default:
		return parseWCOther(single)
	}
}

func parseWCEqual(
	single *influxql.BinaryExpr, options *tsdbjson.ParsedQueryOptions) error {
	vref, ok := single.LHS.(*influxql.VarRef)
	if !ok {
		return ErrUnsupported
	}
	lit, ok := single.RHS.(*influxql.StringLiteral)
	if !ok {
		return ErrUnsupported
	}
	switch vref.Val {
	case kInfluxHost:
		if options.HostNameFilter != nil {
			return ErrUnsupported
		}
		options.HostNameFilter = &tsdbjson.FilterSpec{
			Type:  "literal_or",
			Value: lit.Val,
		}
		return nil
	case kInfluxAppName:
		if options.AppNameFilter != nil {
			return ErrUnsupported
		}
		options.AppNameFilter = &tsdbjson.FilterSpec{
			Type:  "literal_or",
			Value: lit.Val,
		}
		return nil
	default:
		return ErrUnsupported

	}
}

func parseWCOther(single *influxql.BinaryExpr) error {
	vref, ok := single.LHS.(*influxql.VarRef)
	if !ok {
		return ErrUnsupported
	}
	if vref.Val == "time" {
		return nil
	}
	return ErrUnsupported
}
