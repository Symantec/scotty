package qlutils_test

import (
	"github.com/Symantec/scotty/influx/qlutils"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	now := time.Date(2016, 12, 1, 0, 1, 0, 0, time.UTC)

	Convey("With no time range query", t, func() {

		ql := "select mean(value) from foo;select mean(value) from bar"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Time range should include everything", func() {
			min, max, err := qlutils.QueryTimeRange(query, now)
			So(err, ShouldBeNil)
			So(max, ShouldResemble, now.UTC())
			So(min, ShouldBeZeroValue)
		})

		Convey("Set Time range should go to all statements", func() {

			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-10*time.Hour),
				now.Add(-5*time.Hour))
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T19:01:00Z';
SELECT mean(value) FROM bar WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T19:01:00Z'`,
			)
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

	Convey("With query having non select", t, func() {
		ql := "select mean(value) from foo;create database bar"
		_, err := qlutils.NewQuery(ql, now)

		Convey("ErrNonSelectStatement should be thrown", func() {
			So(err, ShouldEqual, qlutils.ErrNonSelectStatement)
		})

	})

	Convey("With query having lower bound time range only", t, func() {
		ql := "select mean(value) from foo where time >= now() - 13h"

		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Early Time range should produce nil query", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query, now.Add(-15*time.Hour), now.Add(-13*time.Hour))
			So(err, ShouldBeNil)
			So(newQuery, ShouldBeNil)
		})

		Convey("more recent start time should override lower bound", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query, now.Add(-12*time.Hour), now)
			So(err, ShouldBeNil)
			So(newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T12:01:00Z' AND time < '2016-12-01T00:01:00Z'`,
			)
		})
		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)
	})

	Convey("With query having overlapping time ranges", t, func() {

		ql := "select mean(value) from foo where time < now() - 3h and time >= now() - 10h;select mean(value) from bar where time < now() - 6h and time >= now() - 12h"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Time range should go to all statements", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-13*time.Hour),
				now)
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T21:01:00Z';
SELECT mean(value) FROM bar WHERE time >= '2016-11-30T12:01:00Z' AND time < '2016-11-30T18:01:00Z'`,
			)
		})

		Convey("Time range should go to all statements 2", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-11*time.Hour),
				now.Add(-4*time.Hour))
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T14:01:00Z' AND time < '2016-11-30T20:01:00Z';
SELECT mean(value) FROM bar WHERE time >= '2016-11-30T13:01:00Z' AND time < '2016-11-30T18:01:00Z'`,
			)
		})

		Convey("Time range should go to all statements 3", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-9*time.Hour),
				now.Add(-7*time.Hour))
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T15:01:00Z' AND time < '2016-11-30T17:01:00Z';
SELECT mean(value) FROM bar WHERE time >= '2016-11-30T15:01:00Z' AND time < '2016-11-30T17:01:00Z'`,
			)

		})

		Convey("Latter statement skipped with early time range", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-12*time.Hour),
				now.Add(-10*time.Hour))
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM bar WHERE time >= '2016-11-30T12:01:00Z' AND time < '2016-11-30T14:01:00Z'`,
			)

		})

		Convey("Earlier statment skipped with later time range", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-6*time.Hour),
				now.Add(-3*time.Hour))
			So(err, ShouldBeNil)
			So(
				newQuery.String(),
				ShouldEqual,
				`SELECT mean(value) FROM foo WHERE time >= '2016-11-30T18:01:00Z' AND time < '2016-11-30T21:01:00Z'`,
			)
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

	Convey("With query having non overlapping time ranges", t, func() {

		ql := "select mean(value) from foo where time < now() - 3h and time >= now() - 6h;select mean(value) from bar where time < now() - 10h and time >= now() - 12h"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Time range not falling in any statement should return nil", func() {
			newQuery, err := qlutils.QuerySetTimeRange(
				query,
				now.Add(-10*time.Hour),
				now.Add(-6*time.Hour))
			So(err, ShouldBeNil)
			So(newQuery, ShouldBeNil)
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

}
