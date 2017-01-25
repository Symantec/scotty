package qlutils_test

import (
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var (
	kNow = time.Date(2016, 12, 1, 0, 1, 0, 0, time.UTC)
)

func TestConvert(t *testing.T) {
	now := kNow

	Convey("With simple query", t, func() {

		ql := "select mean(value) from \"a/metric\" WHERE time > now() - 2h group by time(10m)"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Conversion should succeed", func() {
			pq, colNames, err := qlutils.ParseQuery(query, now)
			So(err, ShouldBeNil)
			So(pq, ShouldHaveLength, 1)
			So(
				pq[0],
				ShouldResemble,
				tsdbjson.ParsedQuery{
					Metric: "a/metric",
					Aggregator: tsdbjson.AggregatorSpec{
						Type: "avg",
						DownSample: &tsdbjson.DownSampleSpec{
							DurationInSeconds: 600.0,
							Type:              "avg",
						},
					},
					Start: duration.TimeToFloat(now) - 7200.0,
					End:   duration.TimeToFloat(now),
				},
			)
			So(colNames, ShouldResemble, [][]string{{"time", "mean"}})
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

	Convey("With where clause query", t, func() {

		ql := "select count(value) from \"/b/metric\" WHERE host='some-host' and appname = 'some-app' and time > now() - 1h group by time(5m)"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Conversion should succeed", func() {
			pq, colNames, err := qlutils.ParseQuery(query, now)
			So(err, ShouldBeNil)
			So(pq, ShouldHaveLength, 1)
			So(
				pq[0],
				ShouldResemble,
				tsdbjson.ParsedQuery{
					Metric: "/b/metric",
					Aggregator: tsdbjson.AggregatorSpec{
						Type: "sum",
						DownSample: &tsdbjson.DownSampleSpec{
							DurationInSeconds: 300.0,
							Type:              "count",
						},
					},
					Start: duration.TimeToFloat(now) - 3600.0,
					End:   duration.TimeToFloat(now),
					Options: tsdbjson.ParsedQueryOptions{
						HostNameFilter: &tsdbjson.FilterSpec{
							Type:  "literal_or",
							Value: "some-host",
						},
						AppNameFilter: &tsdbjson.FilterSpec{
							Type:  "literal_or",
							Value: "some-app",
						},
					},
				},
			)
			So(colNames, ShouldResemble, [][]string{{"time", "count"}})
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

	Convey("With group by query", t, func() {

		ql := "select sum(value) from \"/c/metric\" WHERE time > now() - 30m group by host, appname, time(6m)"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)
		origQueryString := query.String()

		Convey("Conversion should succeed", func() {
			pq, colNames, err := qlutils.ParseQuery(query, now)
			So(err, ShouldBeNil)
			So(pq, ShouldHaveLength, 1)
			So(
				pq[0],
				ShouldResemble,
				tsdbjson.ParsedQuery{
					Metric: "/c/metric",
					Aggregator: tsdbjson.AggregatorSpec{
						Type: "sum",
						DownSample: &tsdbjson.DownSampleSpec{
							DurationInSeconds: 360.0,
							Type:              "sum",
						},
					},
					Start: duration.TimeToFloat(now) - 1800.0,
					End:   duration.TimeToFloat(now),
					Options: tsdbjson.ParsedQueryOptions{
						GroupByHostName: true,
						GroupByAppName:  true,
					},
				},
			)
			So(colNames, ShouldResemble, [][]string{{"time", "sum"}})
		})

		// Original query must remain unchanged
		So(query.String(), ShouldEqual, origQueryString)

	})

	Convey("With all caps aggregator query", t, func() {

		ql := "SELECT SUM(value) FROM \"/c/metric\" WHERE time > now() - 30m group by host, appname, time(6m); SELECT MEAN(value) FROM \"/c/metric\" WHERE time > now() - 45m group by host, time(9m)"
		query, err := qlutils.NewQuery(ql, now)
		So(err, ShouldBeNil)

		Convey("aggregators not case sensitive", func() {
			pq, _, err := qlutils.ParseQuery(query, now)
			So(err, ShouldBeNil)
			So(pq, ShouldHaveLength, 2)
			So(pq[0].Aggregator.Type, ShouldEqual, "sum")
			So(pq[1].Aggregator.Type, ShouldEqual, "avg")
		})
	})

	Convey("Usupported queries give an error", t, func() {
		checkUnsupported("select value from \"metric\"")
		checkUnsupported("select distinct value from \"metric\"")
		checkUnsupported("select mean(value) from \"metric\"")
		checkUnsupported("select mean(value) from \"a/metric\" WHERE time > now() - 2h group by time(10m) fill(none)")
		checkUnsupported("select mean(value) from \"a/metric\" WHERE time > now() - 2h group by time(10m) limit 5")
		checkUnsupported("select mean(value) from \"a/metric\" WHERE time > now() - 2h group by time(10m) order by time asc")
		checkUnsupported("select mean(value) from \"a/metric\" WHERE time > '1968-08-28T23:00:01.232000000Z' group by time(10m)")
	})
}

func checkUnsupported(q string) {
	Convey(q, func() {
		query, err := qlutils.NewQuery(q, kNow)
		So(err, ShouldBeNil)
		_, _, err = qlutils.ParseQuery(query, kNow)
		So(err, ShouldEqual, qlutils.ErrUnsupported)
	})
}
