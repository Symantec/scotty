package tsdbjson_test

import (
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbjson"
	"reflect"
	"sort"
	"testing"
)

func TestJson(t *testing.T) {
	timeSeriesSet := &tsdb.TaggedTimeSeriesSet{
		MetricName: "someMetric",
		Data: []tsdb.TaggedTimeSeries{
			{
				Values: tsdb.TimeSeries{
					{48.0, 72.0},
					{55.0, 93.0},
				},
			},
		},
	}
	expected := []tsdbjson.TimeSeries{
		{
			Metric:        "someMetric",
			Tags:          map[string]string{},
			AggregateTags: []string{"HostName", "appname"},
			Dps: tsdb.TimeSeries{
				{48.0, 72.0},
				{55.0, 93.0},
			},
		},
	}
	assertTimeSeriesSliceEquals(
		t, expected, tsdbjson.NewTimeSeriesSlice(timeSeriesSet))

	timeSeriesSet = &tsdb.TaggedTimeSeriesSet{
		MetricName: "someMetric",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host1",
				},
				Values: tsdb.TimeSeries{
					{48.0, 72.0},
					{55.0, 93.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
				},
				Values: tsdb.TimeSeries{
					{21.0, 29.0},
				},
			},
		},
		GroupedByHostName: true,
	}
	expected = []tsdbjson.TimeSeries{
		{
			Metric:        "someMetric",
			Tags:          map[string]string{"HostName": "host1"},
			AggregateTags: []string{"appname"},
			Dps: tsdb.TimeSeries{
				{48.0, 72.0},
				{55.0, 93.0},
			},
		},
		{
			Metric:        "someMetric",
			Tags:          map[string]string{"HostName": "host2"},
			AggregateTags: []string{"appname"},
			Dps: tsdb.TimeSeries{
				{21.0, 29.0},
			},
		},
	}
	assertTimeSeriesSliceEquals(
		t, expected, tsdbjson.NewTimeSeriesSlice(timeSeriesSet))

	timeSeriesSet = &tsdb.TaggedTimeSeriesSet{
		MetricName: "someMetric",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					AppName: "app1",
				},
				Values: tsdb.TimeSeries{
					{2.0, 3.0},
					{5.0, 8.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					AppName: "app2",
				},
				Values: tsdb.TimeSeries{
					{13.0, 21.0},
				},
			},
		},
		GroupedByAppName: true,
	}
	expected = []tsdbjson.TimeSeries{
		{
			Metric:        "someMetric",
			Tags:          map[string]string{"appname": "app1"},
			AggregateTags: []string{"HostName"},
			Dps: tsdb.TimeSeries{
				{2.0, 3.0},
				{5.0, 8.0},
			},
		},
		{
			Metric:        "someMetric",
			Tags:          map[string]string{"appname": "app2"},
			AggregateTags: []string{"HostName"},
			Dps: tsdb.TimeSeries{
				{13.0, 21.0},
			},
		},
	}
	assertTimeSeriesSliceEquals(
		t, expected, tsdbjson.NewTimeSeriesSlice(timeSeriesSet))

	timeSeriesSet = &tsdb.TaggedTimeSeriesSet{
		MetricName: "someMetric",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host1",
					AppName:  "app1",
				},
				Values: tsdb.TimeSeries{
					{101.0, 202.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
					AppName:  "app2",
				},
				Values: tsdb.TimeSeries{
					{303.0, 404.0},
				},
			},
		},
		GroupedByHostName: true,
		GroupedByAppName:  true,
	}
	expected = []tsdbjson.TimeSeries{
		{
			Metric: "someMetric",
			Tags: map[string]string{
				"HostName": "host1",
				"appname":  "app1",
			},
			AggregateTags: []string{},
			Dps: tsdb.TimeSeries{
				{101.0, 202.0},
			},
		},
		{
			Metric: "someMetric",
			Tags: map[string]string{
				"HostName": "host2",
				"appname":  "app2",
			},
			AggregateTags: []string{},
			Dps: tsdb.TimeSeries{
				{303.0, 404.0},
			},
		},
	}
	assertTimeSeriesSliceEquals(
		t, expected, tsdbjson.NewTimeSeriesSlice(timeSeriesSet))
}

func TestParseQueryBadRequest(t *testing.T) {
	request := &tsdbjson.QueryRequest{
		StartInMillis: 1456789123125,
		EndInMillis:   1511789123125,
		Queries: []*tsdbjson.Query{
			{
				Metric:     "Hi_20there",
				Aggregator: "avg",
				DownSample: "abadvalue",
			},
		},
	}
	_, err := tsdbjson.ParseQueryRequest(request)
	assertValueEquals(t, tsdbjson.ErrBadValue, err)
}

func TestEnsureStartTimeRecentEnough(t *testing.T) {
	pq := tsdbjson.ParsedQuery{
		Start: 100000.0,
		End:   900000.0,
	}
	// Should be no-op
	pq.EnsureStartTimeRecentEnough()
	assertValueEquals(t, 100000.0, pq.Start)
	assertValueEquals(t, 900000.0, pq.End)

	pq = tsdbjson.ParsedQuery{
		Start: 100000.0,
		End:   900000.0,
		Aggregator: tsdbjson.AggregatorSpec{
			DownSample: &tsdbjson.DownSampleSpec{
				DurationInSeconds: 50.0,
			},
		},
	}

	pq.EnsureStartTimeRecentEnough()
	assertValueEquals(t, 850000.0, pq.Start)
	assertValueEquals(t, 900000.0, pq.End)

	// Should be no-op already adjusted
	pq.EnsureStartTimeRecentEnough()
	assertValueEquals(t, 850000.0, pq.Start)
	assertValueEquals(t, 900000.0, pq.End)
}

func TestParseQueryRequest(t *testing.T) {
	request := &tsdbjson.QueryRequest{
		StartInMillis: 1456789123125,
		EndInMillis:   1511789123125,
		Queries: []*tsdbjson.Query{
			{
				Metric:     "Hi_20there",
				Aggregator: "avg",
				DownSample: "15m-avg-zero",
			},
			{
				Metric:     "What_27s_20up",
				Aggregator: "sum",
				DownSample: "10m-avg",
				Filters: []*tsdbjson.Filter{
					{
						Type:    "literal_or",
						Tagk:    "HostName",
						Filter:  "some_20Host",
						GroupBy: true,
					},
					{
						Type:    "wildcard",
						Tagk:    "appname",
						Filter:  "some_20App",
						GroupBy: false,
					},
					{
						Type:    "literal_or",
						Tagk:    "region",
						Filter:  "some_20Region",
						GroupBy: true,
					},
					{
						Type:    "literal_or",
						Tagk:    "ipaddress",
						Filter:  "some_20IpAddress",
						GroupBy: true,
					},
				},
			},
			{
				Metric:     "AThird",
				Aggregator: "prod",
			},
		},
	}
	parsedRequests, err := tsdbjson.ParseQueryRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	expected := []tsdbjson.ParsedQuery{
		{
			Metric: "Hi there",
			Aggregator: tsdbjson.AggregatorSpec{
				Type: "avg",
				DownSample: &tsdbjson.DownSampleSpec{
					DurationInSeconds: 900.0,
					Type:              "avg",
					Fill:              "zero",
				},
			},
			Start: 1456789123.125,
			End:   1511789123.125,
		},
		{
			Metric: "What's up",
			Aggregator: tsdbjson.AggregatorSpec{
				Type: "sum",
				DownSample: &tsdbjson.DownSampleSpec{
					DurationInSeconds: 600.0,
					Type:              "avg",
				},
			},
			Start: 1456789123.125,
			End:   1511789123.125,
			Options: tsdbjson.ParsedQueryOptions{
				HostNameFilter: &tsdbjson.FilterSpec{
					Type:  "literal_or",
					Value: "some_20Host",
				},
				AppNameFilter: &tsdbjson.FilterSpec{
					Type:  "wildcard",
					Value: "some_20App",
				},
				RegionFilter: &tsdbjson.FilterSpec{
					Type:  "literal_or",
					Value: "some_20Region",
				},
				IpAddressFilter: &tsdbjson.FilterSpec{
					Type:  "literal_or",
					Value: "some_20IpAddress",
				},
				GroupByHostName:  true,
				GroupByRegion:    true,
				GroupByIpAddress: true,
			},
		},
		{
			Metric: "AThird",
			Aggregator: tsdbjson.AggregatorSpec{
				Type: "prod",
			},
			Start: 1456789123.125,
			End:   1511789123.125,
		},
	}
	assertValueDeepEquals(t, expected, parsedRequests)
}

func TestTagFilter(t *testing.T) {
	tagFilter, err := tsdbjson.NewTagFilter(
		"literal_or", "Bad_20To|the_20bone")
	if err != nil {
		t.Fatal(err)
	}
	assertValueEquals(t, true, tagFilter.Filter("Bad To"))
	assertValueEquals(t, true, tagFilter.Filter("the bone"))
	assertValueEquals(t, false, tagFilter.Filter("the bon"))

	_, err = tsdbjson.NewTagFilter("NoSuchFilter", "abcd")
	assertValueEquals(t, tsdbjson.ErrUnsupportedFilter, err)
}

func TestEscape(t *testing.T) {
	assertValueEquals(t, "motown", escape("motown"))
	assertValueEquals(t, "mo_20town", escape("mo town"))
	assertValueEquals(t, "_5Fa_5Fb_5Fc_5F", escape("_a_b_c_"))
}

func TestUnescape(t *testing.T) {
	assertValueEquals(t, "What now", unescape("What_20now"))
	assertValueEquals(t, "strange_one", unescape("strange_one"))
	assertValueEquals(t, "strange_one", unescape("strange_one"))
	assertValueEquals(t, "_9", unescape("_9"))
	assertValueEquals(t, "big", unescape("big"))
	assertValueEquals(t, "x+y", unescape("x_2by"))
	assertValueEquals(t, "x+y", unescape("x_2By"))
}

func escape(orig string) string {
	timeSeriesSet := &tsdb.TaggedTimeSeriesSet{
		MetricName: orig,
		Data: []tsdb.TaggedTimeSeries{
			{
				Values: tsdb.TimeSeries{
					{2.0, 3.0},
				},
			},
		},
	}
	result := tsdbjson.NewTimeSeriesSlice(timeSeriesSet)
	return result[0].Metric
}

func unescape(orig string) string {
	request := &tsdbjson.QueryRequest{
		StartInMillis: 1456789123125,
		EndInMillis:   1511789123125,
		Queries: []*tsdbjson.Query{
			{
				Metric:     orig,
				Aggregator: "avg",
				DownSample: "15m-avg-zero",
			},
		},
	}
	parsedRequests, err := tsdbjson.ParseQueryRequest(request)
	if err != nil {
		panic(err)
	}
	return parsedRequests[0].Metric
}

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}

func assertTimeSeriesSliceEquals(
	t *testing.T,
	expected, actual []tsdbjson.TimeSeries) {
	copyOfActual := make([]tsdbjson.TimeSeries, len(actual))
	copy(copyOfActual, actual)
	for i := range copyOfActual {
		copyOfActual[i].AggregateTags = sortedStrings(
			copyOfActual[i].AggregateTags)
	}
	assertValueDeepEquals(t, expected, copyOfActual)
}

func sortedStrings(src []string) (result []string) {
	if src == nil {
		return nil
	}
	result = make([]string, len(src))
	copy(result, src)
	sort.Strings(result)
	return
}

func assertValueDeepEquals(
	t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}
