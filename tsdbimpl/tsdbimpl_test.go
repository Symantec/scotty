package tsdbimpl_test

import (
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/scotty/sources/trisource"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/scotty/tsdbimpl"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"reflect"
	"sort"
	"testing"
)

func addValues(
	t *testing.T,
	aStore *store.Store,
	endpointId interface{},
	path string,
	data ...float64) {
	if len(data)%2 != 0 {
		t.Fatal("Timestamp value pairs expected")
	}
	aMetric := metrics.SimpleList{
		{
			Path:        path,
			Description: "A description",
		},
	}
	dataLen := len(data)
	for i := 0; i < dataLen; i += 2 {
		aMetric[0].TimeStamp = duration.FloatToTime(data[i])
		aMetric[0].Value = data[i+1]
		if _, err := aStore.AddBatch(endpointId, 1000.0, aMetric); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAPI(t *testing.T) {
	alBuilder := datastructs.NewApplicationListBuilder()
	alBuilder.Add(
		37, "AnApp", sources.ConnectorList{trisource.GetConnector()})
	alBuilder.Add(
		97, "AnotherApp", sources.ConnectorList{trisource.GetConnector()})
	appList := alBuilder.Build()
	appStatus := datastructs.NewApplicationStatuses(
		appList,
		newStore(
			t, "TestAPI", 2, 100, 1.0, 10))
	appStatus.MarkHostsActiveExclusively(
		100.0,
		[]string{"host1", "host2", "host3", "host4", "host5"})
	endpointId, aStore := appStatus.EndpointIdByHostAndName(
		"host1", "AnApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 30.0, 510.0, 31.0, 520.0, 32.0, 530.0, 33.0, 540.0, 34.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host1", "AnotherApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 40.0, 510.0, 41.0, 520.0, 42.0, 530.0, 43.0, 540.0, 44.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 50.0, 510.0, 51.0, 520.0, 52.0, 530.0, 53.0, 540.0, 54.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host2", "AnotherApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 60.0, 510.0, 61.0, 520.0, 62.0, 530.0, 63.0, 540.0, 64.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 70.0, 510.0, 71.0, 520.0, 72.0, 530.0, 73.0, 540.0, 74.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host3", "AnotherApp")
	addValues(t, aStore, endpointId, "/foo",
		500.0, 80.0, 510.0, 81.0, 520.0, 82.0, 530.0, 83.0, 540.0, 84.0)
	endpointId, aStore = appStatus.EndpointIdByHostAndName(
		"host5", "AnApp")
	addValues(t, aStore, endpointId, "/bar",
		700.0, 80.0, 800.0, 100.0)
	var taggedTimeSeriesSet *tsdb.TaggedTimeSeriesSet
	var err error
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		500.0, 600.0,
		nil); err != nil {
		t.Fatal(err)
	}
	expected := &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Values: tsdb.TimeSeries{
					{500.0, 55.5}, {520.0, 57.5}, {540.0, 59.0},
				},
			},
		},
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	if _, err = tsdbimpl.Query(
		appStatus,
		"/not there",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		500.0, 600.0,
		nil); err != tsdbimpl.ErrNoSuchMetric {
		t.Error("Expected ErrNoSuchName")
	}

	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/bar",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		500.0, 1000.0,
		nil); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/bar",
		Data: []tsdb.TaggedTimeSeries{
			{
				Values: tsdb.TimeSeries{
					{700.0, 80.0}, {800.0, 100.0},
				},
			},
		},
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		501.0, 539.0,
		nil); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options := &tsdbimpl.QueryOptions{
		GroupByHostName: true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host1",
				},
				Values: tsdb.TimeSeries{
					{500.0, 35.5}, {520.0, 37.5}, {540.0, 39.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
				},
				Values: tsdb.TimeSeries{
					{500.0, 55.5}, {520.0, 57.5}, {540.0, 59.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
				},
				Values: tsdb.TimeSeries{
					{500.0, 75.5}, {520.0, 77.5}, {540.0, 79.0},
				},
			},
		},
		GroupedByHostName: true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		GroupByHostName: true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		700.0, 900.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName:        "/foo",
		GroupedByHostName: true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		GroupByAppName: true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					AppName: "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					AppName: "AnotherApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 60.5}, {520.0, 62.5}, {540.0, 64},
				},
			},
		},
		GroupedByAppName: true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		GroupByHostName: true,
		GroupByAppName:  true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host1",
					AppName:  "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 30.5}, {520.0, 32.5}, {540.0, 34.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host1",
					AppName:  "AnotherApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 40.5}, {520.0, 42.5}, {540.0, 44.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
					AppName:  "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
					AppName:  "AnotherApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 60.5}, {520.0, 62.5}, {540.0, 64.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
					AppName:  "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 70.5}, {520.0, 72.5}, {540.0, 74.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
					AppName:  "AnotherApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 80.5}, {520.0, 82.5}, {540.0, 84.0},
				},
			},
		},
		GroupedByHostName: true,
		GroupedByAppName:  true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		GroupByHostName: true,
		GroupByAppName:  true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		700.0, 900.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName:        "/foo",
		GroupedByHostName: true,
		GroupedByAppName:  true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		HostNameFilter: tagFilter(func(s string) bool {
			return s == "host2" || s == "host3"
		}),
		AppNameFilter: tagFilter(func(s string) bool {
			return s == "AnApp"
		}),
		GroupByHostName: true,
		GroupByAppName:  true,
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
					AppName:  "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
					AppName:  "AnApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 70.5}, {520.0, 72.5}, {540.0, 74.0},
				},
			},
		},
		GroupedByHostName: true,
		GroupedByAppName:  true,
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		HostNameFilter: tagFilter(func(s string) bool {
			return s == "host2" || s == "host3"
		}),
		AppNameFilter: tagFilter(func(s string) bool {
			return s == "AnApp"
		}),
	}
	if taggedTimeSeriesSet, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Values: tsdb.TimeSeries{
					{500.0, 60.5}, {520.0, 62.5}, {540.0, 64.0},
				},
			},
		},
	}
	assertTaggedTimeSeriesSetEquals(
		t,
		expected,
		taggedTimeSeriesSet,
	)

	options = &tsdbimpl.QueryOptions{
		HostNameFilter: tagFilter(func(s string) bool {
			return s == "host2" || s == "host3"
		}),
		AppNameFilter: tagFilter(func(s string) bool {
			return s == "No app"
		}),
	}
	if _, err = tsdbimpl.Query(
		appStatus,
		"/foo",
		func(start, end float64) (tsdb.Aggregator, error) {
			return aggregators.New(
				start,
				end,
				aggregators.Avg,
				20.0,
				aggregators.Avg,
				aggregators.NaN,
				nil), nil
		},
		400.0, 700.0,
		options); err != tsdbimpl.ErrNoSuchMetric {
		t.Error("Expected ErrNoSuchMetric")
	}
}

func newStore(
	t *testing.T,
	testName string,
	valueCount,
	pageCount uint,
	inactiveThreshhold float64,
	degree uint) *store.Store {
	result := store.NewStore(
		valueCount, pageCount, inactiveThreshhold, degree)
	dirSpec, err := tricorder.RegisterDirectory("/" + testName)
	if err != nil {
		t.Fatalf("Duplicate test: %s", testName)
	}
	result.RegisterMetrics(dirSpec)
	return result
}

func assertTaggedTimeSeriesSetEquals(
	t *testing.T,
	expected, actual *tsdb.TaggedTimeSeriesSet) {
	// Adjust actual by sorting the tags
	copyOfActual := *actual
	copyOfActual.Data = sortedByTags(copyOfActual.Data)
	assertValueDeepEquals(t, expected, &copyOfActual)
}

func sortedByTags(orig []tsdb.TaggedTimeSeries) (result []tsdb.TaggedTimeSeries) {
	if orig == nil {
		return nil
	}
	result = make([]tsdb.TaggedTimeSeries, len(orig))
	copy(result, orig)
	sort.Sort(byTagsType(result))
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

type byTagsType []tsdb.TaggedTimeSeries

func (b byTagsType) Len() int {
	return len(b)
}

func (b byTagsType) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func (b byTagsType) Less(i, j int) bool {
	if b[i].Tags.HostName < b[j].Tags.HostName {
		return true
	}
	if b[i].Tags.HostName > b[j].Tags.HostName {
		return false
	}
	if b[i].Tags.AppName < b[j].Tags.AppName {
		return true
	}
	return false
}

type tagFilter func(s string) bool

func (t tagFilter) Filter(s string) bool {
	return t(s)
}
