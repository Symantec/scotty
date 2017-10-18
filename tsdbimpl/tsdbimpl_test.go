package tsdbimpl_test

import (
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/awsinfo"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/namesandports"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/scotty/tsdbimpl"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"reflect"
	"sort"
	"testing"
	"time"
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

func toMachines(hostNames []string) []mdb.Machine {
	result := make([]mdb.Machine, len(hostNames))
	for i := range result {
		result[i].Hostname = hostNames[i]
	}
	return result
}

func TestAPI(t *testing.T) {
	appStatus := machine.NewEndpointStore(
		newStore(t, "TestAPI", 2, 100, 1.0, 10),
		awsinfo.Config{CloudWatchRefresh: 5 * time.Minute},
		3)
	appStatus.UpdateMachines(
		100.0,
		toMachines([]string{"host1", "host2", "host3", "host4", "host5"}))
	appStatus.UpdateEndpoints(
		100.0,
		map[string]machine.EndpointObservation{
			"host1": {
				SeqNo:     1,
				Endpoints: namesandports.NamesAndPorts{"AnotherApp": {Port: 6997}},
			},
			"host2": {
				SeqNo:     1,
				Endpoints: namesandports.NamesAndPorts{"AnotherApp": {Port: 6997}},
			},
			"host3": {
				SeqNo:     1,
				Endpoints: namesandports.NamesAndPorts{"AnotherApp": {Port: 6997}},
			},
			"host4": {
				SeqNo:     1,
				Endpoints: namesandports.NamesAndPorts{"AnotherApp": {Port: 6997}},
			},
			"host5": {
				SeqNo:     1,
				Endpoints: namesandports.NamesAndPorts{"AnotherApp": {Port: 6997}},
			},
		})
	endpointId, aStore := appStatus.ByHostAndName(
		"host1", application.HealthAgentName)
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 30.0, 500.0, 31.0, 510.0, 32.0, 520.0, 33.0, 530.0, 34.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host1", "AnotherApp")
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 40.0, 500.0, 41.0, 510.0, 42.0, 520.0, 43.0, 530.0, 44.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host2", application.HealthAgentName)
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 50.0, 500.0, 51.0, 510.0, 52.0, 520.0, 53.0, 530.0, 54.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host2", "AnotherApp")
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 60.0, 500.0, 61.0, 510.0, 62.0, 520.0, 63.0, 530.0, 64.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host3", application.HealthAgentName)
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 70.0, 500.0, 71.0, 510.0, 72.0, 520.0, 73.0, 530.0, 74.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host3", "AnotherApp")
	addValues(t, aStore, endpointId.App.EP, "/foo",
		490.0, 80.0, 500.0, 81.0, 510.0, 82.0, 520.0, 83.0, 530.0, 84.0)
	endpointId, aStore = appStatus.ByHostAndName(
		"host5", application.HealthAgentName)
	addValues(t, aStore, endpointId.App.EP, "/bar",
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
		490.0, 590.0,
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
		490.0, 590.0,
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
		490.0, 990.0,
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
		491.0, 529.0,
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
		390.0, 690.0,
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
		690.0, 890.0,
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
		390.0, 690.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					AppName: "AnotherApp",
				},
				Values: tsdb.TimeSeries{
					{500.0, 60.5}, {520.0, 62.5}, {540.0, 64},
				},
			},
			{
				Tags: tsdb.TagSet{
					AppName: application.HealthAgentName,
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
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
		390.0, 690.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
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
					HostName: "host1",
					AppName:  application.HealthAgentName,
				},
				Values: tsdb.TimeSeries{
					{500.0, 30.5}, {520.0, 32.5}, {540.0, 34.0},
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
					HostName: "host2",
					AppName:  application.HealthAgentName,
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
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
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
					AppName:  application.HealthAgentName,
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
		690.0, 890.0,
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
			return s == application.HealthAgentName
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
		390.0, 690.0,
		options); err != nil {
		t.Fatal(err)
	}
	expected = &tsdb.TaggedTimeSeriesSet{
		MetricName: "/foo",
		Data: []tsdb.TaggedTimeSeries{
			{
				Tags: tsdb.TagSet{
					HostName: "host2",
					AppName:  application.HealthAgentName,
				},
				Values: tsdb.TimeSeries{
					{500.0, 50.5}, {520.0, 52.5}, {540.0, 54.0},
				},
			},
			{
				Tags: tsdb.TagSet{
					HostName: "host3",
					AppName:  application.HealthAgentName,
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
			return s == application.HealthAgentName
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
		390.0, 690.0,
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
		390.0, 690.0,
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
