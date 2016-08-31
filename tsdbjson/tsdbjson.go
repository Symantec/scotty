package tsdbjson

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"strings"
	"time"
)

const (
	kNoVal = -256
)

const (
	kMaxDownSampleBuckets = 1000
)

var (
	kErrTimeRangeTooBig = errors.New(
		"Please use a smaller time range or larger downsample size.")
)

var (
	kEscapes = []string{
		"_00", "_01", "_02", "_03", "_04", "_05", "_06", "_07",
		"_08", "_09", "_0A", "_0B", "_0C", "_0D", "_0E", "_0F",
		"_10", "_11", "_12", "_13", "_14", "_15", "_16", "_17",
		"_18", "_19", "_1A", "_1B", "_1C", "_1D", "_1E", "_1F",
		"_20", "_21", "_22", "_23", "_24", "_25", "_26", "_27",
		"_28", "_29", "_2A", "_2B", "_2C", "-", ".", "/",
		"0", "1", "2", "3", "4", "5", "6", "7",
		"8", "9", "_3A", "_3B", "_3C", "_3D", "_3E", "_3F",
		"_40", "A", "B", "C", "D", "E", "F", "G",
		"H", "I", "J", "K", "L", "M", "N", "O",
		"P", "Q", "R", "S", "T", "U", "V", "W",
		"X", "Y", "Z", "_5B", "_5C", "_5D", "_5E", "_5F",
		"_60", "a", "b", "c", "d", "e", "f", "g",
		"h", "i", "j", "k", "l", "m", "n", "o",
		"p", "q", "r", "s", "t", "u", "v", "w",
		"x", "y", "z", "_7B", "_7C", "_7D", "_7E", "_7F",
		"_80", "_81", "_82", "_83", "_84", "_85", "_86", "_87",
		"_88", "_89", "_8A", "_8B", "_8C", "_8D", "_8E", "_8F",
		"_90", "_91", "_92", "_93", "_94", "_95", "_96", "_97",
		"_98", "_99", "_9A", "_9B", "_9C", "_9D", "_9E", "_9F",
		"_A0", "_A1", "_A2", "_A3", "_A4", "_A5", "_A6", "_A7",
		"_A8", "_A9", "_AA", "_AB", "_AC", "_AD", "_AE", "_AF",
		"_B0", "_B1", "_B2", "_B3", "_B4", "_B5", "_B6", "_B7",
		"_B8", "_B9", "_BA", "_BB", "_BC", "_BD", "_BE", "_BF",
		"_C0", "_C1", "_C2", "_C3", "_C4", "_C5", "_C6", "_C7",
		"_C8", "_C9", "_CA", "_CB", "_CC", "_CD", "_CE", "_CF",
		"_D0", "_D1", "_D2", "_D3", "_D4", "_D5", "_D6", "_D7",
		"_D8", "_D9", "_DA", "_DB", "_DC", "_DD", "_DE", "_DF",
		"_E0", "_E1", "_E2", "_E3", "_E4", "_E5", "_E6", "_E7",
		"_E8", "_E9", "_EA", "_EB", "_EC", "_ED", "_EE", "_EF",
		"_F0", "_F1", "_F2", "_F3", "_F4", "_F5", "_F6", "_F7",
		"_F8", "_F9", "_FA", "_FB", "_FC", "_FD", "_FE", "_FF",
	}
	kHexValues = []int{
		/* 0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 10 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 20 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 30 */ 0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 40 */ kNoVal, 10, 11, 12, 13, 14, 15, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 50 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 60 */ kNoVal, 10, 11, 12, 13, 14, 15, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 70 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 80 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* 90 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* A0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* B0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* C0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* D0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* E0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		/* F0 */ kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
		kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal, kNoVal,
	}
)

func newTimeSeriesSlice(
	taggedTimeSeriesSet *tsdb.TaggedTimeSeriesSet) []TimeSeries {
	result := make([]TimeSeries, len(taggedTimeSeriesSet.Data))
	for i, taggedTimeSeries := range taggedTimeSeriesSet.Data {
		tags := make(map[string]string)
		if taggedTimeSeriesSet.GroupedByHostName {
			tags[HostName] = escape(taggedTimeSeries.Tags.HostName)
		}
		if taggedTimeSeriesSet.GroupedByAppName {
			tags[AppName] = escape(taggedTimeSeries.Tags.AppName)
		}
		aggregateTags := []string{}
		if !taggedTimeSeriesSet.GroupedByHostName {
			aggregateTags = append(aggregateTags, HostName)
		}
		if !taggedTimeSeriesSet.GroupedByAppName {
			aggregateTags = append(aggregateTags, AppName)
		}
		result[i] = TimeSeries{
			Metric:        escape(taggedTimeSeriesSet.MetricName),
			Tags:          tags,
			AggregateTags: aggregateTags,
			Dps:           taggedTimeSeries.Values,
		}
	}
	return result
}

func needsEscaping(orig string) bool {
	origLen := len(orig)
	for i := 0; i < origLen; i++ {
		if kEscapes[orig[i]][0] == '_' {
			return true
		}
	}
	return false
}

func escape(orig string) string {
	if needsEscaping(orig) {
		buffer := &bytes.Buffer{}
		origLen := len(orig)
		for i := 0; i < origLen; i++ {
			buffer.WriteString(kEscapes[orig[i]])
		}
		return buffer.String()
	}
	return orig
}

func unescape(orig string) string {
	if strings.IndexByte(orig, '_') != -1 {
		buffer := &bytes.Buffer{}
		origLen := len(orig)
		idx := 0
		for idx < origLen {
			if orig[idx] == '_' && idx+3 <= origLen {
				ord := kHexValues[orig[idx+1]]*16 + kHexValues[orig[idx+2]]
				if ord >= 0 {
					buffer.WriteByte(byte(ord))
					idx += 3
				} else {
					buffer.WriteByte(orig[idx])
					idx += 1
				}
			} else {
				buffer.WriteByte(orig[idx])
				idx += 1
			}
		}
		return buffer.String()
	}
	return orig
}

func parseDownSample(downSampleStr string) (result *DownSampleSpec, err error) {
	// down sample is optional.
	if downSampleStr == "" {
		return
	}
	components := strings.SplitN(downSampleStr, "-", 3)
	if len(components) < 2 {
		err = ErrBadValue
		return
	}
	dur, err := time.ParseDuration(components[0])
	if err != nil {
		return
	}
	var spec DownSampleSpec
	spec.DurationInSeconds = duration.ToFloat(dur)
	spec.Type = components[1]
	if len(components) == 3 {
		spec.Fill = components[2]
	}
	return &spec, nil
}

func parseQueryRequest(request *QueryRequest) (
	result []ParsedQuery, err error) {
	parsedQueries := make([]ParsedQuery, len(request.Queries))
	endInMillis := request.EndInMillis
	if endInMillis == 0 {
		now := time.Now()
		endInMillis = now.Unix()*1000 + int64(now.Nanosecond())/1000/1000
	}
	if endInMillis < request.StartInMillis {
		endInMillis = request.StartInMillis
	}
	for i := range request.Queries {
		parsedQueries[i].Metric = unescape(request.Queries[i].Metric)
		parsedQueries[i].Aggregator.Type = request.Queries[i].Aggregator
		parsedQueries[i].Aggregator.DownSample, err = parseDownSample(
			request.Queries[i].DownSample)
		if err != nil {
			return
		}
		parsedQueries[i].Start = float64(request.StartInMillis) / 1000.0
		parsedQueries[i].End = float64(endInMillis) / 1000.0
		for _, filter := range request.Queries[i].Filters {
			if filter.Tagk == HostName {
				parsedQueries[i].Options.HostNameFilter = &FilterSpec{
					Type:  filter.Type,
					Value: filter.Filter,
				}
				parsedQueries[i].Options.GroupByHostName = filter.GroupBy
			} else if filter.Tagk == AppName {
				parsedQueries[i].Options.AppNameFilter = &FilterSpec{
					Type:  filter.Type,
					Value: filter.Filter,
				}
				parsedQueries[i].Options.GroupByAppName = filter.GroupBy
			} else {
				err = errors.New(
					fmt.Sprintf("Unrecognised tagk: '%s'", filter.Tagk))
				return
			}
		}
	}
	return parsedQueries, nil
}

func newAggregatorGenerator(
	aggregator string, downSample *DownSampleSpec) (
	tsdb.AggregatorGenerator, error) {
	switch aggregator {
	case "avg":
		return newAverageGenerator(downSample)
	default:
		return nil, ErrUnsupportedAggregator
	}
}

func newAverageGenerator(downSample *DownSampleSpec) (
	tsdb.AggregatorGenerator, error) {
	if downSample == nil {
		return nil, ErrUnsupportedAggregator
	}
	// Defensive copy as caller could change struct later
	duration := downSample.DurationInSeconds
	return func(start, end float64) (tsdb.Aggregator, error) {
		if (end-start)/duration > kMaxDownSampleBuckets {
			return nil, kErrTimeRangeTooBig
		}
		return aggregators.NewAverage(start, end, duration), nil
	}, nil
}

func newTagFilter(filterType, filterValue string) (tsdb.TagFilter, error) {
	switch filterType {
	case LiteralOr:
		return newLiteralOr(filterValue)
	default:
		return nil, ErrUnsupportedFilter
	}
}

func newLiteralOr(filterValue string) (tsdb.TagFilter, error) {
	tagValues := strings.Split(filterValue, "|")
	tagMap := make(map[string]bool, len(tagValues))
	for _, tagValue := range tagValues {
		tagMap[unescape(tagValue)] = true
	}
	return func(s string) bool {
		return tagMap[s]
	}, nil
}
