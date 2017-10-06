package tsdbimpl

import (
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/tsdb"
)

func (o *QueryOptions) isIncluded(hostName, appName string) bool {
	if o.HostNameFilter != nil && !o.HostNameFilter.Filter(hostName) {
		return false
	}
	if o.AppNameFilter != nil && !o.AppNameFilter.Filter(appName) {
		return false
	}
	return true
}

func query(
	endpoints *machine.EndpointStore,
	metricName string,
	aggregatorGen tsdb.AggregatorGenerator,
	start, end float64,
	options *QueryOptions) (result *tsdb.TaggedTimeSeriesSet, err error) {
	if options == nil {
		options = &QueryOptions{}
	}
	apps, store := endpoints.AllWithStore()
	var taggedTimeSeriesSlice []tsdb.TaggedTimeSeries
	var metricNameFound bool
	if options.GroupByHostName && options.GroupByAppName {
		for i := range apps {
			if options.isIncluded(apps[i].App.EP.HostName(), apps[i].App.EP.AppName()) {
				timeSeries, earliest, ok := store.TsdbTimeSeries(
					metricName,
					apps[i].App.EP,
					start,
					end)
				if ok {
					metricNameFound = true
					var aggregator tsdb.Aggregator
					aggregator, err = aggregatorGen(start, end)
					if err != nil {
						return
					}
					aggregator.Add(timeSeries)
					aggregatedTimeSeries := aggregator.Aggregate().EarlyTruncate(earliest)
					if len(aggregatedTimeSeries) != 0 {
						taggedTimeSeriesSlice = append(
							taggedTimeSeriesSlice, tsdb.TaggedTimeSeries{
								Tags: tsdb.TagSet{
									HostName: apps[i].App.EP.HostName(),
									AppName:  apps[i].App.EP.AppName(),
								},
								Values: aggregatedTimeSeries,
							})
					}
				}
			}
		}
	} else {
		aggregatorMap := make(map[tsdb.TagSet]tsdb.Aggregator)
		earliestMap := make(map[tsdb.TagSet]float64)
		for i := range apps {
			if options.isIncluded(apps[i].App.EP.HostName(), apps[i].App.EP.AppName()) {
				timeSeries, earliest, ok := store.TsdbTimeSeries(
					metricName,
					apps[i].App.EP,
					start,
					end)
				if ok {
					metricNameFound = true
					var tagSet tsdb.TagSet
					if options.GroupByHostName {
						tagSet.HostName = apps[i].App.EP.HostName()
					}
					if options.GroupByAppName {
						tagSet.AppName = apps[i].App.EP.AppName()
					}
					aggregator := aggregatorMap[tagSet]
					if aggregator == nil {
						aggregator, err = aggregatorGen(start, end)
						if err != nil {
							return
						}
						aggregatorMap[tagSet] = aggregator
					}
					aggregator.Add(timeSeries)
					if earliest > earliestMap[tagSet] {
						earliestMap[tagSet] = earliest
					}
				}
			}
		}
		if metricNameFound {
			for k, v := range aggregatorMap {
				aggregatedTimeSeries := v.Aggregate().EarlyTruncate(earliestMap[k])
				if len(aggregatedTimeSeries) != 0 {
					taggedTimeSeriesSlice = append(
						taggedTimeSeriesSlice, tsdb.TaggedTimeSeries{
							Tags:   k,
							Values: aggregatedTimeSeries,
						})
				}
			}
		}
	}
	if metricNameFound {
		return &tsdb.TaggedTimeSeriesSet{
			MetricName:        metricName,
			Data:              taggedTimeSeriesSlice,
			GroupedByHostName: options.GroupByHostName,
			GroupedByAppName:  options.GroupByAppName,
		}, nil
	}
	return nil, ErrNoSuchMetric
}
