# How to add an OpenTSDB filter to scotty

## Background

Scotty supports a small subset of the OpenTSDB API to power grafana
displays. OpenTSDB API allows for implementations to provide filters
to filter time series by tag values when collecting data.
Common examples of such filters are literal_or and wildcard.

This document describes how to add additional OpenTSDB filters to
scotty.

## This package

This package is responsible for translating JSON OpenTSDB requests. It
is in this package that the filter logic lives.

## Writing your own filter

This section explains the mechanics of writing your own filter.

### Register your filter

Edit the tsdbjson.go file in this package by adding your own filter to
the map in the var block around line 33. Typically, your declaration will
look like this:

	var (
		kTagFiltersByName = map[string]*tagFilterInfoType {
			...
			"literal_or": {
				// filter creating function
				New: newLiteralOr,
				Description: &FilterDescription{
					// shown in grafana display
					Examples: "host=literal_or(web01|web02)",
					// shown in grafana display
					Description: "Matches if value is one of listed values",
				},
			},
			...
		}
	)

This step registers your filter with scotty so that it shows up
in the combo boxes when connecting to scotty with grafana.

### Implement your filter

At the bottom of the tsdbjson.go file, write the filter creating
function you used when registering your filter. In the above example,
the filter creating function is "newLiteralOr."

Filter creating functions take one string parameter: The filter
criteria user entered in the grafana display and produces the
corresponding tsdb.Filter value. Filter creating functions should
return an error if the filter criteria the user entered cannot
be parsed.

See the newLiteralOr function at the bottom of tsdbjson.go for an
example.

## Testing your work

To test your work, add a test to the tsdbjson_test.go file. The
existing test, TestTagFilter, already tests the literal_or filter.
Use it as an example.

## Example of adding an openTSDB filter.

No good commits to show just yet.
