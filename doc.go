/*
Package scotty collects endpoint health metrics asynchronously. Although the
GO API in this package is subject to change at anytime, the REST API that
the scotty application provides will be stable.

Fetching metrics from scotty using REST API

REST urls (always use GET)

	http://scottyhostname.com/api/hosts/someHostName/someAppName
		Returns every metric from someAppName on someHostName
		along with values for the last hour.
	http://scottyhostname.com/api/hosts/someHostName/someAppName/a/path
		Returns every metric under /a/path from someAppName on
		someHostName along with values for the last hour.
		If no metrics match, returns an empty list.
	http://scottyhostname.com/api/errors
		Returns a list of every endpoint that scotty cannot currently
		reach along with the timestamp of the last attempt and the
		error encountered. If no errors, returns an empty list.

Global optional REST Query Parameters

	format=text
		Adds indentation and line feeds to JSON to make it human
		readable.

Optional REST Query parameters for /api/hosts calls.

	singleton=true
		For the second api/hosts URL listed, returned metrics
		must match the specified metric path exactly. Use if you
		want a particular metric rather than all the metrics under
		a particular path.
	history=123
		Shows values for the last 123 minutes. history=0 shows only
		the most recent values. If omitted, default is 60 minutes.

Sample JSON for api/hosts calls.

	[
		{
			"path": "/proc/args",
			"description": "Program args",
			"unit": "None",
			"kind": "string",
			"values": [
				{
					"timestamp": "1450810183.939569234",
					"value": "-r -t -n"
				}
			]
		},
		{
			"path": "/proc/cpu/sys",
			"description": "User CPU time used",
			"unit": "Seconds",
			"kind": "duration",
			"values": [
				{
					"timestamp": "1450812042.050224065",
					"value": "579.000432200"
				},
				{
					"timestamp": "1450811980.899730682",
					"value": "579.000167902"
				},
				{
					"timestamp": "1450811923.538924217",
					"value": "579.000008265"
				}
			}
		}
	]

Sample JSON for api/error call

	[
		{
			"hostName": "b-imserv-r07e14-prod.ash1.symcpe.net*1037",
			"timestamp": "1450812819.791744615",
			"error": "dial tcp 10.119.150.73:7776: getsockopt: connection refused"
		},
		{
			"hostName": "b-imserv-r07e14-prod.ash1.symcpe.net*2037",
			"timestamp": "1450812819.807774730",
			"error": "dial tcp 10.119.150.73:7776: getsockopt: connection refused"
		}
	]

For more information on the json schema, see the scotty/messages package
*/
package scotty
