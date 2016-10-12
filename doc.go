/*
Package scotty collects endpoint health metrics asynchronously. The scotty
code and the REST API that the scotty application provides are subject to
change.

Fetching metrics from scotty using REST API

REST urls (always use GET)

	http://scottyhostname.com:6980/api/hosts/someHostName/someAppName
		Returns every metric from someAppName on someHostName
		along with values for the last hour.
	http://scottyhostname.com:6980/api/hosts/someHostName/someAppName/a/path
		Returns every metric under /a/path from someAppName on
		someHostName along with values for the last hour.
		If no metrics match, returns an empty list.
	http://scottyhostname.com:6980/api/latest/a/path
		Returns the latest values of all metrics under /a/path on all
		endpoints sorted by hostname first, appname second, and path third.
	http://scottyhostname.com:6980/api/errors
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
					"value": "-r -t -n",
					"active": true
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
					"active": true
				},
				{
					"timestamp": "1450811980.899730682",
					"value": "579.000167902"
					"active": true
				},
				{
					"timestamp": "1450811923.538924217",
					"value": "579.000008265"
					"active": true
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

Sample JSON for api/latest call

	[
		{
			"hostName": "first.net",
			"appName": "My App",
			"path": "/proc/args",
			"description": "Program args",
			"unit": "None",
			"kind": "string",
			"timestamp": "1476307345.592506885",
			"value": "-f --size=1000"
		},
		{
			"hostName": "second.net",
			"appName": "My App",
			"path": "/proc/args",
			"description": "Program args",
			"unit": "None",
			"kind": "string",
			"timestamp": "1476307345.554426431",
			"value": "--size=2000"
		},

		...
	]

For more information on the json schema, see the scotty/messages package

Scotty GO RPC

Scotty GO RPC is available on the same port as the REST API, usually 6980.
You can see these methods by visiting http://scottyhostname.com:6980/debug/rpc

Scotty.Latest

Gets the latest available metrics under a given path for all active endpoints.
Pass the absolute path as the input. Empty string ("") means get all the
latest available metrics. "/foo" means get the metric "/foo" from each
endpoint or any metric underneath "/foo" from each endpoint.
The output are the latest metrics as []*messages.LatestMetric.
The output are sorted first by hostname then by application name and then by
metric path.

GO RPC scotty.Latest Example code:

	import "fmt"
	import "github.com/Symantec/scotty/messages"
	import "log"
	import "net/rpc"

	client, _ := rpc.DialHTTP("tcp", "a.hostname.net:6980")
	defer client.Close()
	var latest []*messages.LatestMetric
	if err := client.Call("Scotty.Latest", "/path/to/metrics", &latest); err != nil {
		log.Fatal(err)
	}
	for _, m := range latest {
		fmt.Println("Path: ", m.Path)
		fmt.Println("Timestamp: ", m.Timestamp)
		fmt.Println("Value: ", m.Value)
	}
*/
package scotty
