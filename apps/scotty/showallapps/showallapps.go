package showallapps

import (
	"fmt"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/lib/httputil"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	htmlTemplateStr = ` \
	<html>
	<body>
	Total Endpoints: {{.Summary.TotalEndpoints}}<br>
	Total Active Endpoints: {{.Summary.TotalActiveEndpoints}}<br>
	Total Failed Endpoints: {{.Summary.TotalFailedEndpoints}}<br>
	<a href="/showAllApps?up=true">Up only</a>&nbsp;<a href="/showAllApps">All</a>
	<table border="1" style="width:100%">
	  <tr>
	    <th>Machine</th>
	    <th>Port</th>
	    <th>Name</th>
	    <th>Active?</th>
	    <th>Down?</th>
	    <th>Status</th>
	    <th>Staleness</th>
	    <th>Init Metric Count</th>
	    <th>Avg. changing metrics</th>
	    <th>Poll</th>
          </tr>
	\ {{with $top := .}} \
	\ {{range .Apps}} \
	  <tr>
	    <td>{{.EndpointId.HostName}}<br>{{.InstanceId}}<br>{{.AccountNumber}}{{if .CloudWatch}}<br>{{.CloudWatch}}{{end}}</td>
	    <td>{{.EndpointId.Port}}</td>
	    <td><a href="{{$top.Link .}}">{{.Name}}</a></td>
	    <td>{{if .Active}}Yes{{else}}&nbsp;{{end}}</td>
	    \ {{if .Down}} \
	      <td>Yes</td>
	      <td>
	        {{.Status}}<br>
                {{.LastErrorTimeStr}}<br>
		{{.LastError}}
              </td>
	    \ {{else}} \
	      <td>No</td>
	      <td>{{.Status}} {{.EndpointId.ConnectorName}}</td>
	    \ {{end}} \
	    <td>{{if .Staleness}}{{.Staleness}}{{else}}&nbsp;{{end}}</td>
	    <td>{{with .InitialMetricCount}}{{.}}{{else}}&nbsp;{{end}}</td>
	    <td>{{with .AverageChangedMetrics}}{{$top.Float32 .}}{{else}}&nbsp;{{end}}</td>
	    <td>{{if .PollTime}}{{.PollTime}}{{else}}&nbsp;{{end}}</td>
	  </tr>
	\ {{end}} \
	\ {{end}} \
	</table>
	</body>
	</html>
	  `
)

var (
	leadingWhitespace = regexp.MustCompile(`\n\s*\\ `)
	htmlTemplate      = template.Must(
		template.New("showAllApps").Parse(
			strings.Replace(
				leadingWhitespace.ReplaceAllString(
					strings.Replace(
						htmlTemplateStr,
						"\n\t",
						"\n",
						-1),
					"\n"),
				" \\\n",
				"",
				-1)))
)

type view struct {
	Apps    []*datastructs.ApplicationStatus
	Summary EndpointSummary
	History string
}

func (v *view) Float32(x float64) float32 {
	return float32(x)
}

func (v *view) Link(app *datastructs.ApplicationStatus) *url.URL {
	return httputil.NewUrl(
		fmt.Sprintf(
			"/api/hosts/%s/%s",
			app.EndpointId.HostName(),
			app.Name),
		"format", "text",
		"history", v.History)
}

type EndpointSummary struct {
	TotalEndpoints       int
	TotalActiveEndpoints int
	TotalFailedEndpoints int
}

func (e *EndpointSummary) Init(apps []*datastructs.ApplicationStatus) {
	e.TotalEndpoints = len(apps)
	e.TotalActiveEndpoints = 0
	e.TotalFailedEndpoints = 0
	for _, app := range apps {
		if !app.Active {
			continue
		}
		if app.Down {
			e.TotalFailedEndpoints++
		}
		e.TotalActiveEndpoints++
	}
}

type Handler struct {
	AS             *datastructs.ApplicationStatuses
	CollectionFreq time.Duration
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	up := r.Form.Get("up") != ""
	w.Header().Set("Content-Type", "text/html")
	result := h.AS.All()
	datastructs.ByHostAndName(result)
	var summary EndpointSummary
	summary.Init(result)
	if up {
		var toDisplay []*datastructs.ApplicationStatus
		for _, app := range result {
			if app.Active && !app.Down {
				toDisplay = append(toDisplay, app)
			}
		}
		result = toDisplay
	}
	v := h.newView(summary, result)
	if err := htmlTemplate.Execute(w, v); err != nil {
		fmt.Fprintln(w, "Error in template: %v\n", err)
	}
}

func (h *Handler) newView(
	summary EndpointSummary,
	toDisplay []*datastructs.ApplicationStatus) *view {
	result := &view{
		Apps:    toDisplay,
		Summary: summary,
		History: "0",
	}
	return result
}
