package showallapps

import (
	"fmt"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/scotty/lib/httputil"
	"github.com/Symantec/scotty/machine"
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
	  <td>{{.App.EP.HostName}}<br>{{.M.InstanceId}}<br>{{.M.AccountId}}<br>{{if .M.CloudHealth}}CH&nbsp;{{end}}{{if .M.CloudWatchStr}}CW: {{.M.CloudWatchStr}}{{end}}</td>
	    <td>{{.App.Port}}</td>
	    <td><a href="{{$top.Link .}}">{{.App.EP.AppName}}</a></td>
	    <td>{{if .Active}}Yes{{else}}&nbsp;{{end}}</td>
	    \ {{if .App.Down}} \
	      <td>Yes</td>
	      <td>
	        {{.App.Status}}<br>
                {{.App.LastErrorTimeStr}}<br>
		{{.App.LastError}}
              </td>
	    \ {{else}} \
	      <td>No</td>
	      <td>{{.App.Status}} {{.App.EP.ConnectorName}}</td>
	    \ {{end}} \
	    <td>{{if .App.Staleness}}{{.App.Staleness}}{{else}}&nbsp;{{end}}</td>
	    <td>{{with .App.InitialMetricCount}}{{.}}{{else}}&nbsp;{{end}}</td>
	    <td>{{with .App.AverageChangedMetrics}}{{$top.Float32 .}}{{else}}&nbsp;{{end}}</td>
	    <td>{{with .App.PollTime}}{{.}}{{else}}&nbsp;{{end}}</td>
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
	Apps          []*machine.Endpoint
	Summary       EndpointSummary
	History       string
	DefaultCwRate time.Duration
}

func (v *view) Float32(x float64) float32 {
	return float32(x)
}

func (v *view) Link(app *machine.Endpoint) *url.URL {
	return httputil.NewUrl(
		fmt.Sprintf(
			"/api/hosts/%s/%s",
			app.App.EP.HostName(),
			app.App.EP.AppName()),
		"format", "text",
		"history", v.History)
}

type EndpointSummary struct {
	TotalEndpoints       int
	TotalActiveEndpoints int
	TotalFailedEndpoints int
}

func (e *EndpointSummary) Init(endpoints []*machine.Endpoint) {
	e.TotalEndpoints = len(endpoints)
	e.TotalActiveEndpoints = 0
	e.TotalFailedEndpoints = 0
	for _, endpoint := range endpoints {
		if !endpoint.Active() {
			continue
		}
		if endpoint.App.Down {
			e.TotalFailedEndpoints++
		}
		e.TotalActiveEndpoints++
	}
}

type Handler struct {
	ES     *machine.EndpointStore
	Logger log.Logger
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	up := r.Form.Get("up") != ""
	w.Header().Set("Content-Type", "text/html")
	result, _ := h.ES.AllWithStore()
	machine.ByHostAndName(result)
	var summary EndpointSummary
	summary.Init(result)
	if up {
		var toDisplay []*machine.Endpoint
		for _, app := range result {
			if app.Active() && !app.App.Down {
				toDisplay = append(toDisplay, app)
			}
		}
		result = toDisplay
	}
	v := h.newView(summary, result)
	if err := htmlTemplate.Execute(w, v); err != nil {
		fmt.Fprintf(w, "Error in template: %v\n", err)
		h.Logger.Printf("Error in template: %v\n", err)
	}
}

func (h *Handler) newView(
	summary EndpointSummary,
	toDisplay []*machine.Endpoint) *view {
	result := &view{
		Apps:    toDisplay,
		Summary: summary,
		History: "0",
	}
	return result
}
