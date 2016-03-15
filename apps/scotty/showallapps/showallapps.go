package showallapps

import (
	"fmt"
	"github.com/Symantec/scotty/datastructs"
	"html/template"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

const (
	htmlTemplateStr = ` \
	<html>
	<body>
	Total apps: {{.TotalApps}}<br>
	Total failed apps: {{.TotalFailedApps}}<br>
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
	    <td>{{.EndpointId.HostName}}</td>
	    <td>{{.EndpointId.Port}}</td>
	    <td>{{.Name}}</td>
	    <td>{{if .Active}}Yes{{else}}&nbsp;{{end}}</td>
	    <td>{{if .Down}}Yes{{else}}&nbsp;{{end}}</td>
	    <td>{{.Status}}</td>
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
	Apps            []*datastructs.ApplicationStatus
	TotalApps       int
	TotalFailedApps int
}

func (v *view) Float32(x float64) float32 {
	return float32(x)
}

func newView(
	apps []*datastructs.ApplicationStatus) *view {
	result := &view{Apps: apps}
	for _, app := range result.Apps {
		if !app.Active {
			continue
		}
		if app.Down {
			result.TotalFailedApps++
		}
		result.TotalApps++
	}
	return result
}

type Handler struct {
	AS *datastructs.ApplicationStatuses
}

type byNameAndPort []*datastructs.ApplicationStatus

func (b byNameAndPort) Len() int { return len(b) }

func (b byNameAndPort) Less(i, j int) bool {
	ihostname := b[i].EndpointId.HostName()
	jhostname := b[j].EndpointId.HostName()
	if ihostname < jhostname {
		return true
	} else if jhostname < ihostname {
		return false
	} else if b[i].EndpointId.Port() < b[j].EndpointId.Port() {
		return true
	}
	return false
}

func (b byNameAndPort) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func sortByNameAndPort(rows []*datastructs.ApplicationStatus) {
	sort.Sort(byNameAndPort(rows))
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	w.Header().Set("Content-Type", "text/html")
	result := h.AS.All()
	sortByNameAndPort(result)
	v := newView(result)
	if err := htmlTemplate.Execute(w, v); err != nil {
		fmt.Fprintln(w, "Error in template: %v\n", err)
	}
}
