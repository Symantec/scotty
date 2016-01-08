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
	<table border="1" style="width:100%">
	  <tr>
	    <th>Machine</th>
	    <th>Port</th>
	    <th>Status</th>
	    <th>Staleness</th>
	    <th>Poll</th>
          </tr>
	\ {{range .Apps}} \
	  <tr>
	    <td>{{.MachineId.HostName}}</td>
	    <td>{{.MachineId.Port}}</td>
	    <td>{{.Status}}</td>
	    <td>{{if .Staleness}}{{.Staleness}}{{else}}&nbsp;{{end}}</td>
	    <td>{{if .PollTime}}{{.PollTime}}{{else}}&nbsp;{{end}}</td>
	  </tr>
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
	Apps []*datastructs.ApplicationStatus
}

type Handler struct {
	HPS *datastructs.HostsPortsAndStore
	AS  *datastructs.ApplicationStatuses
}

type byNameAndPort []*datastructs.ApplicationStatus

func (b byNameAndPort) Len() int { return len(b) }

func (b byNameAndPort) Less(i, j int) bool {
	ihostname := b[i].MachineId.HostName()
	jhostname := b[j].MachineId.HostName()
	if ihostname < jhostname {
		return true
	} else if jhostname < ihostname {
		return false
	} else if b[i].MachineId.Port() < b[j].MachineId.Port() {
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
	_, hostsAndPorts := h.HPS.Get()
	result := h.AS.GetAll(hostsAndPorts)
	sortByNameAndPort(result)
	v := &view{Apps: result}
	if err := htmlTemplate.Execute(w, v); err != nil {
		fmt.Fprintln(w, "Error in template: %v\n", err)
	}
}
