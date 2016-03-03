package splash

import (
	"bufio"
	"fmt"
	"github.com/Symantec/Dominator/lib/html"
	"github.com/Symantec/scotty/datastructs"
	"html/template"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	htmlTemplateStr = ` \
	<a href="/showAllApps">Applications</a><br>
	Total apps: {{.TotalApps}}<br>
	Total failed apps: {{.TotalFailedApps}}<br>
	  `
)

var (
	leadingWhitespace = regexp.MustCompile(`\n\s*\\ `)
	htmlTemplate      = template.Must(
		template.New("splash").Parse(
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
	TotalApps       int
	TotalFailedApps int
}

func newView(
	apps []*datastructs.ApplicationStatus) *view {
	result := &view{}
	for _, app := range apps {
		if app.Down {
			result.TotalFailedApps++
		}
		result.TotalApps++
	}
	return result
}

type HtmlWriter interface {
	WriteHtml(writer io.Writer)
}

type Handler struct {
	HPS *datastructs.HostsPortsAndStore
	AS  *datastructs.ApplicationStatuses
	Log HtmlWriter
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	writer := bufio.NewWriter(w)
	defer writer.Flush()
	w.Header().Set("Content-Type", "text/html")
	_, hostsAndPorts := h.HPS.Get()
	result := h.AS.GetAll(hostsAndPorts)
	fmt.Fprintln(writer, "<html>")
	fmt.Fprintln(writer, "<title>Scotty status page</title>")
	fmt.Fprintln(writer, "<body>")
	fmt.Fprintln(writer, "<center>")
	fmt.Fprintln(writer, "<h1><b>Scotty</b> status page</h1>")
	fmt.Fprintln(writer, "</center>")
	html.WriteHeader(writer)
	fmt.Fprintln(writer, "<br>")
	v := newView(result)
	if err := htmlTemplate.Execute(writer, v); err != nil {
		fmt.Fprintln(writer, "Error in template: %v\n", err)
		fmt.Fprintln(writer, "</body>")
		fmt.Fprintln(writer, "</html>")
	}
	h.Log.WriteHtml(writer)
	fmt.Fprintln(writer, "</body>")
	fmt.Fprintln(writer, "</html>")
}
