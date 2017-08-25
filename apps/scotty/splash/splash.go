package splash

import (
	"bufio"
	"fmt"
	"github.com/Symantec/Dominator/lib/html"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/machine"
	"html/template"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	htmlTemplateStr = ` \
	<a href="/showAllApps">Applications</a><br>
	Total Endpoints: {{.Summary.TotalEndpoints}}<br>
	Total Active Endpoints: {{.Summary.TotalActiveEndpoints}}<br>
	Total Failed Endpoints: {{.Summary.TotalFailedEndpoints}}<br>
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
	Summary showallapps.EndpointSummary
}

func newView(
	apps []*machine.Endpoint) *view {
	var result = &view{}
	result.Summary.Init(apps)
	return result
}

type HtmlWriter interface {
	WriteHtml(writer io.Writer)
}

type Handler struct {
	ES  *machine.EndpointStore
	Log HtmlWriter
}

func (h *Handler) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	writer := bufio.NewWriter(w)
	defer writer.Flush()
	w.Header().Set("Content-Type", "text/html")
	result, _ := h.ES.AllWithStore()
	fmt.Fprintln(writer, "<html>")
	fmt.Fprintln(writer, "<title>Scotty status page</title>")
	fmt.Fprintln(writer, "<body>")
	fmt.Fprintln(writer, "<center>")
	fmt.Fprintln(writer, "<h1><b>Scotty</b> status page</h1>")
	fmt.Fprintln(writer, "</center>")
	html.WriteHeaderNoGC(writer)
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
