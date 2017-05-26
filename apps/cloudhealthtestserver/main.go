// cloudhealthtestserver
//
// This application is a fake cloudhealth endpoint. It accepts requests
// for the *real* cloud health endpoint and writes the data to lmm.
//
// cloudhealthtestserver accepts incoming cloudhealth requests at
// http://localhost:7776/endpoint
//
// For this service to work, create a config file at
// /etc/cloudhealthtestserver/lmm.yaml that describes how to connect to lmm.
// the format of this file looks like:
//
// 	endpoints:
//	- "some-kafka-endpoint-for-lmm.net:9092"
//	topic: "awsMetricTopic"
//	apiKey: "your lmm api key goes here"
//	tenantId: "your lmm tenant id goes here"
// 	clientId: "scotty"
//
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/html"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/scotty/lib/dynconfig"
	"github.com/Symantec/scotty/lib/trimetrics"
	"github.com/Symantec/tricorder/go/tricorder"
	"io"
	"log"
	"net/http"
	"time"
)

var (
	fPort = flag.Int(
		"portNum",
		7776,
		"Port number for cloudhealthtestserver.")
	fConfigDir = flag.String(
		"configDir",
		"/etc/cloudhealthtestserver",
		"Config directory location.")
)

type endpointHandler struct {
	Logger  *log.Logger
	Lmm     *dynconfig.DynConfig
	Metrics *trimetrics.WriterMetrics
}

func (h *endpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dryRun := r.Form.Get("dryrun") != ""
	metrics, err := extractMetricsFromBody(r.Body)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, `{ "error": "%v" }`, err)
		return
	}
	if !dryRun {
		start := time.Now()
		if err := h.Lmm.Get().(*lmmWriterType).Write(metrics); err != nil {
			h.Logger.Println("Error writing to LMM: ", err)
			h.Metrics.LogError(time.Since(start), uint64(len(metrics)), err)
		} else {
			h.Metrics.LogSuccess(time.Since(start), uint64(len(metrics)))
		}

	}
	fmt.Fprintln(w, "{")
	fmt.Fprintf(w, "  succeeded: %d,", len(metrics))
	fmt.Fprintln(w, "  failed: 0,")
	fmt.Fprintln(w, "  errors: 0")
	fmt.Fprintln(w, "}")
}

type htmlWriter interface {
	WriteHtml(writer io.Writer)
}

type splashHandler struct {
	Log htmlWriter
}

func (h *splashHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	writer := bufio.NewWriter(w)
	defer writer.Flush()
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintln(writer, "<html>")
	fmt.Fprintln(writer, "<title>CloudHealthTestServer status page</title>")
	fmt.Fprintln(writer, "<body>")
	fmt.Fprintln(writer, "<center>")
	fmt.Fprintln(writer, "<h1><b>CloudHealthTestServer</b> status page</h1>")
	fmt.Fprintln(writer, "</center>")
	html.WriteHeaderNoGC(writer)
	fmt.Fprintln(writer, "<br>")
	h.Log.WriteHtml(writer)
	fmt.Fprintln(writer, "</body>")
	fmt.Fprintln(writer, "</html>")

}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	circularBuffer := logbuf.New()
	logger := log.New(circularBuffer, "", log.LstdFlags)

	lmm, err := newLmmConfig(*fConfigDir, logger)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle(
		"/",
		&splashHandler{
			Log: circularBuffer,
		})

	metrics, err := trimetrics.NewWriterMetrics("/lmm")
	if err != nil {
		log.Fatal(err)
	}

	http.Handle(
		"/endpoint",
		&endpointHandler{
			Logger:  logger,
			Lmm:     lmm,
			Metrics: metrics,
		})

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}
