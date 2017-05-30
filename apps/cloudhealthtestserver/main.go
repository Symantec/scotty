// cloudhealthtestserver
//
// This application is a fake cloudhealth endpoint. It accepts requests
// for the *real* cloud health endpoint and writes the data to lmm. Finally,
// it forwards requests onto the *real* cloud health endpoint.
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
	"bytes"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/html"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/scotty/cloudhealth"
	"github.com/Symantec/scotty/lib/dynconfig"
	"github.com/Symantec/scotty/lib/trimetrics"
	"github.com/Symantec/tricorder/go/tricorder"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
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

var (
	kReverseProxy = newReverseProxy(cloudhealth.DefaultEndpoint)
)

func newReverseProxy(URL string) *httputil.ReverseProxy {
	pURL, err := url.Parse(URL)
	if err != nil {
		log.Fatal(err)
	}
	return httputil.NewSingleHostReverseProxy(pURL)
}

type endpointHandler struct {
	Logger  *log.Logger
	Lmm     *dynconfig.DynConfig
	Metrics *trimetrics.WriterMetrics
}

func extractAsBytes(r io.Reader) []byte {
	var buffer bytes.Buffer
	buffer.ReadFrom(r)
	return buffer.Bytes()
}

func (h *endpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dryRun := r.Form.Get("dryrun") != ""

	// Ultimately, we have to proxy this request onto the real cloudhealth
	// service, but when we read the request body we exhaust it so that it
	// won't get passed on as we want. To get around this, we extract the
	// request body as a slice of bytes and then create a new request body
	// off that slice of bytes.
	bodyAsBytes := extractAsBytes(r.Body)

	// Close the original body as our transport layer won't be able to do
	// this for us.
	r.Body.Close()

	// Now set the body to a byte buffer of the original body so that it
	// can get read again.
	r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyAsBytes))

	// Here we have to promote our content to a stream to call this function
	metrics, err := extractMetricsFromBody(bytes.NewBuffer(bodyAsBytes))
	if err != nil {
		h.Logger.Println(err)
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

	// Because of how the reverse proxy works, we have to make sure the
	// path of the request we send to the proxy is empty without changing
	// the original request

	// Make defensive copy to prevent changing original request
	newReq := *r
	{
		// URL is a pointer field so we have to make another defensive copy
		newUrl := *r.URL
		newReq.URL = &newUrl
	}
	// zero out the path
	newReq.URL.Path = ""
	kReverseProxy.ServeHTTP(w, &newReq)
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
