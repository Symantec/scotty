package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/logbuf"
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/Dominator/lib/mdb/mdbd"
	collector "github.com/Symantec/scotty"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/apps/scotty/splash"
	"github.com/Symantec/scotty/consul"
	"github.com/Symantec/scotty/datastructs"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/scotty/tsdbexec"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	fPort = flag.Int(
		"portNum",
		6980,
		"Port number for scotty.")
	fBytesPerPage = flag.Uint(
		"bytesPerPage",
		1024,
		"Space for new metrics for each endpoint in records")
	fPageCount = flag.Uint(
		"pageCount",
		30*1000*1000,
		"Total page count")
	fMdbFile = flag.String(
		"mdbFile",
		"/var/lib/scotty/mdb",
		"Name of file from which to read mdb data.")
	fCollectionFrequency = flag.Duration(
		"collectionFrequency",
		30*time.Second,
		"Amount of time between metric collections")
	fPidFile = flag.String(
		"pidfile", "", "Name of file to write my PID to")
	fThreshhold = flag.Float64(
		"inactiveThreshhold", 0.1, "Ratio of inactive pages needed to begin purging inactive pages")
	fDegree = flag.Uint(
		"degree", 10, "Degree of btree")
	fConfigDir = flag.String(
		"configDir", "/etc/scotty", "Directory for scotty config files.")
	fCoord = flag.String(
		"coordinator", "", "Leadership election specifications")
)

type gzipResponseWriter struct {
	http.ResponseWriter
	W io.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.W.Write(b)
}

type gzipHandler struct {
	H http.Handler
}

func (h gzipHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		h.H.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	defer gz.Close()
	gzr := &gzipResponseWriter{ResponseWriter: w, W: gz}
	h.H.ServeHTTP(gzr, r)
}

func createApplicationList() *datastructs.ApplicationList {
	f, err := os.Open(path.Join(*fConfigDir, "apps.yaml"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	builder := datastructs.NewApplicationListBuilder()
	if err := builder.ReadConfig(f); err != nil {
		log.Fatal(err)
	}
	return builder.Build()
}

func hostNames(machines []mdb.Machine) (result []string) {
	result = make([]string, len(machines))
	for i := range machines {
		result[i] = machines[i].Hostname
	}
	return
}

func createApplicationStats(
	appList *datastructs.ApplicationList,
	logger *log.Logger,
	tagvAdder suggest.Adder,
	maybeNilMemoryManager *memoryManagerType) *datastructs.ApplicationStatuses {
	var astore *store.Store
	fmt.Println("Initialization started.")
	if maybeNilMemoryManager != nil {
		memoryManager := maybeNilMemoryManager
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage,
			1,
			*fThreshhold,
			*fDegree)
		astore.SetExpanding(true)
		memoryManager.SetMemory(astore)
		if err := memoryManager.RegisterMetrics(); err != nil {
			log.Fatal(err)
		}
	} else {
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage, *fPageCount, *fThreshhold, *fDegree)
	}
	dirSpec, err := tricorder.RegisterDirectory("/store")
	if err != nil {
		log.Fatal(err)
	}
	if err := astore.RegisterMetrics(dirSpec); err != nil {
		log.Fatal(err)
	}
	stats := datastructs.NewApplicationStatuses(appList, astore)
	mdbChannel := mdbd.StartMdbDaemon(*fMdbFile, logger)
	machines := <-mdbChannel
	theHostNames := hostNames(machines.Machines)
	for _, aName := range theHostNames {
		tagvAdder.Add(aName)
	}
	stats.MarkHostsActiveExclusively(
		duration.TimeToFloat(time.Now()), theHostNames)
	fmt.Println("Initialization complete.")
	// Endpoint refresher goroutine
	go func() {
		for {
			machines := <-mdbChannel
			stats.MarkHostsActiveExclusively(
				duration.TimeToFloat(time.Now()),
				hostNames(machines.Machines))
		}
	}()
	return stats
}

func gracefulCleanup() {
	os.Remove(*fPidFile)
	os.Exit(1)
}

func writePidfile() {
	file, err := os.Create(*fPidFile)
	if err != nil {
		return
	}
	defer file.Close()
	fmt.Fprintln(file, os.Getpid())
}

func handleSignals(logger *log.Logger) {
	if *fPidFile == "" {
		return
	}
	sigtermChannel := make(chan os.Signal)
	signal.Notify(sigtermChannel, syscall.SIGTERM, syscall.SIGINT)
	writePidfile()
	go func() {
		for {
			select {
			case <-sigtermChannel:
				gracefulCleanup()
			}
		}
	}()
}

type tsdbAdderType struct {
	wrapped suggest.Adder
}

func (a *tsdbAdderType) Add(s string) {
	a.wrapped.Add(tsdbjson.Escape(s))
}

func newTsdbAdder(adder suggest.Adder) suggest.Adder {
	return &tsdbAdderType{wrapped: adder}
}

type blockingCoordinatorType struct {
	listener func(blocked bool)
}

func (b *blockingCoordinatorType) Lease(float64, float64) (
	float64, float64) {
	if b.listener != nil {
		b.listener(true)
	}
	select {}
}

func (b *blockingCoordinatorType) WithStateListener(
	listener func(blocked bool)) store.Coordinator {
	result := *b
	result.listener = listener
	return &result
}

type totalCountCollectionType []*totalCountType

func (t totalCountCollectionType) Update(store *store.Store, e *collector.Endpoint) {
	for _, count := range t {
		count.Update(store, e)
	}
}

type maybeNilMemoryManagerWrapperType struct {
	wrapped *memoryManagerType
}

func (m *maybeNilMemoryManagerWrapperType) Check() {
	if m.wrapped != nil {
		m.wrapped.Check()
	}
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	circularBuffer := logbuf.New()
	logger := log.New(circularBuffer, "", log.LstdFlags)
	handleSignals(logger)
	// Read configs early so that we will fail fast.
	maybeNilMemoryManager := maybeCreateMemoryManager(logger)
	consumerBuilders, err := newPStoreConsumers(maybeNilMemoryManager)
	if err != nil {
		log.Println("Pstore config file error:", err)
		logger.Println("Pstore config file error:", err)
	}
	metricNameEngine := suggest.NewEngine()
	metricNameAdder := newTsdbAdder(metricNameEngine)
	tagkEngine := suggest.NewSuggester("appname", "HostName")
	tagvEngine := suggest.NewEngine()
	tagvAdder := newTsdbAdder(tagvEngine)

	appList := createApplicationList()
	for _, app := range appList.All() {
		tagvAdder.Add(app.Name())
	}
	applicationStats := createApplicationStats(
		appList, logger, tagvAdder, maybeNilMemoryManager)
	rpc.RegisterName(
		"Scotty",
		&rpcType{AS: applicationStats},
	)
	rpc.HandleHTTP()
	connectionErrors := newConnectionErrorsType()
	if consumerBuilders == nil {
		startCollector(
			applicationStats,
			connectionErrors,
			totalCountCollectionType(nil),
			metricNameAdder,
			&maybeNilMemoryManagerWrapperType{maybeNilMemoryManager})
	} else {
		var coord coordinatorBuilderType
		if *fCoord != "" {
			var err error
			coord, err = consul.NewCoordinator(logger)
			if err != nil {
				logger.Println(err)
				coord = &blockingCoordinatorType{}
			}
		}
		totalCounts := startPStoreLoops(
			applicationStats,
			consumerBuilders,
			logger,
			coord)
		startCollector(
			applicationStats,
			connectionErrors,
			totalCountCollectionType(totalCounts),
			metricNameAdder,
			&maybeNilMemoryManagerWrapperType{maybeNilMemoryManager})
	}

	http.Handle(
		"/",
		gzipHandler{&splash.Handler{
			AS:  applicationStats,
			Log: circularBuffer,
		}})
	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			AS:             applicationStats,
			CollectionFreq: *fCollectionFrequency,
		}})
	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{&byEndpointHandler{
				AS: applicationStats,
			}}))
	http.Handle(
		"/api/latest/",
		http.StripPrefix(
			"/api/latest/",
			gzipHandler{&latestHandler{
				AS: applicationStats,
			}}))

	http.Handle(
		"/api/errors/",
		gzipHandler{&errorHandler{
			ConnectionErrors: connectionErrors,
		}},
	)

	tsdbServeMux := http.NewServeMux()

	tsdbServeMux.Handle(
		"/api/query",
		tsdbexec.NewHandler(
			func(r *tsdbjson.QueryRequest) ([]tsdbjson.TimeSeries, error) {
				return tsdbexec.Query(
					r, applicationStats, *fCollectionFrequency)
			}))
	tsdbServeMux.Handle(
		"/api/suggest",
		tsdbexec.NewHandler(
			func(req url.Values) ([]string, error) {
				return tsdbexec.Suggest(
					req,
					map[string]suggest.Suggester{
						"metrics": metricNameEngine,
						"tagk":    tagkEngine,
						"tagv":    tagvEngine,
					})
			},
		))
	tsdbServeMux.Handle(
		"/api/aggregators",
		tsdbexec.NewHandler(
			func(req url.Values) ([]string, error) {
				return aggregators.Names(), nil
			},
		))
	tsdbServeMux.Handle(
		"/api/version",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"version": "1.0",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api/config",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"tsd.ore.auto_create_metrics": "true",
					"tsd.ore.auto_create_tagks":   "true",
					"tsd.ore.auto_create_tagvs":   "true",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api/config/filters",
		tsdbexec.NewHandler(
			func(req url.Values) (interface{}, error) {
				return tsdbjson.AllFilterDescriptions(), nil
			},
		))
	tsdbServeMux.Handle(
		"/api/dropcaches",
		tsdbexec.NewHandler(
			func(req url.Values) (map[string]string, error) {
				return map[string]string{
					"message": "Caches dropped",
					"status":  "200",
				}, nil
			},
		))
	tsdbServeMux.Handle(
		"/api",
		tsdbexec.NotFoundHandler,
	)

	go func() {
		if err := http.ListenAndServe(":4242", tsdbServeMux); err != nil {
			log.Fatal(err)
		}
	}()

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		log.Fatal(err)
	}
}
