package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/Dominator/lib/log/serverlogger"
	"github.com/Symantec/Dominator/lib/mdb"
	"github.com/Symantec/Dominator/lib/mdb/mdbd"
	"github.com/Symantec/scotty/application"
	"github.com/Symantec/scotty/apps/scotty/showallapps"
	"github.com/Symantec/scotty/apps/scotty/splash"
	"github.com/Symantec/scotty/awsinfo"
	"github.com/Symantec/scotty/consul"
	"github.com/Symantec/scotty/influx/qlutils"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/Symantec/scotty/lib/apiutil"
	"github.com/Symantec/scotty/machine"
	"github.com/Symantec/scotty/store"
	"github.com/Symantec/scotty/suggest"
	"github.com/Symantec/scotty/tsdb/aggregators"
	"github.com/Symantec/scotty/tsdbexec"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/Symantec/tricorder/go/healthserver"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/duration"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/uuid"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	kInfluxEpochConversions = map[string]func(int64) int64{
		"h":  func(ts int64) int64 { return ts / 3600.0 },
		"m":  func(ts int64) int64 { return ts / 60.0 },
		"s":  func(ts int64) int64 { return ts },
		"ms": func(ts int64) int64 { return ts * 1000.0 },
		"u":  func(ts int64) int64 { return ts * 1000.0 * 1000.0 },
		"ns": func(ts int64) int64 { return ts * 1000.0 * 1000.0 * 1000.0 },
	}
)

const (
	kHostNotFoundMsg = "The host on which this instance of scotty is running is not in mdb."
)

var (
	fMdbLoadTesting = flag.Int(
		"mdbLoadTesting",
		0,
		"Number of hosts to use for load testing")
	fPort = flag.Uint(
		"portNum",
		6980,
		"Port number for scotty.")
	fName = flag.String(
		"name",
		"scotty",
		"Name of application")
	fInfluxPort = flag.Int(
		"influxPortNum",
		8086,
		"Influx Port number for scotty.")
	fTsdbPort = flag.Int(
		"tsdbPortNum",
		4242,
		"OpenTSDB Port number for scotty.")
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
	fCloudWatchFreq = flag.Duration(
		"cloudWatchFreq",
		5*time.Minute,
		"Rollup time for cloudwatch")
	fCloudHealthTest = flag.Bool(
		"cloudHealthTest", false, "Whether or not this is testing cloudhealth")
	fCloudWatchTest = flag.Bool(
		"cloudWatchTest", false, "Whether or not this is testing cloudwatch")
)

type stringType struct {
	mu  sync.Mutex
	str string
}

func (s *stringType) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.str
}

func (s *stringType) SetString(str string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.str = str
}

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

func hostNames(machines []mdb.Machine) (result []string) {
	result = make([]string, len(machines))
	for i := range machines {
		result[i] = machines[i].Hostname
	}
	return
}

func loadTestMdbChannel(count int) <-chan *mdb.Mdb {
	machines := make([]mdb.Machine, count)
	for i := range machines {
		machines[i].Hostname = fmt.Sprintf("host_%d", i)
	}
	anMdb := &mdb.Mdb{Machines: machines}
	result := make(chan *mdb.Mdb, 1)
	result <- anMdb
	return result
}

func getMyHostName(machines []mdb.Machine, myIpAddrs []string) string {
	myIpAddrsMap := make(map[string]struct{}, len(myIpAddrs))
	for _, addr := range myIpAddrs {
		myIpAddrsMap[addr] = struct{}{}
	}
	for _, machine := range machines {
		if _, ok := myIpAddrsMap[machine.IpAddress]; ok {
			return machine.Hostname
		}
	}
	return ""
}

func createEndpointStore(
	logger log.Logger,
	tagvAdder suggest.Adder,
	maybeNilMemoryManager *memoryManagerType,
	myIpAddrs []string) (
	*machine.EndpointStore, *stringType) {
	myHostName := &stringType{}
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
			logger.Fatal(err)
		}
	} else {
		astore = store.NewStoreBytesPerPage(
			*fBytesPerPage, *fPageCount, *fThreshhold, *fDegree)
	}
	dirSpec, err := tricorder.RegisterDirectory("/store")
	if err != nil {
		logger.Fatal(err)
	}
	if err := astore.RegisterMetrics(dirSpec); err != nil {
		logger.Fatal(err)
	}
	stats := machine.NewEndpointStore(
		astore,
		awsinfo.Config{
			CloudHealthTest:   *fCloudHealthTest,
			CloudWatchTest:    *fCloudWatchTest,
			CloudWatchRefresh: *fCloudWatchFreq,
		},
		3)
	var mdbChannel <-chan *mdb.Mdb
	if *fMdbLoadTesting > 0 {
		mdbChannel = loadTestMdbChannel(*fMdbLoadTesting)
	} else {
		mdbChannel = mdbd.StartMdbDaemon(*fMdbFile, logger)
	}
	var machines *mdb.Mdb
	select {
	case machines = <-mdbChannel:
	case <-time.After(30 * time.Second):
		machines = &mdb.Mdb{}
		logger.Println("No mdb available.")
	}
	for _, aName := range hostNames(machines.Machines) {
		tagvAdder.Add(aName)
	}
	myHostNameStr := getMyHostName(machines.Machines, myIpAddrs)
	if myHostNameStr == "" {
		logger.Println(kHostNotFoundMsg)
	}
	myHostName.SetString(myHostNameStr)
	fmt.Println("My host name", myHostNameStr)

	stats.UpdateMachines(
		duration.TimeToFloat(time.Now()), machines.Machines)
	fmt.Println("Initialization complete.")
	// Endpoint refresher goroutine
	go func() {
		for {
			machines := <-mdbChannel
			myHostNameStr := getMyHostName(machines.Machines, myIpAddrs)
			if myHostNameStr == "" {
				logger.Println(kHostNotFoundMsg)
			}
			myHostName.SetString(myHostNameStr)
			stats.UpdateMachines(
				duration.TimeToFloat(time.Now()),
				machines.Machines)
		}
	}()
	return stats, myHostName
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

func handleSignals(logger log.Logger) {
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

func (b *blockingCoordinatorType) WatchPStoreConfig(
	done <-chan struct{}) <-chan string {
	result := make(chan string)
	if done != nil {
		go func() {
			defer close(result)
			<-done
		}()
	}
	return result
}

func (b *blockingCoordinatorType) WithStateListener(
	listener func(blocked bool)) store.Coordinator {
	result := *b
	result.listener = listener
	return &result
}

type maybeNilMemoryManagerWrapperType struct {
	wrapped *memoryManagerType
}

func (m *maybeNilMemoryManagerWrapperType) Check() {
	if m.wrapped != nil {
		m.wrapped.Check()
	}
}

func performInfluxQuery(
	queryStr string,
	epoch string,
	endpoints *machine.EndpointStore,
	freq time.Duration) (interface{}, error) {
	// Special case for show databases. Influx client issues this
	// when user types "use scotty"
	switch strings.ToLower(queryStr) {
	case "show databases":
		return responses.Serialise(
			&client.Response{
				Results: []client.Result{
					{
						Series: []models.Row{
							{
								Name:    "databases",
								Columns: []string{"name"},
								Values: [][]interface{}{
									{"_internal"},
									{"scotty"},
								},
							},
						},
					},
				},
			})
	case "show measurements limit 1":
		return responses.Serialise(
			&client.Response{
				Results: []client.Result{
					{
						Series: []models.Row{
							{
								Name:    "measurements",
								Columns: []string{"name"},
								Values: [][]interface{}{
									{"aname"},
								},
							},
						},
					},
				},
			})
	}

	now := time.Now()
	query, err := qlutils.NewQuery(queryStr, now)
	if err != nil {
		return nil, err
	}
	pqs, colNamesForEachStatement, err := qlutils.ParseQuery(query, now)
	if err != nil {
		return nil, err
	}
	seriesSets, err := tsdbexec.RunParsedQueries(pqs, endpoints, freq)
	if err != nil {
		return nil, err
	}
	epochConversion := kInfluxEpochConversions[epoch]
	if epochConversion == nil {
		epochConversion = kInfluxEpochConversions["ns"]
	}

	return responses.Serialise(
		responses.FromTaggedTimeSeriesSets(
			seriesSets, colNamesForEachStatement, pqs, epochConversion))
}

func setHeader(w http.ResponseWriter, r *http.Request, key, value string) {
	r.Header.Set(key, value)
	w.Header().Set(key, value)
}

func uuidHandler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		setHeader(w, r, "Request-Id", uid.String())
		setHeader(w, r, "X-Influxdb-Version", "0.13.0")
		inner.ServeHTTP(w, r)
	})
}

func dateHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setHeader(w, r, "Date", time.Now().UTC().Format("Mon, 2 Jan 2006 15:04:05 MST"))
		w.WriteHeader(204)
	})

}

func toAddrs(ips []net.Addr) []string {
	result := make([]string, len(ips))
	for i, x := range ips {
		result[i] = x.String()
		index := strings.LastIndexByte(result[i], '/')
		if index != -1 {
			result[i] = result[i][:index]
		}
	}
	return result
}

func main() {
	tricorder.RegisterFlags()
	flag.Parse()
	logger := serverlogger.New("")
	handleSignals(logger)
	myIPs, _ := net.InterfaceAddrs()
	myIPAddrs := toAddrs(myIPs)
	logger.Println("My IP Addresses: ", myIPAddrs)
	// Read configs early so that we will fail fast.
	maybeNilMemoryManager := maybeCreateMemoryManager(logger)
	metricNameEngine := suggest.NewEngine()
	metricNameAdder := newTsdbAdder(metricNameEngine)
	tagkEngine := suggest.NewSuggester("appname", "HostName", "region")
	tagvEngine := suggest.NewEngine()
	tagvAdder := newTsdbAdder(tagvEngine)
	// TODO: Fix this somehow to include all apps
	tagvAdder.Add(application.HealthAgentName)

	endpointStore, myHostName := createEndpointStore(
		logger, tagvAdder, maybeNilMemoryManager, myIPAddrs)
	rpc.RegisterName(
		"Scotty",
		&rpcType{ES: endpointStore},
	)
	rpc.HandleHTTP()
	connectionErrors := newConnectionErrorsType()
	var coord coordinatorBuilderType
	if *fCoord != "" {
		var err error
		coord, err = consul.GetCoordinator(*fCoord, logger)
		if err != nil {
			logger.Println(err)
			coord = &blockingCoordinatorType{}
		}
	}
	totalCounts := startPStoreLoops(
		endpointStore,
		maybeNilMemoryManager,
		logger,
		coord)
	startCollector(
		endpointStore,
		connectionErrors,
		totalCounts,
		metricNameAdder,
		&maybeNilMemoryManagerWrapperType{maybeNilMemoryManager},
		myHostName,
		logger)

	http.Handle(
		"/",
		gzipHandler{&splash.Handler{
			ES:  endpointStore,
			Log: logger,
		}})
	http.Handle(
		"/showAllApps",
		gzipHandler{&showallapps.Handler{
			ES:     endpointStore,
			Logger: logger,
		}})
	http.Handle(
		"/api/hosts/",
		http.StripPrefix(
			"/api/hosts/",
			gzipHandler{&byEndpointHandler{
				ES:     endpointStore,
				Logger: logger,
			}}))
	http.Handle(
		"/api/latest/",
		http.StripPrefix(
			"/api/latest/",
			gzipHandler{&latestHandler{
				ES:     endpointStore,
				Logger: logger,
			}}))

	http.Handle(
		"/api/errors/",
		gzipHandler{&errorHandler{
			ConnectionErrors: connectionErrors,
			Logger:           logger,
		}},
	)

	influxServeMux := http.NewServeMux()

	influxServeMux.Handle(
		"/query",
		uuidHandler(
			apiutil.NewHandler(
				func(req url.Values) (interface{}, error) {
					return performInfluxQuery(
						req.Get("q"),
						req.Get("epoch"),
						endpointStore,
						*fCollectionFrequency)
				},
				nil,
			),
		),
	)

	influxServeMux.Handle(
		"/ping",
		uuidHandler(dateHandler()),
	)

	tsdbServeMux := http.NewServeMux()

	tsdbServeMux.Handle(
		"/api/query",
		tsdbexec.NewHandler(
			func(r *tsdbjson.QueryRequest) ([]tsdbjson.TimeSeries, error) {
				return tsdbexec.Query(
					r, endpointStore, *fCollectionFrequency)
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
		"/api/",
		tsdbexec.NotFoundHandler,
	)

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *fTsdbPort), tsdbServeMux); err != nil {
			logger.Fatal(err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *fInfluxPort), influxServeMux); err != nil {
			logger.Fatal(err)
		}
	}()

	healthserver.SetReady()

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *fPort), nil); err != nil {
		logger.Fatal(err)
	}
}
