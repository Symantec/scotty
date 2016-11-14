package mock

import (
	"fmt"
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"sync"
	"time"
)

type hostAppMetric struct {
	Host   string
	App    string
	Metric string
}

var (
	kWriter *writer
	kOnce   sync.Once
)

type writer struct {
	accepted    map[types.Type]bool
	latestTimes map[hostAppMetric]time.Time
}

func newWriter(c Config) (
	result pstore.LimitedRecordWriter, err error) {
	// We use the once mechanism because we must ensure that the
	// mock db survives config file rewrites. Normally we just create
	// new writer objects each time the config file changes which is
	// ok because the real persistent store lives in another
	// process. But since this is an in memory, mock persistent store
	// it must truly persist even across config file rewrites.
	kOnce.Do(func() {
		var accepted map[types.Type]bool
		if len(c.Accepted) > 0 {
			accepted = make(map[types.Type]bool, len(c.Accepted))
			for _, a := range c.Accepted {
				accepted[types.Type(a)] = true
			}
		}
		kWriter = &writer{
			accepted:    accepted,
			latestTimes: make(map[hostAppMetric]time.Time),
		}
	})
	return kWriter, nil
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return w.accepted == nil || w.accepted[t]
}

func (w *writer) Write(records []pstore.Record) (err error) {
	for i := range records {
		tuple := hostAppMetric{
			Host:   records[i].HostName,
			App:    records[i].Tags[pstore.TagAppName],
			Metric: records[i].Path,
		}
		// Enforce that timestamps are recorded in order for a particular
		// time series. Scotty is supposed to guarantee this.
		if !records[i].Timestamp.After(w.latestTimes[tuple]) {
			return fmt.Errorf(
				"For host: %s, app: %s, metric: %s ts %v before %v",
				tuple.Host,
				tuple.App,
				tuple.Metric,
				records[i].Timestamp,
				w.latestTimes[tuple])
		}
		w.latestTimes[tuple] = records[i].Timestamp
	}
	return nil
}
