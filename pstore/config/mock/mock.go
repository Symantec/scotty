package mock

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"sync"
)

var (
	kWriter *writer
	kOnce   sync.Once
)

type writer struct {
	accepted map[types.Type]bool
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
			accepted: accepted,
		}
	})
	return kWriter, nil
}

func (w *writer) IsTypeSupported(t types.Type) bool {
	return w.accepted == nil || w.accepted[t]
}

func (w *writer) Write(records []pstore.Record) (err error) {
	return nil
}
