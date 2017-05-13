package dynconfig

import (
	"github.com/Symantec/Dominator/lib/fsutil"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/Dominator/lib/log/prefixlogger"
	"io"
	"os"
	"time"
)

func newDynConfig(
	path string,
	builder func(io.Reader) (interface{}, error),
	name string,
	logger log.Logger,
	value interface{}) *DynConfig {
	result := &DynConfig{
		path:    path,
		builder: builder,
		name:    name,
		logger:  prefixlogger.New(name+": ", logger),
		value:   value,
	}
	go result.loop()
	return result
}

func readFromPath(
	path string,
	builder func(io.Reader) (interface{}, error),
) (interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return builder(file)
}

func newInitialized(
	path string,
	builder func(io.Reader) (interface{}, error),
	name string,
	logger log.Logger) (*DynConfig, error) {
	value, err := readFromPath(path, builder)
	if err != nil {
		return nil, err
	}
	return newDynConfig(path, builder, name, logger, value), nil
}

func (d *DynConfig) get() interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.value
}

func (d *DynConfig) set(x interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.value = x
}

func (d *DynConfig) consumeChange(rc io.ReadCloser) {
	if result, err := d.builder(rc); err != nil {
		d.logger.Println(err)
	} else {
		d.set(result)
	}
	if err := rc.Close(); err != nil {
		d.logger.Println(err)
	}
}

func (d *DynConfig) loop() {
	changeCh := fsutil.WatchFile(d.path, d.logger)
	select {
	case <-time.After(time.Second):
		d.logger.Println("No config file present at startup.")
	case rc := <-changeCh:
		d.consumeChange(rc)
	}
	for rc := range changeCh {
		d.consumeChange(rc)
	}
}
