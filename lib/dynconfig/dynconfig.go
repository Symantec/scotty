package dynconfig

import (
	"github.com/Symantec/Dominator/lib/fsutil"
	"github.com/Symantec/Dominator/lib/log"
	"github.com/Symantec/Dominator/lib/log/prefixlogger"
	"io"
	"os"
	"reflect"
	"time"
)

var (
	kIOReaderType = reflect.TypeOf((*io.Reader)(nil)).Elem()
	kErrorType    = reflect.TypeOf((*error)(nil)).Elem()
)

func builderToValue(builder interface{}) reflect.Value {
	t := reflect.TypeOf(builder)
	if t.Kind() != reflect.Func {
		panic("builder must be a function")
	}
	if t.NumIn() != 1 || t.IsVariadic() {
		panic("builder takes exactly one parameter")
	}
	if t.In(0) != kIOReaderType {
		panic("builder's parameter must be an io.Reader")
	}
	if t.NumOut() != 2 {
		panic("Builder must return any type and an error")
	}
	if t.Out(1) != kErrorType {
		panic("Builder's second return value must be an error")
	}
	return reflect.ValueOf(builder)
}

func callBuilder(builder reflect.Value, r io.Reader) (interface{}, error) {
	results := builder.Call([]reflect.Value{reflect.ValueOf(r)})
	return results[0].Interface(), results[1].Interface().(error)
}

func newDynConfig(
	path string,
	builder reflect.Value,
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
	builder reflect.Value,
) (interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return callBuilder(builder, file)
}

func newInitialized(
	path string,
	builder reflect.Value,
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
	if result, err := callBuilder(d.builder, rc); err != nil {
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
