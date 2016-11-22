package apiutil

import (
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
)

var (
	kErrorType     = reflect.TypeOf((*error)(nil)).Elem()
	kUrlValuesType = reflect.TypeOf(url.Values(nil))
)

func (o *Options) fixupDefaults() {
	if o.ErrorGenerator == nil {
		o.ErrorGenerator = defaultErrorGenerator
	}
}

type apiHandlerType struct {
	options      *Options
	inType       reflect.Type
	handlerValue reflect.Value
}

func newHandler(
	handler interface{}, options *Options) http.Handler {
	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()
	if handlerType.Kind() != reflect.Func {
		panic("NewHandler argument must be a func.")
	}
	if handlerType.NumIn() != 1 {
		panic("NewHandler argument must be a func of one arg")
	}
	if handlerType.NumOut() != 2 || handlerType.Out(1) != kErrorType {
		panic("NewHandler argument must be a func returning 1 value and 1 error")
	}
	inType := handlerType.In(0)
	var optionsCopy Options
	if options != nil {
		optionsCopy = *options
	}
	optionsCopy.fixupDefaults()
	return &apiHandlerType{options: &optionsCopy, inType: inType, handlerValue: handlerValue}
}

func (h *apiHandlerType) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var inValue reflect.Value
	if h.inType == kUrlValuesType {
		if err := r.ParseForm(); err != nil {
			showError(w, 400, err, h.options.ErrorGenerator)
			return
		}
		inValue = reflect.ValueOf(r.Form)
	} else {
		ptrValue := reflect.New(h.inType)
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(ptrValue.Interface()); err != nil {
			showError(w, 400, err, h.options.ErrorGenerator)
			return
		}
		inValue = ptrValue.Elem()
	}
	// Set up response headers
	headers := w.Header()
	headers.Add("Content-Type", "application/json; charset=UTF-8")
	// Call the handler
	results := h.handlerValue.Call([]reflect.Value{inValue})
	if !results[1].IsNil() {
		showError(
			w, 400, results[1].Interface().(error), h.options.ErrorGenerator)
		return
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(results[0].Interface())
}

func showError(
	w http.ResponseWriter,
	statusCode int,
	err error,
	errGen func(statusCode int, err error) interface{}) {
	var whatToEncode interface{}
	switch e := err.(type) {
	case HTTPError:
		w.WriteHeader(e.Status())
		whatToEncode = e
	default:
		w.WriteHeader(statusCode)
		whatToEncode = errGen(statusCode, err)
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(whatToEncode)
}

func defaultErrorGenerator(status int, err error) interface{} {

	return &struct {
		Error string `json:"error,omitempty"`
	}{
		Error: err.Error(),
	}
}
