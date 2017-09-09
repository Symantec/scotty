package jsonutil

// StrictUnmarshalJSON unmarshals JSON storing at structPtr, but returns an
// error if the JSON contains unrecognized top level fields.
//
// structPtr must be a pointer to a struct, not a slice or map.
func StrictUnmarshalJSON(
	b []byte, structPtr interface{}) error {
	return strictUnmarshalJSON(b, structPtr)
}
