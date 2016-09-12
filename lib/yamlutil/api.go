package yamlutil

// StrictUnmarshalYAML unmarshals YAML storing at structPtr, but returns an
// error if the YAML contains unrecognized top level fields.
//
// unmarshal is what is passed to the standard UnmarshalYAML method.
// structPtr must be a pointer to a struct, not a slice or map.
func StrictUnmarshalYAML(
	unmarshal func(interface{}) error, structPtr interface{}) error {
	return strictUnmarshalYAML(unmarshal, structPtr)
}
