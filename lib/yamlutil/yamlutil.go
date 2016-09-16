package yamlutil

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func extractYAMLName(tag string) string {
	if !strings.HasPrefix(tag, "yaml:\"") {
		return ""
	}
	return strings.SplitN(tag[6:len(tag)-1], ",", 2)[0]

}

func extractYAMLFields(ptr interface{}) (result []string) {
	t := reflect.TypeOf(ptr).Elem()
	for i := 0; i < t.NumField(); i++ {
		fieldInfo := t.Field(i)
		if name := extractYAMLName(string(fieldInfo.Tag)); name != "" {
			result = append(result, name)
		} else if fieldInfo.PkgPath == "" {
			result = append(result, strings.ToLower(fieldInfo.Name))
		}
	}
	return
}

func fieldError(nameValues map[string]interface{}) error {
	var unrecognized []string
	for key := range nameValues {
		unrecognized = append(unrecognized, key)
	}
	return errors.New(
		fmt.Sprintf(
			"Unrecognized fields: %s",
			strings.Join(unrecognized, ", ")))
}

func strictUnmarshalYAML(
	unmarshal func(part interface{}) error, structPtr interface{}) error {
	// first unmarshal structPtr in the default way
	if err := unmarshal(structPtr); err != nil {
		return err
	}
	// now unmarshal the same chunk into a map
	var nameValues map[string]interface{}
	if err := unmarshal(&nameValues); err != nil {
		return err
	}
	// For each field in ptr, remove corresponding field from nameValues
	for _, fieldName := range extractYAMLFields(structPtr) {
		delete(nameValues, fieldName)
	}
	// anything left in the nameValues map is an unrecognised field.
	if len(nameValues) != 0 {
		return fieldError(nameValues)
	}
	return nil
}
