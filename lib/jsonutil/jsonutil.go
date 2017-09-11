package jsonutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func extractJSONName(tag string) string {
	if !strings.HasPrefix(tag, "json:\"") {
		return ""
	}
	return strings.SplitN(tag[6:len(tag)-1], ",", 2)[0]

}

func extractJSONFields(ptr interface{}) (result []string) {
	t := reflect.TypeOf(ptr).Elem()
	for i := 0; i < t.NumField(); i++ {
		fieldInfo := t.Field(i)
		if name := extractJSONName(string(fieldInfo.Tag)); name != "" {
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

func strictUnmarshalJSON(
	b []byte, structPtr interface{}) error {
	// first unmarshal structPtr in the default way
	if err := json.Unmarshal(b, structPtr); err != nil {
		return err
	}
	// now unmarshal the same chunk into a map
	var nameValues map[string]interface{}
	if err := json.Unmarshal(b, &nameValues); err != nil {
		return err
	}
	// For each field in ptr, remove corresponding field from nameValues
	for _, fieldName := range extractJSONFields(structPtr) {
		delete(nameValues, fieldName)
	}
	// anything left in the nameValues map is an unrecognised field.
	if len(nameValues) != 0 {
		return fieldError(nameValues)
	}
	return nil
}
