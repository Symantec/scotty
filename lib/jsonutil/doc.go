/*
Package jsonutil provides utilities for reading and unmarshaling JSON
including strict unmarshaling of JSON into go structs.

Background

To support both backward and forward compatiblity, the encoding/json
package simply ignores unrecognised fields when unmarshaling JSON into a
struct. While this may be a desirable feature for machine generated JSON,
it is not an ideal feature for hand crafted JSON files as humans make mistakes.
This package provides fast failure when a user misspells a field name in a
JSON file rather than silently ignoring the misspelled field.

Usage

If a go struct provides an UnmarshalJSON with a pointer receiver,
encoding/json calls that method to unmarshal rather than unmarshaling in
the default way. One can achieve strict unmarshaling for a particular struct,
which we call the target struct, by defining an UnmarshalJSON method with a
pointer receiver on that struct that delegates to the StrictUnmarshalJSON
function in this package.

This method of defining an UnmarshalJSON method on the target struct is NOT
recursive. That is, if such a struct contains nested structs that do not
also provide similar UnmarshalJSON methods themselves, those nested structs
will not be unmarshaled strictly even though the enclosing target struct will
be. If a struct containing nested structs requires strict unmarshaling all the
way through, each nested struct must also provide an UnmarshalJSON method
that delegates to StrictUnmarshalJSON.

Example

	import (
		"github.com/Symantec/scotty/lib/jsonutil"
		"encoding/json"
	)
	...

	type MyJSONContents struct {
		Name string
		YearBorn int `json:"yearBorn"`
		Employer string
	}

	func (m *MyYJSONContents) UnmarshalJSON(b []byte) error {
		// Needed to prevent infinite recursion with UnmarshalJSON
		type myJSONContentsFields MyJSONContents
		return jsonutil.StrictUnmarshalJSON(b, (*myJSONContentsFields)(m))
	}
	...

	var myStruct MyJSONContents
	// If fileContents contains any top level field besides name, yearBorn, or
	// employer, this if statement emits an error.
	if err := json.Unmarshal(fileContents.Bytes(), &myStruct); err != nil {
		log.Fatal(err)
	}

Future Improvements

Ideally the unarshaling strategy (strict vs. relaxed) would not be tightly
coupled with the struct type. Ideally, the developer would choose the
unmarshaling strategy when unmarshaling happens so that JSON could be
unmarshaled into a struct of a particular type either strictly or in the
default way depending on the application. However, designing a package that
can do that is much more difficult and may not provide good return on
investment. Most of our structs are designed for JSON files used
within a single context so this package does fine for now.
*/
package jsonutil
