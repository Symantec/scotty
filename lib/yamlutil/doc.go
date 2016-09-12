/*
Package yamlutil provides strict unmarshaling of YAML into go structs.

Requirements

This package is designed to work with gopkg.in/yaml.v2 YAML package.

Background

To support both backward and forward compatiblity, the gopkg.in/yaml.v2
package simply ignores unrecognised fields when unmarshaling YAML into a
struct. While this may be a desirable feature for machine generated YAML,
it is not an ideal feature for hand crafted YAML files as humans make mistakes.
This package provides fast failure when a user misspells a field name in a
YAML file rather than silently ignoring the misspelled field.

Usage

If a go struct provides an UnmarshalYAML with a pointer receiver,
gopkg.in/yaml.v2 calls that method to unmarshal rather than unmarshaling in
the default way. One can achieve strict unmarshaling for a particular struct,
which we call the target struct, by defining an UnmarshalYAML method with a
pointer receiver on that struct that delegates to the StrictUnmarshalYAML
function in this package.

This method of defining an UnmarshalYAML method on the target struct is NOT
recursive. That is, if such a struct contains nested structs that do not
also provide similar UnmarshalYAML methods themselves, those nested structs
will not be unmarshaled strictly even though the enclosing target struct will
be. If a struct containing nested structs requires strict unmarshaling all the
way through, each nested struct must also provide an UnmarshalYAML method
that delegates to StrictUnmarshalYAML.

Example

	import (
		"github.com/Symantec/scotty/lib/yamlutil"
		"gopkg.in/yaml.v2"
	)
	...

	type MyYAMLContents struct {
		Name string
		YearBorn int `yaml:"yearBorn"`
		Employer string
	}

	func (m *MyYAMLContents) UnmarshalYAML(
		unmarshal func(interface{}) error) error {
		// Needed to prevent infinite recursion with UnmarshalYAML
		type MyYAMLContentsFields MyYAMLContents
		return yamlutil.StrictUnmarshalYAML(
			unmarshal, (*MyYAMLContentsFields)(m))
	}
	...

	var myStruct MyYAMLContents
	// If fileContents contains any top level field besides name, yearBorn, or
	// employer, this if statement emits an error.
	if err := yaml.Unmarshal(fileContents.Bytes(), &myStruct); err != nil {
		log.Fatal(err)
	}

Future Improvements

Ideally the unarshaling strategy (strict vs. relaxed) would not be tightly
coupled with the struct type. Ideally, the developer would choose the
unmarshaling strategy when unmarshaling happens so that YAML could be
unmarshaled into a struct of a particular type either strictly or in the
default way depending on the application. However, designing a package that
can do that is much more difficult and may not provide good return on
investment. Most of our structs are designed for YAML files used
within a single context so this package does fine for now.
*/
package yamlutil
