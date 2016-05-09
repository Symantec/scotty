// Package lmm contains common utilities for writing to lmm
package lmm

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/types"
)

const (
	Version = "1"
)

// IsTypeSupported returns true if lmm supports the given metric type
func IsTypeSupported(t types.Type) bool {
	return isTypeSupported(t)
}

// ToFloat64 converts the metric value in r to a floating point value for lmm.
func ToFloat64(r *pstore.Record) float64 {
	return asFloat64(r)
}
