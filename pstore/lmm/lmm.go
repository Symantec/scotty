package lmm

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/tricorder/go/tricorder/messages"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"time"
)

var (
	supportedTypes = map[types.Type]bool{
		types.Bool:       true,
		types.Int8:       true,
		types.Int16:      true,
		types.Int32:      true,
		types.Int64:      true,
		types.Uint8:      true,
		types.Uint16:     true,
		types.Uint32:     true,
		types.Uint64:     true,
		types.Float32:    true,
		types.Float64:    true,
		types.GoTime:     true,
		types.GoDuration: true,
	}
)

func isTypeSupported(t types.Type) bool {
	return supportedTypes[t]
}

func asFloat64(r *pstore.Record) float64 {
	switch r.Kind {
	case types.Bool:
		if r.Value.(bool) {
			return 1.0
		}
		return 0.0
	case types.Int8:
		return float64(r.Value.(int8))
	case types.Int16:
		return float64(r.Value.(int16))
	case types.Int32:
		return float64(r.Value.(int32))
	case types.Int64:
		return float64(r.Value.(int64))
	case types.Uint8:
		return float64(r.Value.(uint8))
	case types.Uint16:
		return float64(r.Value.(uint16))
	case types.Uint32:
		return float64(r.Value.(uint32))
	case types.Uint64:
		return float64(r.Value.(uint64))
	case types.Float32:
		return float64(r.Value.(float32))
	case types.Float64:
		return r.Value.(float64)
	case types.GoTime:
		return messages.TimeToFloat(r.Value.(time.Time))
	case types.GoDuration:
		return messages.DurationToFloat(
			r.Value.(time.Duration)) * units.FromSeconds(
			r.Unit)
	default:
		panic("Unsupported type")

	}
}
