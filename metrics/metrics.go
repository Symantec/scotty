package metrics

import (
	"errors"
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"time"
)

var (
	errTimeStamp = errors.New("metrics: TimeStamp must be present in all values or absent in all values.")
	errGroupId   = errors.New("metrics: Invalid GroupId found.")
)

func verifyList(list List) error {
	length := list.Len()
	pathSet := make(map[string]bool, length)
	groupIdToTimeStamp := make(map[int]time.Time)
	var zeroTsFound, nonZeroTsFound bool
	for i := 0; i < length; i++ {
		var value Value
		list.Index(i, &value)
		if pathSet[value.Path] {
			return errors.New(
				fmt.Sprintf("Duplicate path: %s", value.Path))
		}
		pathSet[value.Path] = true
		if types.FromGoValue(value.Value) == types.Unknown {
			return errors.New(
				fmt.Sprintf("Bad value: %v", value.Value))
		}
		if value.TimeStamp.IsZero() {
			if nonZeroTsFound {
				return errTimeStamp
			}
			zeroTsFound = true
		} else {
			if zeroTsFound {
				return errTimeStamp
			}
			nonZeroTsFound = true
		}
		if zeroTsFound {
			if value.GroupId != 0 {
				return errGroupId
			}
		} else {
			lastTs, ok := groupIdToTimeStamp[value.GroupId]
			if !ok {
				groupIdToTimeStamp[value.GroupId] = value.TimeStamp
			} else if value.TimeStamp != lastTs {
				return errGroupId
			}
		}
	}
	return nil
}
