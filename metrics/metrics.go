package metrics

import (
	"errors"
	"fmt"
	"github.com/Symantec/tricorder/go/tricorder/types"
	"time"
)

var (
	errGroupId = errors.New("metrics: Conflicting Timestamps for group Id.")
)

func verifyList(list List) error {
	length := list.Len()
	pathSet := make(map[string]bool, length)
	groupIdToTimeStamp := make(map[int]time.Time)
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
		if !value.TimeStamp.IsZero() {
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
