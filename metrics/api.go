// Package metrics provides a view of metrics that is independent of source.
package metrics

import (
	"github.com/Symantec/tricorder/go/tricorder/units"
	"sort"
	"time"
)

// Value represents a single metric value.
type Value struct {
	// Required. The name of the metric. For tricorder metrics, this is
	// the metric path. This field must be unique across a list of metrics.
	Path string
	// Optional. The description of the metric.
	Description string
	// Optional. The unit of the metric.
	Unit units.Unit
	// Required. Value is the value of the metric. The type stored in
	// value must be one of the following:
	// bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64,
	// float32, float64, string, time.Time, time.Duration, or
	// tricorder/go/tricorder/messages.Distribution
	Value interface{}
	// Optional. The timestamp of the value. If present in one value in a
	// list then it is required for ALL values in that same list.
	TimeStamp time.Time
	// Optional. The timestamp group ID. Required if TimeStamp is provided.
	// If a.GroupId == b.GroupId then a.TimeStamp == b.TimeStamp, but
	// the converse does not have to be true. Furthermore, GroupId must
	// remain the same for all values of a given metric. However,
	// multiple metrics can share the same group ID.
	GroupId int
}

// List represents a list of metrics sorted in ascending order by path from an
// endpoint. To compare two paths, first split the paths by '/' then compare
// the first parts. On tie, compare the second parts. If still a tie, compare
// the third parts and so forth. By this method "/foo/bar" precedes "/foo.bar"
// even though "/foo.bar" precedes "/foo/bar" lexicoographically
type List interface {
	// Len returns the number of metrics.
	Len() int
	// Index stores the ith metric at value
	Index(i int, value *Value)
}

// Find returns the index in list having the path equal to or just greater
// than path. If path is greater than the last value in list, Find returns
// the size of the list.
func Find(list List, path string) int {
	return find(list, newPath(path))
}

// FileSystems returns all the distinct file systems mount points in the list.
// A metric for file system with mount point "/some/filesystem" would look
// like "/sys/fs/some/filesystem/METRICS/some/metric"
func FileSystems(list List) []string {
	return fileSystems(list)
}

// Endpoints returns the name and port of each reported application.
// The key of the returned map is the application name; the value is
// the port number. The applications are listed under /health-checks
// metrics tree.
func Endpoints(list List) map[string]uint {
	return endpoints(list)
}

// GetFloat64 gets the float64 value at path.
// GetFloat64 returns false if path doesn't exist or if value is not float64.
func GetFloat64(list List, path string) (float64, bool) {
	return getFloat64(list, path)
}

// GetUint64 gets the Uint64 value at path.
// GetUint64 returns false if path doesn't exist or if value is not uint64.
func GetUint64(list List, path string) (uint64, bool) {
	return getUint64(list, path)
}

// VerifyList verifies that the given list adheres to the specification.
func VerifyList(list List) error {
	return verifyList(list)
}

// SimpleList lets a slice of Value instances be a List.
type SimpleList []Value

func (s SimpleList) Len() int {
	return len(s)
}

func (s SimpleList) Index(i int, value *Value) {
	*value = s[i]
}

func (s SimpleList) Less(i, j int) bool {
	return comparePaths(newPath(s[i].Path), newPath(s[j].Path)) < 0
}

func (s SimpleList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Returns a brand new SimpleList like this one but with values sorted by
// path.
func (s SimpleList) Sorted() SimpleList {
	result := make(SimpleList, len(s))
	copy(result, s)
	sort.Sort(result)
	return result
}
