// Package sysmemory tells scotty how much memory to use.
package sysmemory

// TotalMemoryToUse returns how much memory in bytes scotty should use.
// Returning 0 means that user did not specify total memory to use.
func TotalMemoryToUse() (uint64, error) {
	return totalMemoryToUse()
}
