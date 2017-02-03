package main

import (
	"github.com/Symantec/scotty/pstore"
	"github.com/Symantec/scotty/sysmemory"
	"github.com/Symantec/tricorder/go/tricorder"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

func totalMemoryUsed() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}

// Returns alloc memory needed to trigger gc along with gc cycle.
func gcCollectThresh() (uint64, uint32) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.NextGC, memStats.NumGC
}

func totalSystemMemory() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Sys
}

// store.Store implements this interface. This interface is here to decouple
// the store package from memory management code.
type memoryType interface {
	// Returns true if we allocate new memory rather than recycling it.
	IsExpanding() bool
	// Pass false to recycle memory; pass true to allocate new memory as
	// needed.
	SetExpanding(b bool)
	// Sends request to free up bytesToFree bytes on the spot.
	FreeUpBytes(bytesToFree uint64)
}

// Metrics for memory management
type memoryManagerMetricsType struct {
	// Total number of GC cycles
	TotalCycleCount uint64
	// Number of GC cycles that we inspected. Will be less than total cycle
	// count. Regrettably, there is no way to be called every time a new
	// GC cycle begins or ends. The best we can do is check the GC at
	// opportune times.
	InspectedCycleCount uint64
	// Number of forced, stop-the-world, garbage collections.
	STWGCCount uint64
	// Number of cycles where we needed to ensure expanding is turned off
	// as in memoryType.SetExpanding(false)
	NoExpandCount uint64
	// Total number of bytes freed with memoryType.FreeUpBytes
	PageFreeInBytes uint64
	// Number of times memoryType.FreeUpBytes was called
	PageFreeCount uint64
	// Number of alloced bytes on heap needed to trigger next GC collection
	AllocBytesNeededForGC uint64
	// Largest AllocBytesNeededForGC so far
	LargestAllocBytesNeededForGC uint64
}

// Manages scotty's memory
type memoryManagerType struct {
	hardLimit     uint64
	highWaterMark uint64
	lowWaterMark  uint64
	logger        *log.Logger
	gcPercent     int
	gcCh          chan bool
	memoryCh      chan memoryType
	lock          sync.Mutex
	stats         memoryManagerMetricsType
}

func allocateMemory(totalMemoryToUse uint64) (allocatedMemory [][]byte) {
	chunkSize := totalMemoryToUse/1000 + 1
	for totalSystemMemory() < (totalMemoryToUse-chunkSize+1)/100*99 {
		chunk := make([]byte, chunkSize)
		allocatedMemory = append(allocatedMemory, chunk)
	}
	return
}

// Returns the GC percent See https://golang.org/pkg/runtime/#pkg-overview.
// A negative value means GC is turned off.
func getGcPercent() (result int) {
	// Have to do it this way as there is no GetGCPercent
	result = debug.SetGCPercent(-1)
	debug.SetGCPercent(result)
	return
}

// Creates the memory manager if command line args request it. Otherwise
// returns nil.
func maybeCreateMemoryManager(logger *log.Logger) *memoryManagerType {
	totalMemoryToUse, err := sysmemory.TotalMemoryToUse()
	if err != nil {
		log.Fatal(err)
	}
	if totalMemoryToUse > 0 {
		gcPercent := getGcPercent()
		if gcPercent <= 0 {
			log.Fatal("To use dynamic memory management, GC must be enabled")
		}
		allocatedMemory := allocateMemory(totalMemoryToUse)
		// Adjust totalMemoryToUse with actual memory used at
		// this point which is slightly smaller because of
		// system overhead.
		totalMemoryToUse = totalMemoryUsed()
		// Prevent compiler complaining about unused allocated memory
		logger.Printf(
			"totalMemoryInUse: %d\n",
			totalMemoryToUse+uint64(len(allocatedMemory)*0))
		allocatedMemory = nil
		now := time.Now()
		runtime.GC()
		logger.Printf("GCTime: %v; totalMemoryInUse: %d\n", time.Since(now), totalMemoryUsed())
		result := &memoryManagerType{
			hardLimit:     totalMemoryToUse,
			highWaterMark: uint64(float64(totalMemoryToUse) * 0.90),
			lowWaterMark:  uint64(float64(totalMemoryToUse) * 0.80),
			logger:        logger,
			gcPercent:     gcPercent,
			gcCh:          make(chan bool),
			memoryCh:      make(chan memoryType, 10),
		}
		go result.loop()
		return result
	}
	return nil
}

// Check tells the memory manager to inspect the current GC cycle and take
// necessary actions.
func (m *memoryManagerType) Check() {
	select {
	case m.gcCh <- true:
	default:
	}
}

// SetMemory tells the memory manager what is implementing memoryType.
func (m *memoryManagerType) SetMemory(memory memoryType) {
	m.memoryCh <- memory
}

// Metrics returns the memory manager metrics at stats
func (m *memoryManagerType) Metrics(stats *memoryManagerMetricsType) {
	m.lock.Lock()
	defer m.lock.Unlock()
	*stats = m.stats
}

// RegisterMetrics registers the memory manager metrics
func (m *memoryManagerType) RegisterMetrics() (err error) {
	var data memoryManagerMetricsType
	group := tricorder.NewGroup()
	group.RegisterUpdateFunc(
		func() time.Time {
			m.Metrics(&data)
			return time.Now()
		})
	if err = group.RegisterMetric(
		"/proc/memory-manager/alloc-bytes-needed-for-gc",
		&data.AllocBytesNeededForGC,
		units.Byte,
		"Number of allocated bytes needed to trigger GC"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/largest-alloc-bytes-needed-for-gc",
		&data.LargestAllocBytesNeededForGC,
		units.Byte,
		"Number of allocated bytes needed to trigger GC"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/inspected-cycle-count",
		&data.InspectedCycleCount,
		units.None,
		"Number of gc cycles inspected"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/total-cycle-count",
		&data.TotalCycleCount,
		units.None,
		"Number of total gc cycles"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/stw-gc-count",
		&data.STWGCCount,
		units.None,
		"Number of stop the world GCs"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/no-expand-count",
		&data.NoExpandCount,
		units.None,
		"Inspected cycle counts where we disabled expanding"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/page-free-bytes",
		&data.PageFreeInBytes,
		units.Byte,
		"Total number of pages freed in bytes"); err != nil {
		return
	}
	if err = group.RegisterMetric(
		"/proc/memory-manager/page-free-count",
		&data.PageFreeCount,
		units.None,
		"Number of times pages freed"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/hard-limit",
		&m.hardLimit,
		units.Byte,
		"Target memory usage"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/high-water-mark",
		&m.highWaterMark,
		units.Byte,
		"Memory usage that triggers a GC"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/low-water-mark",
		&m.lowWaterMark,
		units.Byte,
		"Desired memory usage after GC happens"); err != nil {
		return
	}
	if err = tricorder.RegisterMetric(
		"proc/memory-manager/gc-percent",
		&m.gcPercent,
		units.None,
		"GC percentage"); err != nil {
		return
	}
	return
}

func (m *memoryManagerType) setMetrics(stats *memoryManagerMetricsType) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.stats = *stats
}

// This function is the workhorse of memory management.
// Changing memory management strategies means changing this function
func (m *memoryManagerType) loop() {
	var lastCycleCount uint32
	// The memory object
	var memory memoryType
	// Are we allocating new memory (true) or are we trying to recycle (false)?
	var isExpanding bool
	// Our statistics for metrics.
	var stats memoryManagerMetricsType
	for {
		// Block until we are asked to check GC cycle.
		select {
		// Some goroutine called Check()
		case <-m.gcCh:
			// Some goroutine gave us the memory to monitor, in particular
			// the store.Store instance which uses most of scotty's
			// memory.
		case memory = <-m.memoryCh:
			isExpanding = memory.IsExpanding()
		}
		// Find out the max memory usage needed to trigger the next GC.
		// If we had already processed this cycle, just skip it.
		maxMemory, cycleCount := gcCollectThresh()
		if cycleCount > lastCycleCount {
			stats.AllocBytesNeededForGC = maxMemory
			if stats.AllocBytesNeededForGC > stats.LargestAllocBytesNeededForGC {
				stats.LargestAllocBytesNeededForGC = stats.AllocBytesNeededForGC
			}
			stats.TotalCycleCount = uint64(cycleCount)
			stats.InspectedCycleCount++
			// If the allocated memory needed for a GC exceeds the low
			// watermark, turn off expanding once and for all so that we
			// recycle pages in scotty instead of allocating new ones. Once
			// we turn off expanding, we expect total memory usage to remain
			// constant.
			if maxMemory >= m.lowWaterMark {
				stats.NoExpandCount++
				if memory != nil && isExpanding {
					memory.SetExpanding(false)
					isExpanding = false
				}
			}
			m.setMetrics(&stats)
			lastCycleCount = cycleCount
		}
	}
}

type writeHookerType struct {
	wrapped *memoryManagerType
}

func (w writeHookerType) WriteHook(
	unusedRecords []pstore.Record, unusedError error) {
	w.wrapped.Check()
}
