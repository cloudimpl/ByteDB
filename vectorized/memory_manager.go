package vectorized

import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// AdaptiveMemoryManager manages memory allocation and batch sizing for vectorized operations
type AdaptiveMemoryManager struct {
	mu                    sync.RWMutex
	maxMemoryMB          int64
	currentMemoryMB      int64
	batchPools           map[int]*VectorBatchPool
	adaptiveBatchSize    int
	memoryPressure       float64
	lastGCTime           time.Time
	allocatedBatches     map[*VectorBatch]*BatchAllocation
	memoryStats          *MemoryStats
	config               *MemoryConfig
}

// MemoryConfig holds configuration for memory management
type MemoryConfig struct {
	MaxMemoryMB        int64   // Maximum memory limit in MB
	MinBatchSize       int     // Minimum batch size
	MaxBatchSize       int     // Maximum batch size
	DefaultBatchSize   int     // Default batch size
	GCThreshold        float64 // Memory pressure threshold to trigger GC
	AdaptiveEnabled    bool    // Enable adaptive batch sizing
	MemoryPoolEnabled  bool    // Enable memory pooling
	CompressionEnabled bool    // Enable data compression
	PreallocationRatio float64 // Ratio of memory to preallocate
}

// VectorBatchPool manages a pool of vector batches for reuse
type VectorBatchPool struct {
	mu        sync.Mutex
	batches   []*VectorBatch
	batchSize int
	maxSize   int
	hits      int64
	misses    int64
}

// BatchAllocation tracks allocation metadata for a batch
type BatchAllocation struct {
	AllocatedAt   time.Time
	SizeBytes     int64
	BatchSize     int
	Schema        *Schema
	InUse         bool
	LastAccessed  time.Time
}

// MemoryStats tracks memory usage statistics
type MemoryStats struct {
	TotalAllocatedMB    int64
	PeakMemoryMB        int64
	CurrentBatches      int
	PoolHitRate         float64
	GCCount             int64
	AvgBatchSize        float64
	MemoryEfficiency    float64
	CompressionRatio    float64
	AdaptiveSizeChanges int64
}

// Memory pressure levels
const (
	LowPressure    = 0.7  // Below 70% of max memory
	MediumPressure = 0.85 // Between 70-85% of max memory
	HighPressure   = 0.95 // Above 85% of max memory
)

// NewAdaptiveMemoryManager creates a new adaptive memory manager
func NewAdaptiveMemoryManager(config *MemoryConfig) *AdaptiveMemoryManager {
	if config == nil {
		config = DefaultMemoryConfig()
	}
	
	manager := &AdaptiveMemoryManager{
		maxMemoryMB:       config.MaxMemoryMB,
		batchPools:       make(map[int]*VectorBatchPool),
		adaptiveBatchSize: config.DefaultBatchSize,
		allocatedBatches: make(map[*VectorBatch]*BatchAllocation),
		memoryStats:      &MemoryStats{},
		config:           config,
		lastGCTime:       time.Now(),
	}
	
	// Initialize common batch size pools
	commonSizes := []int{64, 256, 1024, 4096, 16384}
	for _, size := range commonSizes {
		manager.batchPools[size] = NewVectorBatchPool(size, 16) // 16 batches per pool
	}
	
	// Start background monitoring if adaptive is enabled
	if config.AdaptiveEnabled {
		go manager.adaptiveMonitor()
	}
	
	return manager
}

// DefaultMemoryConfig returns default memory configuration
func DefaultMemoryConfig() *MemoryConfig {
	// Get system memory info
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	systemMemoryMB := int64(memStats.Sys / 1024 / 1024)
	
	return &MemoryConfig{
		MaxMemoryMB:        min64(systemMemoryMB/4, 2048), // Use 25% of system memory, max 2GB
		MinBatchSize:       64,
		MaxBatchSize:       32768,
		DefaultBatchSize:   4096,
		GCThreshold:        0.85,
		AdaptiveEnabled:    true,
		MemoryPoolEnabled:  true,
		CompressionEnabled: false, // Disabled by default for performance
		PreallocationRatio: 0.1,   // Preallocate 10% of max memory
	}
}

// AllocateBatch allocates a new vector batch with adaptive sizing
func (amm *AdaptiveMemoryManager) AllocateBatch(schema *Schema) (*VectorBatch, error) {
	amm.mu.Lock()
	defer amm.mu.Unlock()
	
	// Update memory pressure
	amm.updateMemoryPressure()
	
	// Determine optimal batch size
	batchSize := amm.getOptimalBatchSize()
	
	// Try to get batch from pool first
	if amm.config.MemoryPoolEnabled {
		if batch := amm.getBatchFromPool(schema, batchSize); batch != nil {
			return batch, nil
		}
	}
	
	// Check memory limits
	estimatedSize := amm.estimateBatchSize(schema, batchSize)
	if amm.currentMemoryMB+estimatedSize > amm.maxMemoryMB {
		// Try to free memory
		amm.freeMemory()
		
		// Check again
		if amm.currentMemoryMB+estimatedSize > amm.maxMemoryMB {
			return nil, fmt.Errorf("insufficient memory: need %d MB, available %d MB", 
				estimatedSize, amm.maxMemoryMB-amm.currentMemoryMB)
		}
	}
	
	// Create new batch
	batch := NewVectorBatch(schema, batchSize)
	
	// Track allocation
	allocation := &BatchAllocation{
		AllocatedAt:  time.Now(),
		SizeBytes:    estimatedSize * 1024 * 1024, // Convert MB to bytes
		BatchSize:    batchSize,
		Schema:       schema,
		InUse:        true,
		LastAccessed: time.Now(),
	}
	
	amm.allocatedBatches[batch] = allocation
	amm.currentMemoryMB += estimatedSize
	amm.memoryStats.TotalAllocatedMB += estimatedSize
	amm.memoryStats.CurrentBatches++
	
	// Update peak memory
	if amm.currentMemoryMB > amm.memoryStats.PeakMemoryMB {
		amm.memoryStats.PeakMemoryMB = amm.currentMemoryMB
	}
	
	return batch, nil
}

// FreeBatch frees a vector batch and returns it to the pool if possible
func (amm *AdaptiveMemoryManager) FreeBatch(batch *VectorBatch) {
	amm.mu.Lock()
	defer amm.mu.Unlock()
	
	allocation, exists := amm.allocatedBatches[batch]
	if !exists {
		return // Batch not tracked
	}
	
	// Update memory tracking
	sizeMB := allocation.SizeBytes / (1024 * 1024)
	amm.currentMemoryMB -= sizeMB
	amm.memoryStats.CurrentBatches--
	
	// Try to return to pool
	if amm.config.MemoryPoolEnabled && amm.canReturnToPool(batch) {
		amm.returnBatchToPool(batch)
	}
	
	delete(amm.allocatedBatches, batch)
}

// GetOptimalBatchSize returns the current optimal batch size
func (amm *AdaptiveMemoryManager) GetOptimalBatchSize() int {
	amm.mu.RLock()
	defer amm.mu.RUnlock()
	return amm.adaptiveBatchSize
}

// GetMemoryStats returns current memory statistics
func (amm *AdaptiveMemoryManager) GetMemoryStats() *MemoryStats {
	amm.mu.RLock()
	defer amm.mu.RUnlock()
	
	// Calculate derived statistics
	stats := *amm.memoryStats
	stats.MemoryEfficiency = float64(amm.currentMemoryMB) / float64(amm.maxMemoryMB)
	
	// Calculate pool hit rate
	totalHits, totalMisses := int64(0), int64(0)
	for _, pool := range amm.batchPools {
		pool.mu.Lock()
		totalHits += pool.hits
		totalMisses += pool.misses
		pool.mu.Unlock()
	}
	
	if totalHits+totalMisses > 0 {
		stats.PoolHitRate = float64(totalHits) / float64(totalHits+totalMisses)
	}
	
	// Calculate average batch size
	if stats.CurrentBatches > 0 {
		totalSize := int64(0)
		for _, allocation := range amm.allocatedBatches {
			totalSize += int64(allocation.BatchSize)
		}
		stats.AvgBatchSize = float64(totalSize) / float64(stats.CurrentBatches)
	}
	
	return &stats
}

// updateMemoryPressure calculates current memory pressure
func (amm *AdaptiveMemoryManager) updateMemoryPressure() {
	amm.memoryPressure = float64(amm.currentMemoryMB) / float64(amm.maxMemoryMB)
}

// getOptimalBatchSize determines the optimal batch size based on current conditions
func (amm *AdaptiveMemoryManager) getOptimalBatchSize() int {
	if !amm.config.AdaptiveEnabled {
		return amm.config.DefaultBatchSize
	}
	
	baseSize := amm.adaptiveBatchSize
	
	// Adjust based on memory pressure
	switch {
	case amm.memoryPressure > HighPressure:
		// High pressure: reduce batch size
		baseSize = max(baseSize/2, amm.config.MinBatchSize)
		amm.memoryStats.AdaptiveSizeChanges++
		
	case amm.memoryPressure < LowPressure:
		// Low pressure: increase batch size if beneficial
		baseSize = min(baseSize*3/2, amm.config.MaxBatchSize)
		amm.memoryStats.AdaptiveSizeChanges++
		
	default:
		// Medium pressure: keep current size
	}
	
	// Update adaptive batch size with smoothing
	amm.adaptiveBatchSize = (amm.adaptiveBatchSize*3 + baseSize) / 4
	
	return amm.adaptiveBatchSize
}

// getBatchFromPool tries to get a batch from the appropriate pool
func (amm *AdaptiveMemoryManager) getBatchFromPool(schema *Schema, batchSize int) *VectorBatch {
	// Find closest pool size
	poolSize := amm.findClosestPoolSize(batchSize)
	pool, exists := amm.batchPools[poolSize]
	if !exists {
		return nil
	}
	
	batch := pool.Get()
	if batch != nil {
		// Reset batch for reuse
		batch.Reset()
		amm.resizeBatch(batch, schema, batchSize)
		return batch
	}
	
	return nil
}

// returnBatchToPool returns a batch to the appropriate pool
func (amm *AdaptiveMemoryManager) returnBatchToPool(batch *VectorBatch) {
	poolSize := amm.findClosestPoolSize(batch.Capacity)
	pool, exists := amm.batchPools[poolSize]
	if exists {
		pool.Put(batch)
	}
}

// findClosestPoolSize finds the closest pool size for a given batch size
func (amm *AdaptiveMemoryManager) findClosestPoolSize(size int) int {
	bestSize := amm.config.DefaultBatchSize
	bestDiff := abs(bestSize - size)
	
	for poolSize := range amm.batchPools {
		diff := abs(poolSize - size)
		if diff < bestDiff {
			bestSize = poolSize
			bestDiff = diff
		}
	}
	
	return bestSize
}

// estimateBatchSize estimates memory usage for a batch
func (amm *AdaptiveMemoryManager) estimateBatchSize(schema *Schema, batchSize int) int64 {
	totalSize := int64(0)
	
	for _, field := range schema.Fields {
		columnSize := int64(batchSize) * int64(field.DataType.Size())
		nullMaskSize := int64(batchSize+63) / 64 * 8 // Null mask bitmap
		totalSize += columnSize + nullMaskSize
	}
	
	// Add overhead for batch structure
	overhead := int64(1024) // 1KB overhead
	totalSize += overhead
	
	// Convert to MB and round up
	return (totalSize + 1024*1024 - 1) / (1024 * 1024)
}

// canReturnToPool checks if a batch can be returned to the pool
func (amm *AdaptiveMemoryManager) canReturnToPool(batch *VectorBatch) bool {
	allocation := amm.allocatedBatches[batch]
	if allocation == nil {
		return false
	}
	
	// Don't pool very large or very small batches
	if allocation.BatchSize > amm.config.MaxBatchSize/2 || allocation.BatchSize < amm.config.MinBatchSize*2 {
		return false
	}
	
	// Don't pool if memory pressure is high
	if amm.memoryPressure > HighPressure {
		return false
	}
	
	return true
}

// resizeBatch adjusts a batch to match new requirements
func (amm *AdaptiveMemoryManager) resizeBatch(batch *VectorBatch, schema *Schema, newSize int) {
	if batch.Capacity < newSize {
		// Need to reallocate - this is expensive, try to avoid
		*batch = *NewVectorBatch(schema, newSize)
	} else {
		// Can reuse existing batch
		batch.RowCount = 0
		batch.Reset()
	}
}

// freeMemory attempts to free memory by cleaning up unused batches
func (amm *AdaptiveMemoryManager) freeMemory() {
	// Force garbage collection
	runtime.GC()
	amm.memoryStats.GCCount++
	amm.lastGCTime = time.Now()
	
	// Clean up unused batches (this is a simplified version)
	now := time.Now()
	for batch, allocation := range amm.allocatedBatches {
		if !allocation.InUse && now.Sub(allocation.LastAccessed) > 5*time.Minute {
			sizeMB := allocation.SizeBytes / (1024 * 1024)
			amm.currentMemoryMB -= sizeMB
			delete(amm.allocatedBatches, batch)
		}
	}
}

// adaptiveMonitor runs in the background to adjust batch sizes
func (amm *AdaptiveMemoryManager) adaptiveMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		amm.mu.Lock()
		
		// Update memory pressure
		amm.updateMemoryPressure()
		
		// Trigger GC if memory pressure is high and it's been a while
		if amm.memoryPressure > amm.config.GCThreshold && time.Since(amm.lastGCTime) > 30*time.Second {
			go func() {
				runtime.GC()
				amm.mu.Lock()
				amm.memoryStats.GCCount++
				amm.lastGCTime = time.Now()
				amm.mu.Unlock()
			}()
		}
		
		amm.mu.Unlock()
	}
}

// VectorBatchPool implementation

// NewVectorBatchPool creates a new vector batch pool
func NewVectorBatchPool(batchSize, maxSize int) *VectorBatchPool {
	return &VectorBatchPool{
		batches:   make([]*VectorBatch, 0, maxSize),
		batchSize: batchSize,
		maxSize:   maxSize,
	}
}

// Get retrieves a batch from the pool
func (pool *VectorBatchPool) Get() *VectorBatch {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	if len(pool.batches) > 0 {
		batch := pool.batches[len(pool.batches)-1]
		pool.batches = pool.batches[:len(pool.batches)-1]
		pool.hits++
		return batch
	}
	
	pool.misses++
	return nil
}

// Put returns a batch to the pool
func (pool *VectorBatchPool) Put(batch *VectorBatch) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	if len(pool.batches) < pool.maxSize {
		batch.Reset()
		pool.batches = append(pool.batches, batch)
	}
}

// Statistics returns pool statistics
func (pool *VectorBatchPool) Statistics() (hits, misses int64, hitRate float64) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	
	total := pool.hits + pool.misses
	if total > 0 {
		hitRate = float64(pool.hits) / float64(total)
	}
	
	return pool.hits, pool.misses, hitRate
}

// Memory compression utilities (simplified implementation)

// CompressedVector represents a compressed vector for memory efficiency
type CompressedVector struct {
	OriginalType   DataType
	OriginalLength int
	CompressedData []byte
	CompressionType CompressionType
	Metadata       map[string]interface{}
}

// CompressionType defines compression algorithms
type CompressionType int

const (
	NoCompression CompressionType = iota
	RunLengthEncoding
	DictionaryEncoding
	DeltaEncoding
	BitPacking
)

// CompressVector compresses a vector to save memory
func CompressVector(vector *Vector) (*CompressedVector, error) {
	if vector.Length == 0 {
		return nil, fmt.Errorf("cannot compress empty vector")
	}
	
	// Choose compression based on data type and patterns
	compressionType := ChooseCompressionType(vector)
	
	switch compressionType {
	case RunLengthEncoding:
		return compressRunLength(vector)
	case DictionaryEncoding:
		return compressDictionary(vector)
	case DeltaEncoding:
		return compressDelta(vector)
	default:
		return compressNone(vector)
	}
}

// DecompressVector decompresses a vector back to its original form
func DecompressVector(compressed *CompressedVector) (*Vector, error) {
	switch compressed.CompressionType {
	case RunLengthEncoding:
		return decompressRunLength(compressed)
	case DictionaryEncoding:
		return decompressDictionary(compressed)
	case DeltaEncoding:
		return decompressDelta(compressed)
	default:
		return decompressNone(compressed)
	}
}

// ChooseCompressionType selects the best compression for a vector
func ChooseCompressionType(vector *Vector) CompressionType {
	// Simplified heuristics for compression selection
	switch vector.DataType {
	case STRING:
		// Check for repeated strings (dictionary encoding opportunity)
		if estimatedCardinality(vector) < vector.Length/4 {
			return DictionaryEncoding
		}
		return RunLengthEncoding
		
	case INT32, INT64:
		// Check for sequential patterns (delta encoding opportunity)
		if isSequential(vector) {
			return DeltaEncoding
		}
		return RunLengthEncoding
		
	default:
		return RunLengthEncoding
	}
}

// Simplified compression implementations (production would use proper algorithms)

func compressRunLength(vector *Vector) (*CompressedVector, error) {
	// Simplified run-length encoding
	compressed := &CompressedVector{
		OriginalType:    vector.DataType,
		OriginalLength:  vector.Length,
		CompressionType: RunLengthEncoding,
		CompressedData:  make([]byte, 0),
		Metadata:        make(map[string]interface{}),
	}
	
	// This would implement actual RLE compression
	// For now, just copy the data (no compression)
	switch vector.DataType {
	case INT64:
		data := vector.Data.([]int64)
		compressed.CompressedData = (*(*[]byte)(unsafe.Pointer(&data)))[:len(data)*8]
	}
	
	return compressed, nil
}

func compressDictionary(vector *Vector) (*CompressedVector, error) {
	// Simplified dictionary encoding
	return compressRunLength(vector) // Fallback for now
}

func compressDelta(vector *Vector) (*CompressedVector, error) {
	// Simplified delta encoding
	return compressRunLength(vector) // Fallback for now
}

func compressNone(vector *Vector) (*CompressedVector, error) {
	// No compression - just wrap the data
	return compressRunLength(vector)
}

func decompressRunLength(compressed *CompressedVector) (*Vector, error) {
	vector := NewVector(compressed.OriginalType, compressed.OriginalLength)
	vector.Length = compressed.OriginalLength
	
	// This would implement actual RLE decompression
	// For now, just copy the data back
	switch compressed.OriginalType {
	case INT64:
		data := (*(*[]int64)(unsafe.Pointer(&compressed.CompressedData)))[:compressed.OriginalLength]
		copy(vector.Data.([]int64), data)
	}
	
	return vector, nil
}

func decompressDictionary(compressed *CompressedVector) (*Vector, error) {
	return decompressRunLength(compressed) // Fallback for now
}

func decompressDelta(compressed *CompressedVector) (*Vector, error) {
	return decompressRunLength(compressed) // Fallback for now
}

func decompressNone(compressed *CompressedVector) (*Vector, error) {
	return decompressRunLength(compressed)
}

// Utility functions

func estimatedCardinality(vector *Vector) int {
	// Simplified cardinality estimation
	// In production, this would use HyperLogLog or similar
	uniqueValues := make(map[interface{}]bool)
	
	for i := 0; i < min(vector.Length, 1000); i++ { // Sample first 1000 values
		if !vector.IsNull(i) {
			switch vector.DataType {
			case STRING:
				if value, ok := vector.GetString(i); ok {
					uniqueValues[value] = true
				}
			case INT64:
				if value, ok := vector.GetInt64(i); ok {
					uniqueValues[value] = true
				}
			}
		}
	}
	
	// Extrapolate to full vector
	sampleSize := min(vector.Length, 1000)
	return len(uniqueValues) * vector.Length / sampleSize
}

func isSequential(vector *Vector) bool {
	// Check if vector contains sequential values (good for delta encoding)
	if vector.Length < 2 {
		return false
	}
	
	switch vector.DataType {
	case INT64:
		prev, ok := vector.GetInt64(0)
		if !ok {
			return false
		}
		
		for i := 1; i < min(vector.Length, 100); i++ { // Sample first 100 values
			curr, ok := vector.GetInt64(i)
			if !ok || curr != prev+1 {
				return false
			}
			prev = curr
		}
		return true
	}
	
	return false
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}