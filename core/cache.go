package core

import (
	"crypto/md5"
	"fmt"
	"sync"
	"time"
)

// CacheEntry represents a cached query result with metadata
type CacheEntry struct {
	Result    *QueryResult
	CreatedAt time.Time
	TTL       time.Duration
	Size      int64 // Estimated memory size in bytes
}

// CacheConfig holds configuration for the query cache
type CacheConfig struct {
	MaxMemoryMB int           // Maximum memory usage in MB
	DefaultTTL  time.Duration // Default TTL for cached entries
	Enabled     bool          // Whether caching is enabled
}

// QueryCache implements an LRU cache with TTL and memory management
type QueryCache struct {
	config   CacheConfig
	entries  map[string]*CacheEntry
	keyOrder []string // For LRU eviction
	mutex    sync.RWMutex
	stats    CacheStats
}

// CacheStats tracks cache performance metrics
type CacheStats struct {
	Hits         int64
	Misses       int64
	Evictions    int64
	TotalQueries int64
	CurrentSize  int64 // Current memory usage in bytes
}

// NewQueryCache creates a new query cache with the given configuration
func NewQueryCache(config CacheConfig) *QueryCache {
	return &QueryCache{
		config:   config,
		entries:  make(map[string]*CacheEntry),
		keyOrder: make([]string, 0),
		stats:    CacheStats{},
	}
}

// Get retrieves a cached result if it exists and is not expired
func (qc *QueryCache) Get(sql string) (*QueryResult, bool) {
	if !qc.config.Enabled {
		return nil, false
	}

	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	qc.stats.TotalQueries++
	key := qc.generateCacheKey(sql)

	entry, exists := qc.entries[key]
	if !exists {
		qc.stats.Misses++
		return nil, false
	}

	// Check if entry has expired
	if qc.isExpired(entry) {
		qc.removeEntry(key)
		qc.stats.Misses++
		return nil, false
	}

	// Move to end for LRU (most recently used)
	qc.moveToEnd(key)
	qc.stats.Hits++

	return entry.Result, true
}

// Put stores a query result in the cache
func (qc *QueryCache) Put(sql string, result *QueryResult) {
	if !qc.config.Enabled {
		return
	}

	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	key := qc.generateCacheKey(sql)
	entry := &CacheEntry{
		Result:    result,
		CreatedAt: time.Now(),
		TTL:       qc.config.DefaultTTL,
		Size:      qc.estimateSize(result),
	}

	// Remove existing entry if it exists
	if _, exists := qc.entries[key]; exists {
		qc.removeEntry(key)
	}

	// Add new entry
	qc.entries[key] = entry
	qc.keyOrder = append(qc.keyOrder, key)
	qc.stats.CurrentSize += entry.Size

	// Enforce memory limits
	qc.evictIfNeeded()
}

// generateCacheKey creates a unique key for the SQL query
func (qc *QueryCache) generateCacheKey(sql string) string {
	// Use MD5 hash of normalized SQL as cache key
	// In production, you might want to normalize the SQL (remove extra spaces, etc.)
	hash := md5.Sum([]byte(sql))
	return fmt.Sprintf("%x", hash)
}

// isExpired checks if a cache entry has exceeded its TTL
func (qc *QueryCache) isExpired(entry *CacheEntry) bool {
	if entry.TTL <= 0 {
		return false // No expiration
	}
	return time.Since(entry.CreatedAt) > entry.TTL
}

// moveToEnd moves a key to the end of the order slice (most recently used)
func (qc *QueryCache) moveToEnd(key string) {
	// Find and remove the key from its current position
	for i, k := range qc.keyOrder {
		if k == key {
			qc.keyOrder = append(qc.keyOrder[:i], qc.keyOrder[i+1:]...)
			break
		}
	}
	// Add to the end
	qc.keyOrder = append(qc.keyOrder, key)
}

// removeEntry removes an entry from the cache
func (qc *QueryCache) removeEntry(key string) {
	if entry, exists := qc.entries[key]; exists {
		qc.stats.CurrentSize -= entry.Size
		delete(qc.entries, key)

		// Remove from order slice
		for i, k := range qc.keyOrder {
			if k == key {
				qc.keyOrder = append(qc.keyOrder[:i], qc.keyOrder[i+1:]...)
				break
			}
		}
	}
}

// evictIfNeeded removes old entries if memory limit is exceeded
func (qc *QueryCache) evictIfNeeded() {
	maxBytes := int64(qc.config.MaxMemoryMB) * 1024 * 1024

	// Remove expired entries first
	qc.removeExpiredEntries()

	// Then remove LRU entries if still over limit
	for qc.stats.CurrentSize > maxBytes && len(qc.keyOrder) > 0 {
		// Remove the least recently used (first in order)
		keyToEvict := qc.keyOrder[0]
		qc.removeEntry(keyToEvict)
		qc.stats.Evictions++
	}
}

// removeExpiredEntries removes all expired entries from the cache
func (qc *QueryCache) removeExpiredEntries() {
	var keysToRemove []string

	for key, entry := range qc.entries {
		if qc.isExpired(entry) {
			keysToRemove = append(keysToRemove, key)
		}
	}

	for _, key := range keysToRemove {
		qc.removeEntry(key)
	}
}

// Clear removes all entries from the cache
func (qc *QueryCache) Clear() {
	qc.mutex.Lock()
	defer qc.mutex.Unlock()

	qc.entries = make(map[string]*CacheEntry)
	qc.keyOrder = make([]string, 0)
	qc.stats.CurrentSize = 0
}

// GetStats returns current cache statistics
func (qc *QueryCache) GetStats() CacheStats {
	qc.mutex.RLock()
	defer qc.mutex.RUnlock()

	stats := qc.stats
	if stats.TotalQueries > 0 {
		// Add hit rate calculation
		stats.Hits = qc.stats.Hits
		stats.Misses = qc.stats.Misses
	}

	return stats
}

// estimateSize estimates the memory size of a QueryResult
func (qc *QueryCache) estimateSize(result *QueryResult) int64 {
	if result == nil {
		return 0
	}

	size := int64(0)

	// Base struct size
	size += 100 // QueryResult struct overhead

	// Columns slice
	for _, col := range result.Columns {
		size += int64(len(col)) + 16 // string overhead
	}

	// Rows data
	for _, row := range result.Rows {
		size += 50 // Row map overhead
		for key, value := range row {
			size += int64(len(key)) + 16 // key string
			size += qc.estimateValueSize(value)
		}
	}

	// Query string
	size += int64(len(result.Query)) + 16

	// Error string
	size += int64(len(result.Error)) + 16

	return size
}

// estimateValueSize estimates the memory size of an interface{} value
func (qc *QueryCache) estimateValueSize(value interface{}) int64 {
	if value == nil {
		return 8
	}

	switch v := value.(type) {
	case string:
		return int64(len(v)) + 16
	case int, int32, int64, float32, float64:
		return 8
	case bool:
		return 1
	default:
		return 50 // Conservative estimate for unknown types
	}
}

// GetHitRate returns the cache hit rate as a percentage
func (stats *CacheStats) GetHitRate() float64 {
	if stats.TotalQueries == 0 {
		return 0.0
	}
	return (float64(stats.Hits) / float64(stats.TotalQueries)) * 100.0
}

// GetMemoryUsageMB returns current memory usage in MB
func (stats *CacheStats) GetMemoryUsageMB() float64 {
	return float64(stats.CurrentSize) / (1024 * 1024)
}
