package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TableStatistics contains statistics for a single table
type TableStatistics struct {
	TableName   string                       `json:"table_name"`
	RowCount    int64                        `json:"row_count"`
	SizeBytes   int64                        `json:"size_bytes"`
	LastUpdated time.Time                    `json:"last_updated"`
	ColumnStats map[string]*ColumnStatistics `json:"column_stats"`
}

// ColumnStatistics contains statistics for a single column
type ColumnStatistics struct {
	ColumnName    string      `json:"column_name"`
	DataType      string      `json:"data_type"`
	DistinctCount int64       `json:"distinct_count"`
	NullCount     int64       `json:"null_count"`
	MinValue      interface{} `json:"min_value"`
	MaxValue      interface{} `json:"max_value"`
	AvgLength     float64     `json:"avg_length"` // For string columns
	Histogram     []Bucket    `json:"histogram"`  // Distribution histogram
}

// Bucket represents a histogram bucket
type Bucket struct {
	LowerBound interface{} `json:"lower_bound"`
	UpperBound interface{} `json:"upper_bound"`
	Frequency  int64       `json:"frequency"`
}

// StatsCollector collects and manages table statistics
type StatsCollector struct {
	dataPath  string
	statsPath string
	cache     map[string]*TableStatistics
	cacheMu   sync.RWMutex
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		cache: make(map[string]*TableStatistics),
	}
}

// Initialize sets up the stats collector with data paths
func (sc *StatsCollector) Initialize(dataPath string) {
	sc.dataPath = dataPath
	sc.statsPath = filepath.Join(dataPath, ".stats")

	// Create stats directory if it doesn't exist
	os.MkdirAll(sc.statsPath, 0755)

	// Load existing statistics
	sc.loadAllStats()
}

// CollectTableStats collects statistics for a specific table
func (sc *StatsCollector) CollectTableStats(tableName string) (*TableStatistics, error) {
	reader, err := NewParquetReader(filepath.Join(sc.dataPath, tableName+".parquet"))
	if err != nil {
		return nil, fmt.Errorf("failed to open table %s: %w", tableName, err)
	}
	defer reader.Close()

	stats := &TableStatistics{
		TableName:   tableName,
		LastUpdated: time.Now(),
		ColumnStats: make(map[string]*ColumnStatistics),
	}

	// Get file info for size
	fileInfo, err := os.Stat(filepath.Join(sc.dataPath, tableName+".parquet"))
	if err == nil {
		stats.SizeBytes = fileInfo.Size()
	}

	// Read all rows to collect statistics
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	stats.RowCount = int64(len(rows))

	// Collect column statistics
	columns := reader.GetColumnNames()
	for _, col := range columns {
		colStats := sc.collectColumnStats(col, rows)
		stats.ColumnStats[col] = colStats
	}

	// Cache and persist statistics
	sc.cacheMu.Lock()
	sc.cache[tableName] = stats
	sc.cacheMu.Unlock()

	// Save to disk
	if err := sc.saveStats(tableName, stats); err != nil {
		// Log error but don't fail
		fmt.Printf("Warning: failed to save statistics for %s: %v\n", tableName, err)
	}

	return stats, nil
}

// collectColumnStats collects statistics for a single column
func (sc *StatsCollector) collectColumnStats(column string, rows []Row) *ColumnStatistics {
	stats := &ColumnStatistics{
		ColumnName: column,
	}

	// Track distinct values and other metrics
	distinctValues := make(map[interface{}]int64)
	var totalLength int64
	var nonNullCount int64

	for _, row := range rows {
		value, exists := row[column]
		if !exists || value == nil {
			stats.NullCount++
			continue
		}

		nonNullCount++
		distinctValues[value]++

		// Update min/max
		if stats.MinValue == nil || sc.compareValues(value, stats.MinValue) < 0 {
			stats.MinValue = value
		}
		if stats.MaxValue == nil || sc.compareValues(value, stats.MaxValue) > 0 {
			stats.MaxValue = value
		}

		// Track length for strings
		if strVal, ok := value.(string); ok {
			totalLength += int64(len(strVal))
			if stats.DataType == "" {
				stats.DataType = "string"
			}
		} else if _, ok := value.(int); ok {
			if stats.DataType == "" {
				stats.DataType = "int"
			}
		} else if _, ok := value.(float64); ok {
			if stats.DataType == "" {
				stats.DataType = "float"
			}
		}
	}

	stats.DistinctCount = int64(len(distinctValues))

	if nonNullCount > 0 && stats.DataType == "string" {
		stats.AvgLength = float64(totalLength) / float64(nonNullCount)
	}

	// Create histogram for numeric columns
	if stats.DataType == "int" || stats.DataType == "float" {
		stats.Histogram = sc.createHistogram(distinctValues, 10) // 10 buckets
	}

	return stats
}

// createHistogram creates a histogram from value frequencies
func (sc *StatsCollector) createHistogram(values map[interface{}]int64, bucketCount int) []Bucket {
	if len(values) == 0 || bucketCount <= 0 {
		return nil
	}

	// For simplicity, create equal-width buckets for numeric values
	// In a real implementation, this would be more sophisticated
	var buckets []Bucket

	// Find min and max numeric values
	var minVal, maxVal float64
	first := true
	for val := range values {
		numVal := sc.toFloat64(val)
		if first {
			minVal = numVal
			maxVal = numVal
			first = false
		} else {
			if numVal < minVal {
				minVal = numVal
			}
			if numVal > maxVal {
				maxVal = numVal
			}
		}
	}

	// Create equal-width buckets
	width := (maxVal - minVal) / float64(bucketCount)
	for i := 0; i < bucketCount; i++ {
		lower := minVal + float64(i)*width
		upper := minVal + float64(i+1)*width
		if i == bucketCount-1 {
			upper = maxVal
		}

		bucket := Bucket{
			LowerBound: lower,
			UpperBound: upper,
			Frequency:  0,
		}

		// Count values in this bucket
		for val, freq := range values {
			numVal := sc.toFloat64(val)
			if numVal >= lower && numVal <= upper {
				bucket.Frequency += freq
			}
		}

		if bucket.Frequency > 0 {
			buckets = append(buckets, bucket)
		}
	}

	return buckets
}

// GetTableStats retrieves statistics for a table
func (sc *StatsCollector) GetTableStats(tableName string) *TableStatistics {
	sc.cacheMu.RLock()
	stats, exists := sc.cache[tableName]
	sc.cacheMu.RUnlock()

	if exists {
		return stats
	}

	// Try to load from disk
	if loadedStats, err := sc.loadStats(tableName); err == nil {
		sc.cacheMu.Lock()
		sc.cache[tableName] = loadedStats
		sc.cacheMu.Unlock()
		return loadedStats
	}

	// Collect fresh statistics
	if freshStats, err := sc.CollectTableStats(tableName); err == nil {
		return freshStats
	}

	return nil
}

// RefreshAllStats refreshes statistics for all tables
func (sc *StatsCollector) RefreshAllStats() error {
	files, err := os.ReadDir(sc.dataPath)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".parquet" {
			tableName := file.Name()[:len(file.Name())-8] // Remove .parquet
			if _, err := sc.CollectTableStats(tableName); err != nil {
				fmt.Printf("Warning: failed to collect stats for %s: %v\n", tableName, err)
			}
		}
	}

	return nil
}

// saveStats saves statistics to disk
func (sc *StatsCollector) saveStats(tableName string, stats *TableStatistics) error {
	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal stats: %w", err)
	}

	statsFile := filepath.Join(sc.statsPath, tableName+".stats.json")
	return os.WriteFile(statsFile, data, 0644)
}

// loadStats loads statistics from disk
func (sc *StatsCollector) loadStats(tableName string) (*TableStatistics, error) {
	statsFile := filepath.Join(sc.statsPath, tableName+".stats.json")
	data, err := os.ReadFile(statsFile)
	if err != nil {
		return nil, err
	}

	var stats TableStatistics
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, err
	}

	return &stats, nil
}

// loadAllStats loads all available statistics
func (sc *StatsCollector) loadAllStats() {
	if sc.statsPath == "" {
		return
	}

	files, err := os.ReadDir(sc.statsPath)
	if err != nil {
		return
	}

	sc.cacheMu.Lock()
	defer sc.cacheMu.Unlock()

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			tableName := file.Name()[:len(file.Name())-11] // Remove .stats.json
			if stats, err := sc.loadStats(tableName); err == nil {
				sc.cache[tableName] = stats
			}
		}
	}
}

// compareValues compares two values
func (sc *StatsCollector) compareValues(a, b interface{}) int {
	// Convert to float64 for numeric comparison
	aFloat := sc.toFloat64(a)
	bFloat := sc.toFloat64(b)

	if aFloat < bFloat {
		return -1
	} else if aFloat > bFloat {
		return 1
	}
	return 0
}

// toFloat64 converts a value to float64
func (sc *StatsCollector) toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		// For strings, use length as numeric value
		return float64(len(v))
	default:
		return 0
	}
}
