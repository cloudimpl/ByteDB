package worker

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

// Worker handles distributed query fragment execution
type Worker struct {
	id           string
	dataPath     string
	engine       *core.QueryEngine
	status       string
	activeQueries int
	queryMutex   sync.RWMutex
	startTime    time.Time
	lastActivity time.Time
	resources    communication.WorkerResources
}

// NewWorker creates a new worker instance
func NewWorker(id, dataPath string) *Worker {
	tracer := core.GetTracer()
	tracer.Info(core.TraceComponentWorker, "Initializing worker", core.TraceContext(
		"workerID", id,
		"dataPath", dataPath,
	))
	
	// Create query engine for this worker's data
	engine := core.NewQueryEngine(dataPath)
	
	// Check if there's a worker-specific table mapping configuration
	workerConfigPath := fmt.Sprintf("%s/table_mappings.json", dataPath)
	if _, err := os.Stat(workerConfigPath); err == nil {
		if err := engine.LoadTableMappings(workerConfigPath); err != nil {
			tracer.Error(core.TraceComponentWorker, "Failed to load table mappings", core.TraceContext(
				"workerID", id,
				"configPath", workerConfigPath,
				"error", err.Error(),
			))
		} else {
			tracer.Info(core.TraceComponentWorker, "Loaded table mappings", core.TraceContext(
				"workerID", id,
				"configPath", workerConfigPath,
			))
		}
	}
	
	w := &Worker{
		id:           id,
		dataPath:     dataPath,
		engine:       engine,
		status:       "active",
		startTime:    time.Now(),
		lastActivity: time.Now(),
		resources: communication.WorkerResources{
			CPUCores:    runtime.NumCPU(),
			MemoryMB:    1024, // Default, should be configurable
			DiskSpaceGB: getDiskSpace(dataPath),
		},
	}
	
	return w
}

// ExecuteFragment implements the WorkerService interface
func (w *Worker) ExecuteFragment(ctx context.Context, fragment *communication.QueryFragment) (*communication.FragmentResult, error) {
	startTime := time.Now()
	tracer := core.GetTracer()
	
	w.queryMutex.Lock()
	w.activeQueries++
	w.lastActivity = time.Now()
	w.queryMutex.Unlock()
	
	defer func() {
		w.queryMutex.Lock()
		w.activeQueries--
		w.queryMutex.Unlock()
	}()
	
	tracer.Info(core.TraceComponentFragment, "Executing query fragment", core.TraceContext(
		"workerID", w.id,
		"fragmentID", fragment.ID,
		"sql", fragment.SQL,
		"activeQueries", w.activeQueries,
	))
	
	// Execute the fragment using the core query engine
	result, err := w.executeQueryFragment(ctx, fragment)
	if err != nil {
		tracer.Error(core.TraceComponentFragment, "Fragment execution failed", core.TraceContext(
			"workerID", w.id,
			"fragmentID", fragment.ID,
			"error", err.Error(),
			"duration", time.Since(startTime),
		))
		return &communication.FragmentResult{
			FragmentID: fragment.ID,
			Error:      err.Error(),
			Stats: communication.ExecutionStats{
				Duration: time.Since(startTime),
			},
		}, nil
	}
	
	duration := time.Since(startTime)
	stats := communication.ExecutionStats{
		Duration:      duration,
		RowsProcessed: result.Count,
		BytesRead:     w.estimateBytesRead(result.Count), // Estimated
	}
	
	// Get cache stats if available
	cacheStats := w.engine.GetCacheStats()
	stats.CacheHits = int(cacheStats.Hits)
	stats.CacheMisses = int(cacheStats.Misses)
	
	fragmentResult := &communication.FragmentResult{
		FragmentID: fragment.ID,
		Rows:       result.Rows,
		Columns:    result.Columns,
		Count:      result.Count,
		Stats:      stats,
	}
	
	tracer.Info(core.TraceComponentFragment, "Fragment execution completed", core.TraceContext(
		"workerID", w.id,
		"fragmentID", fragment.ID,
		"duration", duration,
		"rowsReturned", result.Count,
		"bytesRead", stats.BytesRead,
		"cacheHits", stats.CacheHits,
		"cacheMisses", stats.CacheMisses,
	))
	
	return fragmentResult, nil
}

// GetStatus implements the WorkerService interface
func (w *Worker) GetStatus(ctx context.Context) (*communication.WorkerStatus, error) {
	w.queryMutex.RLock()
	defer w.queryMutex.RUnlock()
	
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	status := &communication.WorkerStatus{
		ID:            w.id,
		Status:        w.status,
		ActiveQueries: w.activeQueries,
		CPUUsage:      w.getCPUUsage(), // Simplified CPU usage
		MemoryUsage:   int(memStats.Alloc / 1024 / 1024), // MB
		LastHeartbeat: w.lastActivity,
	}
	
	return status, nil
}

// GetCacheStats implements the WorkerService interface
func (w *Worker) GetCacheStats(ctx context.Context) (*communication.CacheStats, error) {
	if w.engine == nil {
		return &communication.CacheStats{}, nil
	}
	
	engineStats := w.engine.GetCacheStats()
	
	hitRate := 0.0
	total := engineStats.Hits + engineStats.Misses
	if total > 0 {
		hitRate = float64(engineStats.Hits) / float64(total)
	}
	
	stats := &communication.CacheStats{
		Hits:        int(engineStats.Hits),
		Misses:      int(engineStats.Misses),
		HitRate:     hitRate,
		MemoryUsage: engineStats.CurrentSize,
		EntryCount:  int(engineStats.TotalQueries),
	}
	
	return stats, nil
}

// Health implements the WorkerService interface
func (w *Worker) Health(ctx context.Context) error {
	// Basic health check
	if w.status != "active" {
		return fmt.Errorf("worker %s is not active (status: %s)", w.id, w.status)
	}
	
	// Check if data directory is accessible
	if _, err := os.Stat(w.dataPath); os.IsNotExist(err) {
		return fmt.Errorf("data directory %s not accessible", w.dataPath)
	}
	
	// Check if engine is working
	if w.engine == nil {
		return fmt.Errorf("query engine not initialized")
	}
	
	return nil
}

// Shutdown implements the WorkerService interface
func (w *Worker) Shutdown(ctx context.Context) error {
	log.Printf("Worker %s: Shutting down...", w.id)
	
	w.queryMutex.Lock()
	w.status = "shutting_down"
	w.queryMutex.Unlock()
	
	// Wait for active queries to complete (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			log.Printf("Worker %s: Shutdown timeout, force closing", w.id)
			break
		case <-ticker.C:
			w.queryMutex.RLock()
			active := w.activeQueries
			w.queryMutex.RUnlock()
			
			if active == 0 {
				break
			}
		}
		break
	}
	
	// Close the query engine
	if w.engine != nil {
		w.engine.Close()
	}
	
	w.queryMutex.Lock()
	w.status = "shutdown"
	w.queryMutex.Unlock()
	
	log.Printf("Worker %s: Shutdown complete", w.id)
	return nil
}

// executeQueryFragment executes a query fragment using the core engine
func (w *Worker) executeQueryFragment(ctx context.Context, fragment *communication.QueryFragment) (*core.QueryResult, error) {
	// Handle different fragment types
	switch fragment.FragmentType {
	case communication.FragmentTypeScan:
		return w.executeScanFragment(ctx, fragment)
	case communication.FragmentTypeAggregate:
		return w.executeAggregateFragment(ctx, fragment)
	case communication.FragmentTypeFilter:
		return w.executeFilterFragment(ctx, fragment)
	case communication.FragmentTypeJoin:
		return w.executeJoinFragment(ctx, fragment)
	default:
		// Fallback to SQL execution
		return w.engine.Execute(fragment.SQL)
	}
}

// executeScanFragment executes a scan fragment
func (w *Worker) executeScanFragment(ctx context.Context, fragment *communication.QueryFragment) (*core.QueryResult, error) {
	// Use the core engine to execute the SQL
	// In a more sophisticated implementation, we might construct the query programmatically
	return w.engine.Execute(fragment.SQL)
}

// executeAggregateFragment executes an aggregate fragment
func (w *Worker) executeAggregateFragment(ctx context.Context, fragment *communication.QueryFragment) (*core.QueryResult, error) {
	// For aggregate fragments, we execute the SQL which should include GROUP BY and aggregation functions
	return w.engine.Execute(fragment.SQL)
}

// executeFilterFragment executes a filter fragment
func (w *Worker) executeFilterFragment(ctx context.Context, fragment *communication.QueryFragment) (*core.QueryResult, error) {
	// Execute filter operations
	return w.engine.Execute(fragment.SQL)
}

// executeJoinFragment executes a join fragment
func (w *Worker) executeJoinFragment(ctx context.Context, fragment *communication.QueryFragment) (*core.QueryResult, error) {
	// Execute join operations
	return w.engine.Execute(fragment.SQL)
}

// Helper methods

func (w *Worker) getCPUUsage() float64 {
	// This is a simplified CPU usage calculation
	// In a real implementation, you might want to use a more sophisticated method
	w.queryMutex.RLock()
	active := w.activeQueries
	w.queryMutex.RUnlock()
	
	// Simple heuristic: more active queries = higher CPU usage
	usage := float64(active) * 10.0 // Assume each query uses 10% CPU
	if usage > 100.0 {
		usage = 100.0
	}
	
	return usage
}

func (w *Worker) estimateBytesRead(rowCount int) int64 {
	// Estimate bytes read based on row count
	// This is a rough estimate - in a real implementation you'd want actual metrics
	estimatedBytesPerRow := int64(100) // Assume ~100 bytes per row
	return int64(rowCount) * estimatedBytesPerRow
}

func getDiskSpace(path string) int64 {
	// Get available disk space for the data path
	// This is a simplified implementation
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		// For now, return a default value
		// In a real implementation, you'd use syscalls to get actual disk space
		return 100 * 1024 // 100GB default
	}
	return 0
}

// GetID returns the worker ID
func (w *Worker) GetID() string {
	return w.id
}

// GetDataPath returns the worker's data path
func (w *Worker) GetDataPath() string {
	return w.dataPath
}

// RegisterTable registers a table mapping for this worker
func (w *Worker) RegisterTable(tableName string, filePath string) error {
	return w.engine.RegisterTable(tableName, filePath)
}

// LoadTableMappings loads table mappings from a configuration file
func (w *Worker) LoadTableMappings(configPath string) error {
	return w.engine.LoadTableMappings(configPath)
}

// GetTableRegistry returns the worker's table registry
func (w *Worker) GetTableRegistry() *core.TableRegistry {
	return w.engine.GetTableRegistry()
}