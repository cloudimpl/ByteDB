package monitoring

import (
	"bytedb/distributed/communication"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// WorkerMonitor tracks worker-level health, performance, and resource utilization
type WorkerMonitor struct {
	workerID     string
	registry     *MetricsRegistry
	startTime    time.Time
	lastUpdate   time.Time
	mutex        sync.RWMutex
	healthStatus WorkerHealthStatus
}

func NewWorkerMonitor(workerID string) *WorkerMonitor {
	wm := &WorkerMonitor{
		workerID:  workerID,
		registry:  NewMetricsRegistry(), // Each worker has its own registry
		startTime: time.Now(),
		lastUpdate: time.Now(),
		healthStatus: WorkerHealthStatus{
			Status:    "healthy",
			LastCheck: time.Now(),
		},
	}
	
	// Set worker-specific labels for all metrics
	wm.registry.SetLabels("worker_queries_executed", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_query_duration", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_cpu_usage", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_memory_usage", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_active_queries", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_cache_hits", map[string]string{"worker_id": workerID})
	wm.registry.SetLabels("worker_cache_misses", map[string]string{"worker_id": workerID})
	
	// Start background monitoring
	go wm.startBackgroundMonitoring()
	
	return wm
}

type WorkerHealthStatus struct {
	Status     string    `json:"status"` // healthy, degraded, unhealthy, offline
	LastCheck  time.Time `json:"last_check"`
	Message    string    `json:"message,omitempty"`
	Uptime     time.Duration `json:"uptime"`
	Issues     []string  `json:"issues,omitempty"`
}

// WorkerStats provides comprehensive worker statistics
type WorkerStats struct {
	WorkerID            string                 `json:"worker_id"`
	Status              WorkerHealthStatus     `json:"status"`
	Performance         WorkerPerformanceStats `json:"performance"`
	ResourceUtilization WorkerResourceStats    `json:"resource_utilization"`
	QueryStats          WorkerQueryStats       `json:"query_stats"`
	CacheStats          WorkerCacheStats       `json:"cache_stats"`
	NetworkStats        WorkerNetworkStats     `json:"network_stats"`
	Errors              []WorkerError          `json:"recent_errors,omitempty"`
}

type WorkerPerformanceStats struct {
	QueriesPerSecond    float64       `json:"queries_per_second"`
	AverageResponseTime float64       `json:"average_response_time_ms"`
	ThroughputMBps      float64       `json:"throughput_mbps"`
	LatencyP50          float64       `json:"latency_p50_ms"`
	LatencyP95          float64       `json:"latency_p95_ms"`
	LatencyP99          float64       `json:"latency_p99_ms"`
}

type WorkerResourceStats struct {
	CPUUsagePercent    float64 `json:"cpu_usage_percent"`
	MemoryUsageMB      float64 `json:"memory_usage_mb"`
	MemoryUsagePercent float64 `json:"memory_usage_percent"`
	DiskUsageMB        float64 `json:"disk_usage_mb"`
	DiskUsagePercent   float64 `json:"disk_usage_percent"`
	ActiveConnections  int     `json:"active_connections"`
	OpenFileDescriptors int    `json:"open_file_descriptors"`
}

type WorkerQueryStats struct {
	TotalQueries      int64   `json:"total_queries"`
	ActiveQueries     int     `json:"active_queries"`
	SuccessfulQueries int64   `json:"successful_queries"`
	FailedQueries     int64   `json:"failed_queries"`
	SuccessRate       float64 `json:"success_rate_percent"`
}

type WorkerCacheStats struct {
	CacheHits     int64   `json:"cache_hits"`
	CacheMisses   int64   `json:"cache_misses"`
	HitRate       float64 `json:"hit_rate_percent"`
	CacheSizeMB   float64 `json:"cache_size_mb"`
	EvictionCount int64   `json:"eviction_count"`
}

type WorkerNetworkStats struct {
	BytesReceived    int64   `json:"bytes_received"`
	BytesSent        int64   `json:"bytes_sent"`
	RequestsReceived int64   `json:"requests_received"`
	ResponsesSent    int64   `json:"responses_sent"`
	NetworkLatency   float64 `json:"network_latency_ms"`
}

type WorkerError struct {
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error"`
	Query     string    `json:"query,omitempty"`
	Severity  string    `json:"severity"`
}

// Record query execution
func (wm *WorkerMonitor) RecordQueryStart(queryID string) {
	wm.registry.Counter("worker_queries_executed").Inc()
	wm.registry.Gauge("worker_active_queries").Add(1)
	wm.registry.Timer("worker_query_duration").Start()
}

func (wm *WorkerMonitor) RecordQueryEnd(queryID string, success bool, rowsProcessed int, bytesProcessed int64) {
	wm.registry.Gauge("worker_active_queries").Add(-1)
	duration := wm.registry.Timer("worker_query_duration").Stop()
	
	if success {
		wm.registry.Counter("worker_queries_successful").Inc()
	} else {
		wm.registry.Counter("worker_queries_failed").Inc()
	}
	
	wm.registry.Histogram("worker_rows_processed", 
		[]float64{1, 10, 100, 1000, 10000, 100000}).Observe(float64(rowsProcessed))
	
	wm.registry.Histogram("worker_bytes_processed",
		[]float64{1024, 10240, 102400, 1048576, 10485760}).Observe(float64(bytesProcessed))
	
	// Update throughput metrics
	if duration.Milliseconds() > 0 {
		mbps := (float64(bytesProcessed) / 1024 / 1024) / (float64(duration.Milliseconds()) / 1000)
		wm.registry.Gauge("worker_throughput_mbps").Set(mbps)
	}
	
	wm.lastUpdate = time.Now()
}

func (wm *WorkerMonitor) RecordCacheHit() {
	wm.registry.Counter("worker_cache_hits").Inc()
}

func (wm *WorkerMonitor) RecordCacheMiss() {
	wm.registry.Counter("worker_cache_misses").Inc()
}

func (wm *WorkerMonitor) RecordError(err error, query string, severity string) {
	wm.registry.Counter("worker_errors").Inc()
	wm.registry.Counter(fmt.Sprintf("worker_errors_%s", severity)).Inc()
	
	// Store recent errors (would typically use a circular buffer)
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	// This is simplified - in production you'd want a proper error storage mechanism
	if wm.healthStatus.Status == "healthy" && severity == "critical" {
		wm.healthStatus.Status = "degraded"
		wm.healthStatus.Message = "Critical errors detected"
	}
}

func (wm *WorkerMonitor) RecordNetworkActivity(bytesReceived, bytesSent int64) {
	wm.registry.Counter("worker_network_bytes_received").Add(bytesReceived)
	wm.registry.Counter("worker_network_bytes_sent").Add(bytesSent)
	wm.registry.Counter("worker_network_requests").Inc()
}

// Resource monitoring
func (wm *WorkerMonitor) UpdateResourceMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// Memory metrics
	memoryMB := float64(m.Alloc) / 1024 / 1024
	wm.registry.Gauge("worker_memory_usage_mb").Set(memoryMB)
	
	// CPU usage (simplified - in production you'd use proper CPU monitoring)
	activeQueries := wm.registry.Gauge("worker_active_queries").Get()
	cpuUsage := activeQueries * 10 // Simplified heuristic
	if cpuUsage > 100 {
		cpuUsage = 100
	}
	wm.registry.Gauge("worker_cpu_usage_percent").Set(cpuUsage)
	
	// Goroutine count as a proxy for concurrent load
	wm.registry.Gauge("worker_goroutines").Set(float64(runtime.NumGoroutine()))
	
	// Update health status based on resource utilization
	wm.updateHealthStatus(cpuUsage, memoryMB)
}

func (wm *WorkerMonitor) updateHealthStatus(cpuUsage, memoryMB float64) {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()
	
	wm.healthStatus.LastCheck = time.Now()
	wm.healthStatus.Uptime = time.Since(wm.startTime)
	wm.healthStatus.Issues = []string{}
	
	previousStatus := wm.healthStatus.Status
	
	// Determine health status based on resources
	if cpuUsage > 90 || memoryMB > 8192 { // >90% CPU or >8GB memory
		wm.healthStatus.Status = "degraded"
		if cpuUsage > 90 {
			wm.healthStatus.Issues = append(wm.healthStatus.Issues, "High CPU usage")
		}
		if memoryMB > 8192 {
			wm.healthStatus.Issues = append(wm.healthStatus.Issues, "High memory usage")
		}
	} else if cpuUsage > 95 || memoryMB > 12288 { // >95% CPU or >12GB memory
		wm.healthStatus.Status = "unhealthy"
	} else if len(wm.healthStatus.Issues) == 0 {
		wm.healthStatus.Status = "healthy"
		wm.healthStatus.Message = ""
	}
	
	// Log status changes
	if previousStatus != wm.healthStatus.Status {
		fmt.Printf("Worker %s health status changed: %s -> %s\n", 
			wm.workerID, previousStatus, wm.healthStatus.Status)
	}
}

// Background monitoring routine
func (wm *WorkerMonitor) startBackgroundMonitoring() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		wm.UpdateResourceMetrics()
		
		// Check if worker is responsive
		if time.Since(wm.lastUpdate) > 30*time.Second {
			wm.mutex.Lock()
			if wm.healthStatus.Status != "offline" {
				wm.healthStatus.Status = "offline"
				wm.healthStatus.Message = "Worker appears to be unresponsive"
			}
			wm.mutex.Unlock()
		}
	}
}

// Get comprehensive worker statistics
func (wm *WorkerMonitor) GetWorkerStats() WorkerStats {
	wm.mutex.RLock()
	healthStatus := wm.healthStatus
	wm.mutex.RUnlock()
	
	return WorkerStats{
		WorkerID:            wm.workerID,
		Status:              healthStatus,
		Performance:         wm.getPerformanceStats(),
		ResourceUtilization: wm.getResourceStats(),
		QueryStats:          wm.getQueryStats(),
		CacheStats:          wm.getCacheStats(),
		NetworkStats:        wm.getNetworkStats(),
		Errors:              wm.getRecentErrors(),
	}
}

func (wm *WorkerMonitor) getPerformanceStats() WorkerPerformanceStats {
	queryTimer := wm.registry.Timer("worker_query_duration")
	stats := queryTimer.GetStats()
	
	// Calculate QPS (simplified)
	totalQueries := wm.registry.Counter("worker_queries_executed").Get()
	uptimeSeconds := time.Since(wm.startTime).Seconds()
	qps := 0.0
	if uptimeSeconds > 0 {
		qps = float64(totalQueries) / uptimeSeconds
	}
	
	return WorkerPerformanceStats{
		QueriesPerSecond:    qps,
		AverageResponseTime: stats.Mean,
		ThroughputMBps:      wm.registry.Gauge("worker_throughput_mbps").Get(),
		LatencyP50:          wm.calculatePercentile(stats, 50),
		LatencyP95:          wm.calculatePercentile(stats, 95),
		LatencyP99:          wm.calculatePercentile(stats, 99),
	}
}

func (wm *WorkerMonitor) getResourceStats() WorkerResourceStats {
	return WorkerResourceStats{
		CPUUsagePercent:    wm.registry.Gauge("worker_cpu_usage_percent").Get(),
		MemoryUsageMB:      wm.registry.Gauge("worker_memory_usage_mb").Get(),
		MemoryUsagePercent: (wm.registry.Gauge("worker_memory_usage_mb").Get() / 16384) * 100, // Assume 16GB total
		ActiveConnections:  int(wm.registry.Gauge("worker_active_queries").Get()),
		OpenFileDescriptors: int(wm.registry.Gauge("worker_goroutines").Get()),
	}
}

func (wm *WorkerMonitor) getQueryStats() WorkerQueryStats {
	total := wm.registry.Counter("worker_queries_executed").Get()
	successful := wm.registry.Counter("worker_queries_successful").Get()
	failed := wm.registry.Counter("worker_queries_failed").Get()
	
	successRate := 0.0
	if total > 0 {
		successRate = (float64(successful) / float64(total)) * 100
	}
	
	return WorkerQueryStats{
		TotalQueries:      total,
		ActiveQueries:     int(wm.registry.Gauge("worker_active_queries").Get()),
		SuccessfulQueries: successful,
		FailedQueries:     failed,
		SuccessRate:       successRate,
	}
}

func (wm *WorkerMonitor) getCacheStats() WorkerCacheStats {
	hits := wm.registry.Counter("worker_cache_hits").Get()
	misses := wm.registry.Counter("worker_cache_misses").Get()
	total := hits + misses
	
	hitRate := 0.0
	if total > 0 {
		hitRate = (float64(hits) / float64(total)) * 100
	}
	
	return WorkerCacheStats{
		CacheHits:     hits,
		CacheMisses:   misses,
		HitRate:       hitRate,
		CacheSizeMB:   wm.registry.Gauge("worker_cache_size_mb").Get(),
		EvictionCount: wm.registry.Counter("worker_cache_evictions").Get(),
	}
}

func (wm *WorkerMonitor) getNetworkStats() WorkerNetworkStats {
	return WorkerNetworkStats{
		BytesReceived:    wm.registry.Counter("worker_network_bytes_received").Get(),
		BytesSent:        wm.registry.Counter("worker_network_bytes_sent").Get(),
		RequestsReceived: wm.registry.Counter("worker_network_requests").Get(),
		ResponsesSent:    wm.registry.Counter("worker_network_requests").Get(), // Simplified
		NetworkLatency:   wm.registry.Gauge("worker_network_latency_ms").Get(),
	}
}

func (wm *WorkerMonitor) getRecentErrors() []WorkerError {
	// This would typically be implemented with a circular buffer
	// For now, return empty slice
	return []WorkerError{}
}

func (wm *WorkerMonitor) calculatePercentile(stats HistogramStats, percentile float64) float64 {
	// Simplified percentile calculation
	// In production, you'd want a proper percentile implementation
	if percentile <= 50 {
		return stats.Mean * 0.8
	} else if percentile <= 95 {
		return stats.Mean * 1.5
	} else {
		return stats.Mean * 2.0
	}
}

// Integration with communication layer
func (wm *WorkerMonitor) ToWorkerStatus() *communication.WorkerStatus {
	stats := wm.getQueryStats()
	resources := wm.getResourceStats()
	
	wm.mutex.RLock()
	healthStatus := wm.healthStatus
	wm.mutex.RUnlock()
	
	return &communication.WorkerStatus{
		ID:            wm.workerID,
		Status:        healthStatus.Status,
		ActiveQueries: stats.ActiveQueries,
		CPUUsage:      resources.CPUUsagePercent,
		MemoryUsage:   int(resources.MemoryUsageMB),
		LastHeartbeat: healthStatus.LastCheck,
	}
}

// Performance alerting
func (wm *WorkerMonitor) CheckPerformanceAlerts() []PerformanceAlert {
	var alerts []PerformanceAlert
	
	stats := wm.GetWorkerStats()
	
	// High CPU usage alert
	if stats.ResourceUtilization.CPUUsagePercent > 85 {
		alerts = append(alerts, PerformanceAlert{
			WorkerID:  wm.workerID,
			Type:      "high_cpu",
			Severity:  "warning",
			Message:   fmt.Sprintf("High CPU usage: %.1f%%", stats.ResourceUtilization.CPUUsagePercent),
			Timestamp: time.Now(),
		})
	}
	
	// High memory usage alert
	if stats.ResourceUtilization.MemoryUsagePercent > 80 {
		alerts = append(alerts, PerformanceAlert{
			WorkerID:  wm.workerID,
			Type:      "high_memory",
			Severity:  "warning",
			Message:   fmt.Sprintf("High memory usage: %.1f%%", stats.ResourceUtilization.MemoryUsagePercent),
			Timestamp: time.Now(),
		})
	}
	
	// Low cache hit rate alert
	if stats.CacheStats.HitRate < 50 && stats.CacheStats.CacheHits+stats.CacheStats.CacheMisses > 100 {
		alerts = append(alerts, PerformanceAlert{
			WorkerID:  wm.workerID,
			Type:      "low_cache_hit_rate",
			Severity:  "info",
			Message:   fmt.Sprintf("Low cache hit rate: %.1f%%", stats.CacheStats.HitRate),
			Timestamp: time.Now(),
		})
	}
	
	// High query failure rate alert
	if stats.QueryStats.SuccessRate < 95 && stats.QueryStats.TotalQueries > 10 {
		alerts = append(alerts, PerformanceAlert{
			WorkerID:  wm.workerID,
			Type:      "high_failure_rate",
			Severity:  "critical",
			Message:   fmt.Sprintf("High query failure rate: %.1f%%", 100-stats.QueryStats.SuccessRate),
			Timestamp: time.Now(),
		})
	}
	
	return alerts
}

type PerformanceAlert struct {
	WorkerID  string    `json:"worker_id"`
	Type      string    `json:"type"`
	Severity  string    `json:"severity"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}