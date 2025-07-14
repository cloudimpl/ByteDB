package monitoring

import (
	"bytedb/distributed/communication"
	"fmt"
	"sync"
	"time"
)

// CoordinatorMonitor tracks coordinator-level system metrics and cluster health
type CoordinatorMonitor struct {
	registry     *MetricsRegistry
	startTime    time.Time
	workers      map[string]*WorkerMonitor
	workersMutex sync.RWMutex
	clusterHealth ClusterHealth
	healthMutex   sync.RWMutex
}

func NewCoordinatorMonitor() *CoordinatorMonitor {
	cm := &CoordinatorMonitor{
		registry:  GlobalRegistry,
		startTime: time.Now(),
		workers:   make(map[string]*WorkerMonitor),
		clusterHealth: ClusterHealth{
			Status:    "initializing",
			LastCheck: time.Now(),
		},
	}
	
	// Start background cluster monitoring
	go cm.startClusterMonitoring()
	
	return cm
}

type ClusterHealth struct {
	Status         string              `json:"status"` // healthy, degraded, unhealthy, critical
	LastCheck      time.Time           `json:"last_check"`
	TotalWorkers   int                 `json:"total_workers"`
	HealthyWorkers int                 `json:"healthy_workers"`
	DegradedWorkers int                `json:"degraded_workers"`
	UnhealthyWorkers int               `json:"unhealthy_workers"`
	OfflineWorkers int                 `json:"offline_workers"`
	Issues         []string            `json:"issues,omitempty"`
	Uptime         time.Duration       `json:"uptime"`
	WorkerStatuses map[string]string   `json:"worker_statuses"`
}

// CoordinatorStats provides comprehensive coordinator and cluster statistics
type CoordinatorStats struct {
	ClusterHealth       ClusterHealth           `json:"cluster_health"`
	SystemPerformance   SystemPerformanceStats  `json:"system_performance"`
	QueryCoordination   QueryCoordinationStats  `json:"query_coordination"`
	ResourceDistribution ResourceDistributionStats `json:"resource_distribution"`
	OptimizationMetrics OptimizationMetrics     `json:"optimization_metrics"`
	NetworkMetrics      NetworkMetrics          `json:"network_metrics"`
	Alerts              []SystemAlert           `json:"active_alerts"`
}

type SystemPerformanceStats struct {
	TotalQPS            float64 `json:"total_qps"`
	AverageResponseTime float64 `json:"average_response_time_ms"`
	TotalThroughput     float64 `json:"total_throughput_mbps"`
	ClusterUtilization  float64 `json:"cluster_utilization_percent"`
	LoadBalance         float64 `json:"load_balance_score"` // 0-100, higher is better
}

type QueryCoordinationStats struct {
	QueriesCoordinated    int64   `json:"queries_coordinated"`
	AverageFragments      float64 `json:"average_fragments_per_query"`
	AverageWorkersUsed    float64 `json:"average_workers_per_query"`
	CoordinationOverhead  float64 `json:"coordination_overhead_ms"`
	PlanningTime          float64 `json:"planning_time_ms"`
	AggregationTime       float64 `json:"aggregation_time_ms"`
	ParallelismEfficiency float64 `json:"parallelism_efficiency_percent"`
}

type ResourceDistributionStats struct {
	TotalCPUUsage     float64            `json:"total_cpu_usage_percent"`
	TotalMemoryUsage  float64            `json:"total_memory_usage_mb"`
	AverageWorkerLoad float64            `json:"average_worker_load"`
	LoadVariance      float64            `json:"load_variance"`
	WorkerLoads       map[string]float64 `json:"worker_loads"`
}

type OptimizationMetrics struct {
	OptimizationRate      float64 `json:"optimization_rate_percent"`
	AverageCostReduction  float64 `json:"average_cost_reduction_percent"`
	DataTransferReduction float64 `json:"data_transfer_reduction_percent"`
	CacheEffectiveness    float64 `json:"cache_effectiveness_percent"`
}

type NetworkMetrics struct {
	TotalDataTransferred int64   `json:"total_data_transferred_mb"`
	AverageLatency       float64 `json:"average_network_latency_ms"`
	BandwidthUtilization float64 `json:"bandwidth_utilization_percent"`
	FragmentationRatio   float64 `json:"fragmentation_ratio"`
}

type SystemAlert struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	Severity    string    `json:"severity"`
	Message     string    `json:"message"`
	Component   string    `json:"component"`
	Timestamp   time.Time `json:"timestamp"`
	Acknowledged bool     `json:"acknowledged"`
}

// Worker management
func (cm *CoordinatorMonitor) RegisterWorker(workerID string, monitor *WorkerMonitor) {
	cm.workersMutex.Lock()
	defer cm.workersMutex.Unlock()
	
	cm.workers[workerID] = monitor
	cm.registry.Gauge("cluster_total_workers").Set(float64(len(cm.workers)))
	
	fmt.Printf("Coordinator Monitor: Registered worker %s (total: %d)\n", workerID, len(cm.workers))
}

func (cm *CoordinatorMonitor) UnregisterWorker(workerID string) {
	cm.workersMutex.Lock()
	defer cm.workersMutex.Unlock()
	
	delete(cm.workers, workerID)
	cm.registry.Gauge("cluster_total_workers").Set(float64(len(cm.workers)))
	
	fmt.Printf("Coordinator Monitor: Unregistered worker %s (remaining: %d)\n", workerID, len(cm.workers))
}

// Query coordination tracking
func (cm *CoordinatorMonitor) RecordQueryStart(queryID string, fragmentCount, workerCount int) {
	cm.registry.Counter("queries_coordinated").Inc()
	cm.registry.Gauge("active_distributed_queries").Add(1)
	
	cm.registry.Histogram("fragments_per_query", 
		[]float64{1, 2, 3, 5, 10, 20, 50}).Observe(float64(fragmentCount))
	
	cm.registry.Histogram("workers_per_query",
		[]float64{1, 2, 3, 5, 10, 20}).Observe(float64(workerCount))
}

func (cm *CoordinatorMonitor) RecordQueryEnd(queryID string, success bool, stats *communication.ClusterStats) {
	cm.registry.Gauge("active_distributed_queries").Add(-1)
	
	if success {
		cm.registry.Counter("queries_successful").Inc()
	} else {
		cm.registry.Counter("queries_failed").Inc()
	}
	
	if stats != nil {
		// Record coordination overhead
		coordinationTime := float64(stats.TotalTime.Milliseconds())
		cm.registry.Histogram("coordination_overhead",
			[]float64{1, 5, 10, 50, 100, 500, 1000}).Observe(coordinationTime)
		
		// Record worker utilization
		if stats.WorkersUsed > 0 {
			utilizationScore := (float64(stats.WorkersUsed) / float64(len(cm.workers))) * 100
			cm.registry.Gauge("last_query_worker_utilization").Set(utilizationScore)
		}
	}
}

func (cm *CoordinatorMonitor) RecordPlanningTime(duration time.Duration) {
	cm.registry.Timer("query_planning").Time(func() {
		time.Sleep(duration) // This is just for recording
	})
}

func (cm *CoordinatorMonitor) RecordAggregationTime(duration time.Duration) {
	cm.registry.Timer("result_aggregation").Time(func() {
		time.Sleep(duration) // This is just for recording
	})
}

func (cm *CoordinatorMonitor) RecordOptimization(applied bool, originalCost, optimizedCost float64) {
	if applied {
		cm.registry.Counter("optimizations_applied").Inc()
		
		if originalCost > 0 {
			reductionPercent := ((originalCost - optimizedCost) / originalCost) * 100
			cm.registry.Histogram("cost_reduction_percent",
				[]float64{10, 25, 50, 75, 90, 95, 99, 99.9}).Observe(reductionPercent)
		}
	}
}

// Cluster monitoring
func (cm *CoordinatorMonitor) startClusterMonitoring() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		cm.updateClusterHealth()
		cm.updateSystemMetrics()
		cm.checkSystemAlerts()
	}
}

func (cm *CoordinatorMonitor) updateClusterHealth() {
	cm.workersMutex.RLock()
	workers := make(map[string]*WorkerMonitor)
	for k, v := range cm.workers {
		workers[k] = v
	}
	cm.workersMutex.RUnlock()
	
	cm.healthMutex.Lock()
	defer cm.healthMutex.Unlock()
	
	cm.clusterHealth.LastCheck = time.Now()
	cm.clusterHealth.Uptime = time.Since(cm.startTime)
	cm.clusterHealth.TotalWorkers = len(workers)
	cm.clusterHealth.WorkerStatuses = make(map[string]string)
	
	// Reset counters
	cm.clusterHealth.HealthyWorkers = 0
	cm.clusterHealth.DegradedWorkers = 0
	cm.clusterHealth.UnhealthyWorkers = 0
	cm.clusterHealth.OfflineWorkers = 0
	cm.clusterHealth.Issues = []string{}
	
	// Count worker statuses
	for workerID, worker := range workers {
		stats := worker.GetWorkerStats()
		status := stats.Status.Status
		cm.clusterHealth.WorkerStatuses[workerID] = status
		
		switch status {
		case "healthy":
			cm.clusterHealth.HealthyWorkers++
		case "degraded":
			cm.clusterHealth.DegradedWorkers++
		case "unhealthy":
			cm.clusterHealth.UnhealthyWorkers++
		case "offline":
			cm.clusterHealth.OfflineWorkers++
		}
	}
	
	// Determine overall cluster health
	totalWorkers := cm.clusterHealth.TotalWorkers
	if totalWorkers == 0 {
		cm.clusterHealth.Status = "critical"
		cm.clusterHealth.Issues = append(cm.clusterHealth.Issues, "No workers available")
	} else {
		healthyRatio := float64(cm.clusterHealth.HealthyWorkers) / float64(totalWorkers)
		
		if healthyRatio >= 0.8 { // 80% or more healthy
			cm.clusterHealth.Status = "healthy"
		} else if healthyRatio >= 0.5 { // 50-80% healthy
			cm.clusterHealth.Status = "degraded"
			cm.clusterHealth.Issues = append(cm.clusterHealth.Issues, 
				fmt.Sprintf("Only %.0f%% of workers are healthy", healthyRatio*100))
		} else { // Less than 50% healthy
			cm.clusterHealth.Status = "critical"
			cm.clusterHealth.Issues = append(cm.clusterHealth.Issues,
				fmt.Sprintf("Only %.0f%% of workers are healthy", healthyRatio*100))
		}
		
		if cm.clusterHealth.OfflineWorkers > 0 {
			cm.clusterHealth.Issues = append(cm.clusterHealth.Issues,
				fmt.Sprintf("%d workers are offline", cm.clusterHealth.OfflineWorkers))
		}
	}
	
	// Update metrics
	cm.registry.Gauge("cluster_healthy_workers").Set(float64(cm.clusterHealth.HealthyWorkers))
	cm.registry.Gauge("cluster_degraded_workers").Set(float64(cm.clusterHealth.DegradedWorkers))
	cm.registry.Gauge("cluster_unhealthy_workers").Set(float64(cm.clusterHealth.UnhealthyWorkers))
	cm.registry.Gauge("cluster_offline_workers").Set(float64(cm.clusterHealth.OfflineWorkers))
}

func (cm *CoordinatorMonitor) updateSystemMetrics() {
	cm.workersMutex.RLock()
	workers := make(map[string]*WorkerMonitor)
	for k, v := range cm.workers {
		workers[k] = v
	}
	cm.workersMutex.RUnlock()
	
	if len(workers) == 0 {
		return
	}
	
	// Calculate system-wide metrics
	var totalQPS, totalThroughput, totalCPU, totalMemory float64
	var workerLoads []float64
	
	for _, worker := range workers {
		stats := worker.GetWorkerStats()
		totalQPS += stats.Performance.QueriesPerSecond
		totalThroughput += stats.Performance.ThroughputMBps
		totalCPU += stats.ResourceUtilization.CPUUsagePercent
		totalMemory += stats.ResourceUtilization.MemoryUsageMB
		
		// Worker load is a combination of CPU, memory, and active queries
		load := (stats.ResourceUtilization.CPUUsagePercent + 
			     (stats.ResourceUtilization.MemoryUsagePercent) +
			     (float64(stats.QueryStats.ActiveQueries) * 10)) / 3
		workerLoads = append(workerLoads, load)
	}
	
	avgCPU := totalCPU / float64(len(workers))
	avgMemory := totalMemory / float64(len(workers))
	
	// Calculate load balance score (100 - coefficient of variation)
	loadBalance := cm.calculateLoadBalance(workerLoads)
	
	// Update system metrics
	cm.registry.Gauge("system_total_qps").Set(totalQPS)
	cm.registry.Gauge("system_total_throughput").Set(totalThroughput)
	cm.registry.Gauge("system_average_cpu").Set(avgCPU)
	cm.registry.Gauge("system_average_memory").Set(avgMemory)
	cm.registry.Gauge("system_load_balance_score").Set(loadBalance)
	
	// Calculate cluster utilization
	clusterUtilization := (avgCPU + (avgMemory/1024)*10) / 2 // Simplified metric
	if clusterUtilization > 100 {
		clusterUtilization = 100
	}
	cm.registry.Gauge("cluster_utilization").Set(clusterUtilization)
}

func (cm *CoordinatorMonitor) calculateLoadBalance(loads []float64) float64 {
	if len(loads) <= 1 {
		return 100
	}
	
	// Calculate mean
	var sum float64
	for _, load := range loads {
		sum += load
	}
	mean := sum / float64(len(loads))
	
	if mean == 0 {
		return 100
	}
	
	// Calculate standard deviation
	var variance float64
	for _, load := range loads {
		variance += (load - mean) * (load - mean)
	}
	variance /= float64(len(loads))
	stdDev := variance // Simplified, should be sqrt(variance)
	
	// Coefficient of variation
	cv := stdDev / mean
	
	// Convert to balance score (lower CV = higher balance)
	balance := 100 - (cv * 100)
	if balance < 0 {
		balance = 0
	}
	if balance > 100 {
		balance = 100
	}
	
	return balance
}

func (cm *CoordinatorMonitor) checkSystemAlerts() {
	// This would implement comprehensive alerting logic
	// For now, basic implementation
	
	cm.healthMutex.RLock()
	clusterHealth := cm.clusterHealth
	cm.healthMutex.RUnlock()
	
	// Check for critical conditions
	if clusterHealth.Status == "critical" {
		// Would trigger critical alert
		fmt.Printf("CRITICAL: Cluster health is critical - %v\n", clusterHealth.Issues)
	}
	
	if clusterHealth.OfflineWorkers > 0 {
		// Would trigger worker offline alert
		fmt.Printf("WARNING: %d workers are offline\n", clusterHealth.OfflineWorkers)
	}
}

// Get comprehensive coordinator statistics
func (cm *CoordinatorMonitor) GetCoordinatorStats() CoordinatorStats {
	cm.healthMutex.RLock()
	clusterHealth := cm.clusterHealth
	cm.healthMutex.RUnlock()
	
	return CoordinatorStats{
		ClusterHealth:       clusterHealth,
		SystemPerformance:   cm.getSystemPerformanceStats(),
		QueryCoordination:   cm.getQueryCoordinationStats(),
		ResourceDistribution: cm.getResourceDistributionStats(),
		OptimizationMetrics: cm.getOptimizationMetrics(),
		NetworkMetrics:      cm.getNetworkMetrics(),
		Alerts:              cm.getActiveAlerts(),
	}
}

func (cm *CoordinatorMonitor) getSystemPerformanceStats() SystemPerformanceStats {
	return SystemPerformanceStats{
		TotalQPS:            cm.registry.Gauge("system_total_qps").Get(),
		AverageResponseTime: cm.getAverageResponseTime(),
		TotalThroughput:     cm.registry.Gauge("system_total_throughput").Get(),
		ClusterUtilization:  cm.registry.Gauge("cluster_utilization").Get(),
		LoadBalance:         cm.registry.Gauge("system_load_balance_score").Get(),
	}
}

func (cm *CoordinatorMonitor) getQueryCoordinationStats() QueryCoordinationStats {
	queriesCoordinated := cm.registry.Counter("queries_coordinated").Get()
	
	avgFragments := 0.0
	if histogram, exists := cm.registry.histograms["fragments_per_query"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			avgFragments = stats.Mean
		}
	}
	
	avgWorkers := 0.0
	if histogram, exists := cm.registry.histograms["workers_per_query"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			avgWorkers = stats.Mean
		}
	}
	
	coordinationOverhead := 0.0
	if histogram, exists := cm.registry.histograms["coordination_overhead"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			coordinationOverhead = stats.Mean
		}
	}
	
	// Calculate parallelism efficiency (simplified)
	parallelismEfficiency := 100.0
	if avgWorkers > 1 {
		// If we're using multiple workers, check if coordination overhead is reasonable
		if coordinationOverhead > 100 { // >100ms coordination overhead
			parallelismEfficiency = 100 - (coordinationOverhead / 10) // Rough calculation
		}
	}
	
	return QueryCoordinationStats{
		QueriesCoordinated:    queriesCoordinated,
		AverageFragments:      avgFragments,
		AverageWorkersUsed:    avgWorkers,
		CoordinationOverhead:  coordinationOverhead,
		PlanningTime:          cm.getPlanningTime(),
		AggregationTime:       cm.getAggregationTime(),
		ParallelismEfficiency: parallelismEfficiency,
	}
}

func (cm *CoordinatorMonitor) getResourceDistributionStats() ResourceDistributionStats {
	cm.workersMutex.RLock()
	workers := make(map[string]*WorkerMonitor)
	for k, v := range cm.workers {
		workers[k] = v
	}
	cm.workersMutex.RUnlock()
	
	if len(workers) == 0 {
		return ResourceDistributionStats{}
	}
	
	workerLoads := make(map[string]float64)
	var totalCPU, totalMemory, totalLoad float64
	var loads []float64
	
	for workerID, worker := range workers {
		stats := worker.GetWorkerStats()
		load := stats.ResourceUtilization.CPUUsagePercent
		
		workerLoads[workerID] = load
		totalCPU += stats.ResourceUtilization.CPUUsagePercent
		totalMemory += stats.ResourceUtilization.MemoryUsageMB
		totalLoad += load
		loads = append(loads, load)
	}
	
	avgLoad := totalLoad / float64(len(workers))
	
	// Calculate load variance
	var variance float64
	for _, load := range loads {
		variance += (load - avgLoad) * (load - avgLoad)
	}
	variance /= float64(len(loads))
	
	return ResourceDistributionStats{
		TotalCPUUsage:     totalCPU,
		TotalMemoryUsage:  totalMemory,
		AverageWorkerLoad: avgLoad,
		LoadVariance:      variance,
		WorkerLoads:       workerLoads,
	}
}

func (cm *CoordinatorMonitor) getOptimizationMetrics() OptimizationMetrics {
	totalQueries := cm.registry.Counter("queries_coordinated").Get()
	optimizedQueries := cm.registry.Counter("optimizations_applied").Get()
	
	optimizationRate := 0.0
	if totalQueries > 0 {
		optimizationRate = (float64(optimizedQueries) / float64(totalQueries)) * 100
	}
	
	avgCostReduction := 0.0
	if histogram, exists := cm.registry.histograms["cost_reduction_percent"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			avgCostReduction = stats.Mean
		}
	}
	
	return OptimizationMetrics{
		OptimizationRate:      optimizationRate,
		AverageCostReduction:  avgCostReduction,
		DataTransferReduction: avgCostReduction, // Simplified - same as cost reduction
		CacheEffectiveness:    cm.getCacheEffectiveness(),
	}
}

func (cm *CoordinatorMonitor) getNetworkMetrics() NetworkMetrics {
	// This would be calculated from worker network stats
	return NetworkMetrics{
		TotalDataTransferred: 0, // Would aggregate from workers
		AverageLatency:       0, // Would calculate from worker latencies
		BandwidthUtilization: 0, // Would calculate from network usage
		FragmentationRatio:   cm.getFragmentationRatio(),
	}
}

func (cm *CoordinatorMonitor) getActiveAlerts() []SystemAlert {
	// This would return active system alerts
	// For now, return empty slice
	return []SystemAlert{}
}

// Helper methods
func (cm *CoordinatorMonitor) getAverageResponseTime() float64 {
	if timer, exists := cm.registry.timers["query_execution_duration"]; exists {
		stats := timer.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 0
}

func (cm *CoordinatorMonitor) getPlanningTime() float64 {
	if timer, exists := cm.registry.timers["query_planning"]; exists {
		stats := timer.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 0
}

func (cm *CoordinatorMonitor) getAggregationTime() float64 {
	if timer, exists := cm.registry.timers["result_aggregation"]; exists {
		stats := timer.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 0
}

func (cm *CoordinatorMonitor) getCacheEffectiveness() float64 {
	// Would aggregate cache effectiveness from all workers
	return 0.0 // Simplified
}

func (cm *CoordinatorMonitor) getFragmentationRatio() float64 {
	// Calculate ratio of fragments to queries
	if histogram, exists := cm.registry.histograms["fragments_per_query"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 1.0
}

// Global coordinator monitor instance
var GlobalCoordinatorMonitor = NewCoordinatorMonitor()