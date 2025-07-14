package monitoring

import (
	"fmt"
	"time"
)

// QueryMonitor tracks query-level performance and execution metrics
type QueryMonitor struct {
	registry *MetricsRegistry
}

func NewQueryMonitor() *QueryMonitor {
	return &QueryMonitor{
		registry: GlobalRegistry,
	}
}

// QueryMetrics tracks comprehensive query execution metrics
type QueryMetrics struct {
	QueryID              string        `json:"query_id"`
	SQL                  string        `json:"sql"`
	QueryType            string        `json:"query_type"`
	StartTime            time.Time     `json:"start_time"`
	EndTime              time.Time     `json:"end_time"`
	TotalDuration        time.Duration `json:"total_duration"`
	PlanningDuration     time.Duration `json:"planning_duration"`
	ExecutionDuration    time.Duration `json:"execution_duration"`
	AggregationDuration  time.Duration `json:"aggregation_duration"`
	WorkersUsed          int           `json:"workers_used"`
	FragmentsExecuted    int           `json:"fragments_executed"`
	RowsProcessed        int           `json:"rows_processed"`
	BytesTransferred     int64         `json:"bytes_transferred"`
	CacheHitRate         float64       `json:"cache_hit_rate"`
	OptimizationApplied  bool          `json:"optimization_applied"`
	OriginalCost         float64       `json:"original_cost"`
	OptimizedCost        float64       `json:"optimized_cost"`
	CostReduction        float64       `json:"cost_reduction"`
	ErrorOccurred        bool          `json:"error_occurred"`
	ErrorMessage         string        `json:"error_message,omitempty"`
	NetworkLatency       time.Duration `json:"network_latency"`
	CoordinationOverhead time.Duration `json:"coordination_overhead"`
}

// StartQueryMonitoring begins monitoring a query
func (qm *QueryMonitor) StartQueryMonitoring(queryID, sql string) *QueryExecution {
	return &QueryExecution{
		metrics: &QueryMetrics{
			QueryID:   queryID,
			SQL:       sql,
			StartTime: time.Now(),
		},
		monitor: qm,
		timers: map[string]*TimerMetric{
			"planning":     qm.registry.Timer("query_planning_duration"),
			"execution":    qm.registry.Timer("query_execution_duration"),
			"aggregation":  qm.registry.Timer("query_aggregation_duration"),
			"coordination": qm.registry.Timer("query_coordination_duration"),
		},
	}
}

// QueryExecution tracks a single query execution
type QueryExecution struct {
	metrics *QueryMetrics
	monitor *QueryMonitor
	timers  map[string]*TimerMetric
}

func (qe *QueryExecution) SetQueryType(queryType string) {
	qe.metrics.QueryType = queryType
}

func (qe *QueryExecution) StartPlanning() {
	qe.timers["planning"].Start()
}

func (qe *QueryExecution) EndPlanning() {
	qe.metrics.PlanningDuration = qe.timers["planning"].Stop()
}

func (qe *QueryExecution) StartExecution() {
	qe.timers["execution"].Start()
}

func (qe *QueryExecution) EndExecution() {
	qe.metrics.ExecutionDuration = qe.timers["execution"].Stop()
}

func (qe *QueryExecution) StartAggregation() {
	qe.timers["aggregation"].Start()
}

func (qe *QueryExecution) EndAggregation() {
	qe.metrics.AggregationDuration = qe.timers["aggregation"].Stop()
}

func (qe *QueryExecution) StartCoordination() {
	qe.timers["coordination"].Start()
}

func (qe *QueryExecution) EndCoordination() {
	qe.metrics.CoordinationOverhead = qe.timers["coordination"].Stop()
}

func (qe *QueryExecution) SetWorkerInfo(workersUsed, fragmentsExecuted int) {
	qe.metrics.WorkersUsed = workersUsed
	qe.metrics.FragmentsExecuted = fragmentsExecuted
}

func (qe *QueryExecution) SetDataInfo(rowsProcessed int, bytesTransferred int64) {
	qe.metrics.RowsProcessed = rowsProcessed
	qe.metrics.BytesTransferred = bytesTransferred
}

func (qe *QueryExecution) SetOptimizationInfo(applied bool, originalCost, optimizedCost float64) {
	qe.metrics.OptimizationApplied = applied
	qe.metrics.OriginalCost = originalCost
	qe.metrics.OptimizedCost = optimizedCost
	if originalCost > 0 {
		qe.metrics.CostReduction = ((originalCost - optimizedCost) / originalCost) * 100
	}
}

func (qe *QueryExecution) SetCacheInfo(hitRate float64) {
	qe.metrics.CacheHitRate = hitRate
}

func (qe *QueryExecution) SetError(err error) {
	qe.metrics.ErrorOccurred = true
	if err != nil {
		qe.metrics.ErrorMessage = err.Error()
	}
}

func (qe *QueryExecution) SetNetworkLatency(latency time.Duration) {
	qe.metrics.NetworkLatency = latency
}

func (qe *QueryExecution) Finish() *QueryMetrics {
	qe.metrics.EndTime = time.Now()
	qe.metrics.TotalDuration = qe.metrics.EndTime.Sub(qe.metrics.StartTime)
	
	// Update global metrics
	qe.updateGlobalMetrics()
	
	return qe.metrics
}

func (qe *QueryExecution) GetMetrics() *QueryMetrics {
	return qe.metrics
}

func (qe *QueryExecution) updateGlobalMetrics() {
	// Update counters
	qe.monitor.registry.Counter("queries_total").Inc()
	qe.monitor.registry.Counter(fmt.Sprintf("queries_by_type_%s", qe.metrics.QueryType)).Inc()
	
	if qe.metrics.ErrorOccurred {
		qe.monitor.registry.Counter("queries_failed").Inc()
	} else {
		qe.monitor.registry.Counter("queries_successful").Inc()
	}
	
	if qe.metrics.OptimizationApplied {
		qe.monitor.registry.Counter("queries_optimized").Inc()
	}
	
	// Update gauges
	qe.monitor.registry.Gauge("last_query_duration_ms").Set(float64(qe.metrics.TotalDuration.Milliseconds()))
	qe.monitor.registry.Gauge("last_query_rows_processed").Set(float64(qe.metrics.RowsProcessed))
	qe.monitor.registry.Gauge("last_query_bytes_transferred").Set(float64(qe.metrics.BytesTransferred))
	qe.monitor.registry.Gauge("last_query_workers_used").Set(float64(qe.metrics.WorkersUsed))
	qe.monitor.registry.Gauge("last_query_cost_reduction_percent").Set(qe.metrics.CostReduction)
	
	// Update histograms
	qe.monitor.registry.Histogram("query_duration_distribution", 
		[]float64{10, 50, 100, 500, 1000, 5000, 10000, 30000}).Observe(float64(qe.metrics.TotalDuration.Milliseconds()))
	
	qe.monitor.registry.Histogram("query_rows_distribution",
		[]float64{1, 10, 100, 1000, 10000, 100000, 1000000}).Observe(float64(qe.metrics.RowsProcessed))
	
	qe.monitor.registry.Histogram("query_bytes_distribution",
		[]float64{1024, 10240, 102400, 1048576, 10485760, 104857600}).Observe(float64(qe.metrics.BytesTransferred))
	
	if qe.metrics.OptimizationApplied && qe.metrics.CostReduction > 0 {
		qe.monitor.registry.Histogram("cost_reduction_distribution",
			[]float64{10, 25, 50, 75, 90, 95, 99, 99.9}).Observe(qe.metrics.CostReduction)
	}
}

// GetQueryStats returns current query statistics
func (qm *QueryMonitor) GetQueryStats() QueryStats {
	return QueryStats{
		TotalQueries:        qm.registry.Counter("queries_total").Get(),
		SuccessfulQueries:   qm.registry.Counter("queries_successful").Get(),
		FailedQueries:       qm.registry.Counter("queries_failed").Get(),
		OptimizedQueries:    qm.registry.Counter("queries_optimized").Get(),
		AverageResponseTime: qm.getAverageResponseTime(),
		ThroughputQPS:       qm.getThroughput(),
		OptimizationRate:    qm.getOptimizationRate(),
		AverageCostReduction: qm.getAverageCostReduction(),
	}
}

type QueryStats struct {
	TotalQueries         int64   `json:"total_queries"`
	SuccessfulQueries    int64   `json:"successful_queries"`
	FailedQueries        int64   `json:"failed_queries"`
	OptimizedQueries     int64   `json:"optimized_queries"`
	AverageResponseTime  float64 `json:"average_response_time_ms"`
	ThroughputQPS        float64 `json:"throughput_qps"`
	OptimizationRate     float64 `json:"optimization_rate_percent"`
	AverageCostReduction float64 `json:"average_cost_reduction_percent"`
}

func (qm *QueryMonitor) getAverageResponseTime() float64 {
	if timer, exists := qm.registry.timers["query_execution_duration"]; exists {
		stats := timer.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 0
}

func (qm *QueryMonitor) getThroughput() float64 {
	// This is a simplified calculation - in production you'd want a time-windowed rate
	total := qm.registry.Counter("queries_total").Get()
	if total > 0 {
		// Assuming this is since start time - you'd want to track actual time window
		return float64(total) / 60.0 // rough QPS estimate
	}
	return 0
}

func (qm *QueryMonitor) getOptimizationRate() float64 {
	total := qm.registry.Counter("queries_total").Get()
	optimized := qm.registry.Counter("queries_optimized").Get()
	if total > 0 {
		return (float64(optimized) / float64(total)) * 100
	}
	return 0
}

func (qm *QueryMonitor) getAverageCostReduction() float64 {
	if histogram, exists := qm.registry.histograms["cost_reduction_distribution"]; exists {
		stats := histogram.GetStats()
		if stats.Count > 0 {
			return stats.Mean
		}
	}
	return 0
}

// GetTopQueries returns information about the most resource-intensive queries
func (qm *QueryMonitor) GetTopQueries(limit int) []TopQueryInfo {
	// This would be implemented with query history tracking
	// For now, return empty slice - would need persistent storage
	return []TopQueryInfo{}
}

type TopQueryInfo struct {
	SQL               string        `json:"sql"`
	ExecutionCount    int           `json:"execution_count"`
	AverageDuration   time.Duration `json:"average_duration"`
	TotalDuration     time.Duration `json:"total_duration"`
	AverageRowsProcessed int        `json:"average_rows_processed"`
	LastExecution     time.Time     `json:"last_execution"`
}

// QueryPerformanceReport generates a comprehensive performance report
func (qm *QueryMonitor) QueryPerformanceReport() QueryPerformanceReport {
	stats := qm.GetQueryStats()
	
	return QueryPerformanceReport{
		Summary:           stats,
		TimingBreakdown:   qm.getTimingBreakdown(),
		DistributionStats: qm.getDistributionStats(),
		OptimizationImpact: qm.getOptimizationImpact(),
		Recommendations:   qm.generateRecommendations(stats),
	}
}

type QueryPerformanceReport struct {
	Summary            QueryStats                    `json:"summary"`
	TimingBreakdown    QueryTimingBreakdown         `json:"timing_breakdown"`
	DistributionStats  QueryDistributionStats       `json:"distribution_stats"`
	OptimizationImpact QueryOptimizationImpact      `json:"optimization_impact"`
	Recommendations    []string                     `json:"recommendations"`
}

type QueryTimingBreakdown struct {
	PlanningTime     HistogramStats `json:"planning_time"`
	ExecutionTime    HistogramStats `json:"execution_time"`
	AggregationTime  HistogramStats `json:"aggregation_time"`
	CoordinationTime HistogramStats `json:"coordination_time"`
}

type QueryDistributionStats struct {
	DurationDistribution HistogramStats `json:"duration_distribution"`
	RowsDistribution     HistogramStats `json:"rows_distribution"`
	BytesDistribution    HistogramStats `json:"bytes_distribution"`
}

type QueryOptimizationImpact struct {
	OptimizationRate        float64        `json:"optimization_rate"`
	CostReductionDistribution HistogramStats `json:"cost_reduction_distribution"`
	AverageCostReduction    float64        `json:"average_cost_reduction"`
}

func (qm *QueryMonitor) getTimingBreakdown() QueryTimingBreakdown {
	breakdown := QueryTimingBreakdown{}
	
	if timer, exists := qm.registry.timers["query_planning_duration"]; exists {
		breakdown.PlanningTime = timer.GetStats()
	}
	if timer, exists := qm.registry.timers["query_execution_duration"]; exists {
		breakdown.ExecutionTime = timer.GetStats()
	}
	if timer, exists := qm.registry.timers["query_aggregation_duration"]; exists {
		breakdown.AggregationTime = timer.GetStats()
	}
	if timer, exists := qm.registry.timers["query_coordination_duration"]; exists {
		breakdown.CoordinationTime = timer.GetStats()
	}
	
	return breakdown
}

func (qm *QueryMonitor) getDistributionStats() QueryDistributionStats {
	stats := QueryDistributionStats{}
	
	if histogram, exists := qm.registry.histograms["query_duration_distribution"]; exists {
		stats.DurationDistribution = histogram.GetStats()
	}
	if histogram, exists := qm.registry.histograms["query_rows_distribution"]; exists {
		stats.RowsDistribution = histogram.GetStats()
	}
	if histogram, exists := qm.registry.histograms["query_bytes_distribution"]; exists {
		stats.BytesDistribution = histogram.GetStats()
	}
	
	return stats
}

func (qm *QueryMonitor) getOptimizationImpact() QueryOptimizationImpact {
	impact := QueryOptimizationImpact{
		OptimizationRate:     qm.getOptimizationRate(),
		AverageCostReduction: qm.getAverageCostReduction(),
	}
	
	if histogram, exists := qm.registry.histograms["cost_reduction_distribution"]; exists {
		impact.CostReductionDistribution = histogram.GetStats()
	}
	
	return impact
}

func (qm *QueryMonitor) generateRecommendations(stats QueryStats) []string {
	var recommendations []string
	
	if stats.FailedQueries > int64(float64(stats.SuccessfulQueries)*0.05) { // >5% failure rate
		recommendations = append(recommendations, "High query failure rate detected. Review error logs and query complexity.")
	}
	
	if stats.OptimizationRate < 50 {
		recommendations = append(recommendations, "Low optimization rate. Consider reviewing query patterns and data distribution.")
	}
	
	if stats.AverageResponseTime > 5000 { // >5 seconds
		recommendations = append(recommendations, "High average response time. Consider adding more workers or optimizing queries.")
	}
	
	if stats.AverageCostReduction < 50 && stats.OptimizedQueries > 0 {
		recommendations = append(recommendations, "Low cost reduction from optimization. Review partitioning strategies.")
	}
	
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "System is performing well. Continue monitoring for trends.")
	}
	
	return recommendations
}

// Global query monitor instance
var GlobalQueryMonitor = NewQueryMonitor()