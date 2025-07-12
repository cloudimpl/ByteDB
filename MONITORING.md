# ByteDB Monitoring & Observability Guide

This document provides comprehensive information about ByteDB's monitoring and observability system, which provides real-time visibility into distributed query execution performance.

## 🎯 Overview

ByteDB includes a sophisticated monitoring system that tracks performance metrics across all levels of the distributed query engine:

- **Query-level**: Individual query performance, optimization tracking, and lifecycle monitoring
- **Worker-level**: Resource utilization, health status, and performance alerts  
- **Coordinator-level**: Cluster-wide coordination metrics and system performance
- **System-level**: Real-time dashboard with streaming updates and metrics export

## 🚀 Quick Start

### Launch Monitoring Demo

```bash
# Run the comprehensive metrics demonstration
go run metrics_demo.go

# This will:
# 1. Start real-time dashboard at http://localhost:8091
# 2. Generate sample metrics across all system components
# 3. Show query performance tracking
# 4. Display worker health monitoring
# 5. Demonstrate metrics export capabilities
```

### Access the Dashboard

Once running, visit http://localhost:8091 to access the real-time monitoring dashboard with:
- System overview and cluster health
- Worker status and resource utilization
- Query performance analytics
- Live streaming metrics updates
- Performance alerts and recommendations

## 📊 Core Metrics Framework

### Metric Types

ByteDB supports four fundamental metric types:

#### Counter Metrics
Track incremental values that only increase:
```go
queryCounter := registry.Counter("total_queries")
queryCounter.Inc()        // Increment by 1
queryCounter.Add(5)       // Increment by 5
count := queryCounter.Get() // Get current value
```

Examples:
- `queries_total` - Total number of queries executed
- `queries_successful` - Number of successful queries
- `queries_failed` - Number of failed queries
- `optimizations_applied` - Number of optimizations applied

#### Gauge Metrics
Track values that can increase or decrease:
```go
cpuGauge := registry.Gauge("cpu_usage_percent")
cpuGauge.Set(75.5)        // Set to specific value
cpuGauge.Add(5.0)         // Add to current value
usage := cpuGauge.Get()   // Get current value
```

Examples:
- `cpu_usage_percent` - Current CPU utilization
- `memory_usage_mb` - Current memory usage
- `active_queries` - Number of currently active queries
- `cluster_utilization` - Overall cluster utilization percentage

#### Histogram Metrics
Track distribution of values with configurable buckets:
```go
responseHist := registry.Histogram("response_time_ms", 
    []float64{10, 50, 100, 500, 1000, 5000})
responseHist.Observe(234.5)  // Record a measurement
stats := responseHist.GetStats() // Get distribution statistics
```

Examples:
- `query_duration_distribution` - Query execution time distribution
- `query_rows_distribution` - Number of rows processed per query
- `cost_reduction_distribution` - Query optimization effectiveness

#### Timer Metrics
Specialized histograms for measuring durations:
```go
queryTimer := registry.Timer("query_execution_time")
duration := queryTimer.Time(func() {
    // Execute operation to time
    executeQuery()
})
```

Examples:
- `query_planning_duration` - Time spent planning queries
- `query_execution_duration` - Time spent executing queries
- `coordination_overhead` - Time spent coordinating between nodes

## 🔍 Query Performance Monitoring

### Query Lifecycle Tracking

The system tracks the complete query execution lifecycle:

```go
// Start monitoring a query
execution := queryMonitor.StartQueryMonitoring("query-123", "SELECT * FROM table")
execution.SetQueryType("SELECT")

// Track planning phase
execution.StartPlanning()
// ... planning logic ...
execution.EndPlanning()

// Track execution phase
execution.StartExecution()
// ... execution logic ...
execution.EndExecution()

// Track aggregation phase (for distributed queries)
execution.StartAggregation()
// ... aggregation logic ...
execution.EndAggregation()

// Record additional metrics
execution.SetWorkerInfo(3, 5)  // 3 workers, 5 fragments
execution.SetDataInfo(1500, 127500) // 1500 rows, 127KB processed
execution.SetOptimizationInfo(true, 150.0, 90.0) // 40% cost reduction
execution.SetCacheInfo(78.5) // 78.5% cache hit rate

// Finish monitoring and get comprehensive metrics
metrics := execution.Finish()
```

### Query Metrics Structure

```go
type QueryMetrics struct {
    QueryID              string        // Unique query identifier
    SQL                  string        // Original SQL query
    QueryType            string        // SELECT, SELECT_AGGREGATE, etc.
    TotalDuration        time.Duration // End-to-end execution time
    PlanningDuration     time.Duration // Time spent planning
    ExecutionDuration    time.Duration // Time spent executing
    AggregationDuration  time.Duration // Time spent aggregating results
    WorkersUsed          int           // Number of workers involved
    FragmentsExecuted    int           // Number of query fragments
    RowsProcessed        int           // Total rows processed
    BytesTransferred     int64         // Data transferred over network
    CacheHitRate         float64       // Cache effectiveness
    OptimizationApplied  bool          // Whether optimization was applied
    OriginalCost         float64       // Original estimated cost
    OptimizedCost        float64       // Optimized estimated cost
    CostReduction        float64       // Percentage cost reduction
    ErrorOccurred        bool          // Whether an error occurred
    ErrorMessage         string        // Error details (if any)
    NetworkLatency       time.Duration // Network communication latency
    CoordinationOverhead time.Duration // Coordination overhead
}
```

### Query Performance Analysis

Get comprehensive query statistics:

```go
stats := queryMonitor.GetQueryStats()
// Returns:
// - Total queries executed
// - Success/failure rates
// - Average response times
// - Optimization effectiveness
// - Throughput (QPS)
```

Generate detailed performance reports:

```go
report := queryMonitor.QueryPerformanceReport()
// Includes:
// - Timing breakdown by phase
// - Distribution statistics
// - Optimization impact analysis
// - Performance recommendations
```

## 🖥️ Worker Health Monitoring

### Worker Statistics Tracking

Each worker maintains comprehensive health and performance metrics:

```go
monitor := monitoring.NewWorkerMonitor("worker-1")

// Record query execution
monitor.RecordQueryStart("query-123")
// ... processing ...
monitor.RecordQueryEnd("query-123", true, 500, 50*1024) // success, 500 rows, 50KB

// Record cache activity
monitor.RecordCacheHit()
monitor.RecordCacheMiss()

// Update resource metrics
monitor.UpdateResourceMetrics()

// Get comprehensive statistics
stats := monitor.GetWorkerStats()
```

### Worker Metrics Structure

```go
type WorkerStats struct {
    WorkerID            string                 // Worker identifier
    Status              WorkerHealthStatus     // Current health status
    Performance         WorkerPerformanceStats // Performance metrics
    ResourceUtilization WorkerResourceStats    // Resource usage
    QueryStats          WorkerQueryStats       // Query execution stats
    CacheStats          WorkerCacheStats       // Cache performance
    NetworkStats        WorkerNetworkStats     // Network activity
    Errors              []WorkerError          // Recent errors
}
```

### Performance Alerting

Workers automatically generate performance alerts:

```go
alerts := monitor.CheckPerformanceAlerts()
// Returns alerts for:
// - High CPU usage (>85%)
// - High memory usage (>80%)
// - Low cache hit rate (<50%)
// - High query failure rate (>5%)
```

Alert types include:
- **high_cpu**: CPU usage exceeding threshold
- **high_memory**: Memory usage exceeding threshold  
- **low_cache_hit_rate**: Cache performance below acceptable level
- **high_failure_rate**: Query failure rate too high

## 🏥 Cluster-wide Coordination Monitoring

### Coordinator Statistics

The coordinator tracks system-wide metrics:

```go
coordinator := monitoring.NewCoordinatorMonitor()

// Register workers
coordinator.RegisterWorker("worker-1", workerMonitor1)
coordinator.RegisterWorker("worker-2", workerMonitor2)

// Track query coordination
coordinator.RecordQueryStart("query-123", 3, 2) // 3 fragments, 2 workers
// ... execution ...
coordinator.RecordQueryEnd("query-123", true, clusterStats)

// Track optimization
coordinator.RecordOptimization(true, 150.0, 90.0) // 40% cost reduction

// Get comprehensive cluster statistics
stats := coordinator.GetCoordinatorStats()
```

### Cluster Health Monitoring

The system continuously monitors cluster health:

```go
type ClusterHealth struct {
    Status           string              // healthy, degraded, critical
    TotalWorkers     int                 // Total registered workers
    HealthyWorkers   int                 // Number of healthy workers
    DegradedWorkers  int                 // Number of degraded workers
    UnhealthyWorkers int                 // Number of unhealthy workers
    OfflineWorkers   int                 // Number of offline workers
    Issues           []string            // Current issues
    Uptime           time.Duration       // System uptime
    WorkerStatuses   map[string]string   // Per-worker status
}
```

Health status determination:
- **healthy**: ≥80% of workers are healthy
- **degraded**: 50-80% of workers are healthy
- **critical**: <50% of workers are healthy

### Load Balancing Metrics

The system tracks load distribution effectiveness:

```go
type SystemPerformanceStats struct {
    TotalQPS            float64 // Cluster-wide queries per second
    AverageResponseTime float64 // Average response time
    TotalThroughput     float64 // Total data throughput
    ClusterUtilization  float64 // Overall cluster utilization
    LoadBalance         float64 // Load balance score (0-100)
}
```

Load balance score calculation:
- Based on coefficient of variation of worker loads
- Higher scores indicate better load distribution
- 100 = perfect balance, 0 = completely unbalanced

## 🌐 Real-time Dashboard

### Web Interface Features

The dashboard provides a comprehensive web interface at http://localhost:8091:

#### Main Dashboard (`/`)
- Interactive HTML interface with auto-refresh
- System overview cards
- Real-time metrics display
- Performance charts and graphs

#### API Endpoints

| Endpoint | Description | Response Format |
|----------|-------------|-----------------|
| `/api/overview` | System overview metrics | JSON |
| `/api/cluster` | Cluster health status | JSON |
| `/api/workers` | Worker statistics | JSON |
| `/api/queries` | Query performance metrics | JSON |
| `/api/metrics` | Raw metrics data | JSON |
| `/api/alerts` | Active performance alerts | JSON |
| `/api/performance` | Comprehensive performance report | JSON |
| `/api/stream` | Real-time metrics stream | Server-Sent Events |

#### Live Streaming

The dashboard uses Server-Sent Events for real-time updates:

```javascript
// JavaScript client code
const eventSource = new EventSource('/api/stream');
eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    updateDashboard(data);
};
```

Streaming data includes:
- System overview metrics
- Cluster health status
- Worker resource utilization
- Active alerts
- Performance recommendations

### Dashboard Configuration

```go
// Create and start dashboard
coordinatorMonitor := monitoring.NewCoordinatorMonitor()
queryMonitor := monitoring.NewQueryMonitor()
dashboard := monitoring.NewDashboard(coordinatorMonitor, queryMonitor)

// Add worker monitors
dashboard.AddWorkerMonitor("worker-1", workerMonitor1)
dashboard.AddWorkerMonitor("worker-2", workerMonitor2)

// Start dashboard server
go dashboard.Start(8091) // Port 8091
```

## 📤 Metrics Export & Integration

### Prometheus Integration

Export metrics in Prometheus format:

```go
exporter := monitoring.NewMetricsExporter()
prometheusData := exporter.ExportPrometheusMetrics()
```

Sample Prometheus output:
```
# Counter metrics
bytedb_queries_total 1500 1640995200000
bytedb_queries_successful 1425 1640995200000
bytedb_queries_failed 75 1640995200000

# Gauge metrics
bytedb_cpu_usage_percent{worker_id="worker-1"} 78.5 1640995200000
bytedb_memory_usage_mb{worker_id="worker-1"} 2048.0 1640995200000

# Histogram metrics
bytedb_query_duration_ms_count 1500 1640995200000
bytedb_query_duration_ms_sum 285750.0 1640995200000
bytedb_query_duration_ms_bucket{le="100"} 450 1640995200000
bytedb_query_duration_ms_bucket{le="500"} 1200 1640995200000
bytedb_query_duration_ms_bucket{le="+Inf"} 1500 1640995200000
```

### JSON Export

Export structured metrics data:

```go
jsonData, err := exporter.ExportJSONMetrics()
```

Sample JSON output:
```json
[
  {
    "name": "queries_total",
    "type": "counter",
    "value": 1500,
    "labels": {"component": "coordinator"},
    "timestamp": "2023-12-01T10:00:00Z"
  },
  {
    "name": "cpu_usage_percent",
    "type": "gauge", 
    "value": 78.5,
    "labels": {"worker_id": "worker-1"},
    "timestamp": "2023-12-01T10:00:00Z"
  }
]
```

### External Monitoring Integration

#### Grafana Dashboard Setup

1. Configure Prometheus to scrape ByteDB metrics:
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'bytedb'
    static_configs:
      - targets: ['localhost:8091']
    metrics_path: '/api/metrics'
    scrape_interval: 5s
```

2. Import metrics into Grafana:
   - Query performance: `rate(bytedb_queries_total[5m])`
   - Error rate: `rate(bytedb_queries_failed[5m]) / rate(bytedb_queries_total[5m])`
   - Resource usage: `bytedb_cpu_usage_percent`

#### Elasticsearch Integration

Forward JSON metrics to Elasticsearch:
```bash
curl -X POST "localhost:9200/bytedb-metrics/_doc" \
     -H "Content-Type: application/json" \
     -d "$(curl -s http://localhost:8091/api/overview)"
```

## 🔧 Performance Profiling

### Detailed Execution Analysis

Create performance profiles for detailed analysis:

```go
profiler := monitoring.NewPerformanceProfiler()

// Start profiling
profileID := "query-analysis-session"
profiler.StartProfile(profileID)

// Execute queries to profile
// ...

// Stop profiling and get results
profile := profiler.StopProfile(profileID)

// Analyze results
fmt.Printf("Duration: %v\n", profile.Duration)
fmt.Printf("Throughput: %.2f QPS\n", profile.Summary.ThroughputQPS)
fmt.Printf("Average Latency: %v\n", profile.Summary.AverageLatency)
```

### Profile Data Structure

```go
type ProfileData struct {
    StartTime    time.Time            // Profile start time
    EndTime      time.Time            // Profile end time
    Duration     time.Duration        // Total duration
    Samples      []ProfileSample      // Individual measurements
    Summary      ProfileSummary       // Aggregated statistics
}

type ProfileSummary struct {
    TotalQueries     int64         // Total queries profiled
    AverageLatency   time.Duration // Average query latency
    P95Latency       time.Duration // 95th percentile latency
    ErrorRate        float64       // Error rate percentage
    ThroughputQPS    float64       // Queries per second
    ResourceUsage    map[string]float64 // Resource utilization
}
```

## ⚡ Performance Recommendations

### Automated Analysis

The monitoring system provides automated performance recommendations:

```go
type PerformanceRecommendation struct {
    Type        string    // Type of recommendation
    Priority    string    // high, medium, low
    Description string    // Human-readable description
    Impact      string    // Expected improvement
    Action      string    // Suggested action
}
```

Common recommendations:
- **High Query Failure Rate**: Review error logs and query complexity
- **Low Optimization Rate**: Review query patterns and data partitioning
- **High Response Time**: Consider adding more workers
- **Poor Load Balancing**: Redistribute queries or add workers
- **High Resource Usage**: Scale up worker resources
- **Low Cache Hit Rate**: Review cache configuration

### Performance Tuning Guidelines

Based on monitoring data:

1. **Query Performance**:
   - Queries >5s average: Add more workers or optimize queries
   - Low optimization rate <60%: Review data partitioning
   - High coordination overhead >100ms: Optimize query planning

2. **Worker Health**:
   - CPU >85%: Scale up or add workers
   - Memory >80%: Increase worker memory limits
   - Cache hit rate <50%: Tune cache size or TTL

3. **Cluster Coordination**:
   - Load balance <70%: Review data distribution
   - Network latency >50ms: Optimize network configuration
   - Coordination overhead growing: Review query complexity

## 🔍 Troubleshooting

### Common Monitoring Issues

#### Dashboard Not Accessible
```bash
# Check if dashboard is running
curl http://localhost:8091/api/overview

# If not running, start the dashboard
go run metrics_demo.go
```

#### Metrics Not Updating
```bash
# Check metric collection
curl http://localhost:8091/api/metrics | grep "queries_total"

# Verify worker registration
curl http://localhost:8091/api/workers
```

#### High Resource Usage
```bash
# Check individual worker stats
curl http://localhost:8091/api/workers | jq '.worker-1.resource_utilization'

# Review performance alerts
curl http://localhost:8091/api/alerts
```

### Debug Mode

Enable verbose monitoring logging:
```go
// Set debug mode for detailed logging
monitoring.SetDebugMode(true)

// Monitor logs for:
// - Metric collection timing
// - Worker health check results
// - Coordination overhead details
```

## 📊 Example Integration

### Complete Monitoring Setup

```go
package main

import (
    "bytedb/distributed/monitoring"
    "context"
    "log"
    "time"
)

func main() {
    // Create monitoring components
    coordinatorMonitor := monitoring.NewCoordinatorMonitor()
    queryMonitor := monitoring.NewQueryMonitor()
    
    // Create worker monitors
    worker1Monitor := monitoring.NewWorkerMonitor("worker-1")
    worker2Monitor := monitoring.NewWorkerMonitor("worker-2")
    
    // Register workers with coordinator
    coordinatorMonitor.RegisterWorker("worker-1", worker1Monitor)
    coordinatorMonitor.RegisterWorker("worker-2", worker2Monitor)
    
    // Create and start dashboard
    dashboard := monitoring.NewDashboard(coordinatorMonitor, queryMonitor)
    dashboard.AddWorkerMonitor("worker-1", worker1Monitor)
    dashboard.AddWorkerMonitor("worker-2", worker2Monitor)
    
    // Start dashboard in background
    go func() {
        log.Printf("Starting dashboard on :8091")
        if err := dashboard.Start(8091); err != nil {
            log.Printf("Dashboard error: %v", err)
        }
    }()
    
    // Simulate query execution with monitoring
    ctx := context.Background()
    for i := 0; i < 10; i++ {
        queryID := fmt.Sprintf("query-%d", i)
        sql := "SELECT * FROM employees WHERE salary > 70000"
        
        // Start query monitoring
        execution := queryMonitor.StartQueryMonitoring(queryID, sql)
        execution.SetQueryType("SELECT")
        
        // Simulate query execution phases
        execution.StartPlanning()
        time.Sleep(20 * time.Millisecond)
        execution.EndPlanning()
        
        execution.StartExecution()
        time.Sleep(100 * time.Millisecond)
        execution.EndExecution()
        
        // Record results
        execution.SetWorkerInfo(2, 3)
        execution.SetDataInfo(150, 15*1024)
        execution.SetOptimizationInfo(true, 100.0, 70.0)
        
        // Finish monitoring
        metrics := execution.Finish()
        log.Printf("Query %s completed in %v", queryID, metrics.TotalDuration)
        
        time.Sleep(500 * time.Millisecond)
    }
    
    // Export metrics
    exporter := monitoring.NewMetricsExporter()
    
    // Prometheus format
    prometheusData := exporter.ExportPrometheusMetrics()
    log.Printf("Prometheus metrics: %d bytes", len(prometheusData))
    
    // JSON format
    jsonData, err := exporter.ExportJSONMetrics()
    if err == nil {
        log.Printf("JSON metrics: %d bytes", len(jsonData))
    }
    
    // Keep running
    select {}
}
```

## 📈 Benefits Summary

ByteDB's monitoring system provides:

✅ **Zero-overhead Metrics Collection**: Efficient collection without impacting query performance
✅ **Comprehensive Visibility**: End-to-end monitoring from individual queries to cluster-wide performance  
✅ **Real-time Observability**: Live dashboard with streaming updates for immediate insight
✅ **Automated Alerting**: Performance alerts and recommendations based on established thresholds
✅ **Export Flexibility**: Multiple formats (JSON, Prometheus) for integration with external systems
✅ **Production Ready**: Scalable architecture suitable for large distributed deployments

The monitoring system is designed to provide essential observability for production ByteDB deployments while maintaining the high performance characteristics of the distributed query engine.