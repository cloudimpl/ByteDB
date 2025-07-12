package main

import (
	"bytedb/distributed/communication"
	"bytedb/distributed/monitoring"
	"fmt"
	"math/rand"
	"time"
)

// Focused demo showcasing just the metrics system capabilities
func main() {
	fmt.Println("🎯 ByteDB Metrics System Demo")
	fmt.Println("============================")
	
	demo := NewMetricsDemo()
	demo.RunDemo()
}

type MetricsDemo struct {
	registry *monitoring.MetricsRegistry
}

func NewMetricsDemo() *MetricsDemo {
	return &MetricsDemo{
		registry: monitoring.NewMetricsRegistry(),
	}
}

func (d *MetricsDemo) RunDemo() {
	fmt.Println("\n📊 Phase 1: Basic Metrics Types")
	d.demonstrateBasicMetrics()
	
	fmt.Println("\n📈 Phase 2: Query Performance Monitoring")
	d.demonstrateQueryMonitoring()
	
	fmt.Println("\n🖥️  Phase 3: Worker Health Monitoring")
	d.demonstrateWorkerMonitoring()
	
	fmt.Println("\n🏥 Phase 4: Coordinator System Monitoring")
	d.demonstrateCoordinatorMonitoring()
	
	fmt.Println("\n🌐 Phase 5: Real-time Dashboard")
	d.demonstrateDashboard()
	
	fmt.Println("\n📤 Phase 6: Metrics Export")
	d.demonstrateMetricsExport()
	
	fmt.Println("\n✅ Metrics Demo Complete!")
}

func (d *MetricsDemo) demonstrateBasicMetrics() {
	fmt.Println("\n--- Core Metrics Types ---")
	
	// Counter metrics
	queryCounter := d.registry.Counter("total_queries")
	errorCounter := d.registry.Counter("query_errors")
	
	fmt.Println("🔢 Counter Metrics:")
	for i := 0; i < 10; i++ {
		queryCounter.Inc()
		if i%4 == 0 {
			errorCounter.Inc()
		}
	}
	fmt.Printf("   ✅ Total Queries: %d\n", queryCounter.Get())
	fmt.Printf("   ❌ Query Errors: %d\n", errorCounter.Get())
	
	// Gauge metrics
	cpuGauge := d.registry.Gauge("cpu_usage_percent")
	memoryGauge := d.registry.Gauge("memory_usage_mb")
	
	fmt.Println("\n📊 Gauge Metrics:")
	cpuGauge.Set(75.5)
	memoryGauge.Set(2048.0)
	fmt.Printf("   💻 CPU Usage: %.1f%%\n", cpuGauge.Get())
	fmt.Printf("   💾 Memory Usage: %.0f MB\n", memoryGauge.Get())
	
	// Histogram metrics
	responseTimeHist := d.registry.Histogram("response_time_ms", 
		[]float64{10, 50, 100, 500, 1000, 5000})
	
	fmt.Println("\n📈 Histogram Metrics:")
	// Simulate response times
	responseTimes := []float64{45.2, 123.4, 67.8, 234.5, 89.1, 456.7, 34.2, 567.8, 123.4, 78.9}
	for _, rt := range responseTimes {
		responseTimeHist.Observe(rt)
	}
	
	stats := responseTimeHist.GetStats()
	fmt.Printf("   ⚡ Response Time Distribution:\n")
	fmt.Printf("     - Count: %d samples\n", stats.Count)
	fmt.Printf("     - Sum: %.2f ms\n", stats.Sum)
	fmt.Printf("     - Mean: %.2f ms\n", stats.Mean)
	fmt.Printf("     - Bucket Distribution: %d buckets\n", len(stats.Buckets))
	
	// Timer metrics
	queryTimer := d.registry.Timer("query_execution_time")
	
	fmt.Println("\n⏱️  Timer Metrics:")
	// Simulate timed operations
	for i := 0; i < 5; i++ {
		duration := queryTimer.Time(func() {
			// Simulate work
			time.Sleep(time.Duration(20+i*10) * time.Millisecond)
		})
		fmt.Printf("   Operation %d: %v\n", i+1, duration)
	}
	
	timerStats := queryTimer.GetStats()
	fmt.Printf("   📊 Timer Statistics:\n")
	fmt.Printf("     - Total Operations: %d\n", timerStats.Count)
	fmt.Printf("     - Average Duration: %.2f ms\n", timerStats.Mean)
}

func (d *MetricsDemo) demonstrateQueryMonitoring() {
	fmt.Println("\n--- Query Performance Monitoring ---")
	
	queryMonitor := monitoring.NewQueryMonitor()
	
	// Simulate different types of queries
	queries := []struct {
		id       string
		sql      string
		queryType string
		duration time.Duration
		rows     int
		optimized bool
		originalCost  float64
		optimizedCost float64
	}{
		{"q1", "SELECT * FROM users WHERE age > 25", "SELECT", 150*time.Millisecond, 1500, true, 120.0, 80.0},
		{"q2", "SELECT department, COUNT(*) FROM employees GROUP BY department", "SELECT_AGGREGATE", 300*time.Millisecond, 8, true, 200.0, 100.0},
		{"q3", "SELECT name FROM products WHERE price < 100", "SELECT", 75*time.Millisecond, 450, false, 90.0, 90.0},
		{"q4", "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id", "SELECT_JOIN", 400*time.Millisecond, 2800, true, 300.0, 180.0},
	}
	
	for i, query := range queries {
		fmt.Printf("\n🔍 Query %d: %s\n", i+1, query.queryType)
		fmt.Printf("   SQL: %s\n", query.sql)
		
		// Start monitoring
		execution := queryMonitor.StartQueryMonitoring(query.id, query.sql)
		execution.SetQueryType(query.queryType)
		
		// Planning phase
		execution.StartPlanning()
		time.Sleep(20 * time.Millisecond)
		execution.EndPlanning()
		
		// Execution phase
		execution.StartExecution()
		time.Sleep(query.duration)
		execution.EndExecution()
		
		// Set additional metrics
		execution.SetWorkerInfo(3, 2)
		execution.SetDataInfo(query.rows, int64(query.rows*85)) // ~85 bytes per row
		execution.SetOptimizationInfo(query.optimized, query.originalCost, query.optimizedCost)
		execution.SetCacheInfo(75.5) // 75.5% cache hit rate
		
		// Finish monitoring
		metrics := execution.Finish()
		
		fmt.Printf("   ✅ Completed in %v\n", metrics.TotalDuration)
		fmt.Printf("   📊 Planning: %v | Execution: %v\n", 
			metrics.PlanningDuration, metrics.ExecutionDuration)
		fmt.Printf("   🔢 Rows: %d | Workers: %d\n", 
			metrics.RowsProcessed, metrics.WorkersUsed)
		
		if metrics.OptimizationApplied {
			fmt.Printf("   💡 Optimization: %.1f%% cost reduction\n", metrics.CostReduction)
		}
		fmt.Printf("   🎯 Cache Hit Rate: %.1f%%\n", metrics.CacheHitRate)
	}
	
	// Show overall statistics
	fmt.Println("\n📈 Query Performance Summary:")
	stats := queryMonitor.GetQueryStats()
	fmt.Printf("   📊 Total Queries: %d\n", stats.TotalQueries)
	fmt.Printf("   ✅ Success Rate: %.1f%%\n", 
		float64(stats.SuccessfulQueries)/float64(stats.TotalQueries)*100)
	fmt.Printf("   🎯 Optimization Rate: %.1f%%\n", stats.OptimizationRate)
	fmt.Printf("   ⚡ Avg Response Time: %.2f ms\n", stats.AverageResponseTime)
	fmt.Printf("   💰 Avg Cost Reduction: %.1f%%\n", stats.AverageCostReduction)
}

func (d *MetricsDemo) demonstrateWorkerMonitoring() {
	fmt.Println("\n--- Worker Health Monitoring ---")
	
	// Create worker monitors
	workers := []string{"worker-1", "worker-2", "worker-3"}
	
	for _, workerID := range workers {
		fmt.Printf("\n🖥️  Worker: %s\n", workerID)
		
		monitor := monitoring.NewWorkerMonitor(workerID)
		
		// Simulate worker activity
		for i := 0; i < 8; i++ {
			queryID := fmt.Sprintf("worker-query-%d", i)
			monitor.RecordQueryStart(queryID)
			
			// Simulate processing
			processingTime := time.Duration(30+rand.Intn(100)) * time.Millisecond
			time.Sleep(processingTime)
			
			// Random success/failure
			success := rand.Float32() > 0.1 // 90% success rate
			rows := 100 + rand.Intn(500)
			bytes := int64(rows * (80 + rand.Intn(40))) // 80-120 bytes per row
			
			monitor.RecordQueryEnd(queryID, success, rows, bytes)
			
			// Random cache activity
			if rand.Float32() > 0.3 {
				monitor.RecordCacheHit()
			} else {
				monitor.RecordCacheMiss()
			}
		}
		
		// Update resource metrics
		monitor.UpdateResourceMetrics()
		
		// Get statistics
		stats := monitor.GetWorkerStats()
		
		fmt.Printf("   📊 Health Status: %s\n", stats.Status.Status)
		fmt.Printf("   🔢 Queries: %d total, %d active\n", 
			stats.QueryStats.TotalQueries, stats.QueryStats.ActiveQueries)
		fmt.Printf("   ✅ Success Rate: %.1f%%\n", stats.QueryStats.SuccessRate)
		fmt.Printf("   ⚡ Avg Response: %.2f ms\n", stats.Performance.AverageResponseTime)
		fmt.Printf("   💾 Memory: %.1f MB (%.1f%%)\n", 
			stats.ResourceUtilization.MemoryUsageMB, stats.ResourceUtilization.MemoryUsagePercent)
		fmt.Printf("   🎯 Cache Hit Rate: %.1f%%\n", stats.CacheStats.HitRate)
		fmt.Printf("   🌐 Network: %d bytes received, %d bytes sent\n", 
			stats.NetworkStats.BytesReceived, stats.NetworkStats.BytesSent)
		
		// Check for alerts
		alerts := monitor.CheckPerformanceAlerts()
		if len(alerts) > 0 {
			fmt.Printf("   🚨 Alerts: %d active\n", len(alerts))
			for _, alert := range alerts {
				fmt.Printf("     - %s: %s\n", alert.Severity, alert.Message)
			}
		} else {
			fmt.Printf("   ✅ No performance alerts\n")
		}
	}
}

func (d *MetricsDemo) demonstrateCoordinatorMonitoring() {
	fmt.Println("\n--- Coordinator System Monitoring ---")
	
	coordinator := monitoring.NewCoordinatorMonitor()
	
	// Simulate worker registration
	workerMonitors := make(map[string]*monitoring.WorkerMonitor)
	for _, workerID := range []string{"worker-1", "worker-2", "worker-3"} {
		monitor := monitoring.NewWorkerMonitor(workerID)
		coordinator.RegisterWorker(workerID, monitor)
		workerMonitors[workerID] = monitor
		
		// Simulate worker activity
		d.simulateWorkerActivity(monitor, workerID)
	}
	
	// Simulate cluster queries
	fmt.Println("\n🔄 Simulating Cluster Queries...")
	for i := 0; i < 5; i++ {
		queryID := fmt.Sprintf("cluster-query-%d", i)
		fragmentCount := 2 + rand.Intn(4) // 2-5 fragments
		workerCount := 2 + rand.Intn(2)   // 2-3 workers
		
		coordinator.RecordQueryStart(queryID, fragmentCount, workerCount)
		
		// Simulate execution time
		time.Sleep(time.Duration(80+rand.Intn(120)) * time.Millisecond)
		
		// Create cluster stats
		clusterStats := &communication.ClusterStats{
			TotalTime:         time.Duration(80+rand.Intn(120)) * time.Millisecond,
			WorkersUsed:       workerCount,
			TotalFragments:    fragmentCount,
			DataTransferBytes: int64(500 + rand.Intn(2000)) * 100, // ~100 bytes per row
		}
		
		success := rand.Float32() > 0.05 // 95% success rate
		coordinator.RecordQueryEnd(queryID, success, clusterStats)
		
		// Random optimization
		if rand.Float32() > 0.3 {
			originalCost := 100.0 + rand.Float64()*200.0
			optimizedCost := originalCost * (0.5 + rand.Float64()*0.4) // 50-90% of original
			coordinator.RecordOptimization(true, originalCost, optimizedCost)
		}
	}
	
	// Get comprehensive stats
	stats := coordinator.GetCoordinatorStats()
	
	fmt.Println("\n📊 Cluster Health Summary:")
	fmt.Printf("   🏥 Overall Status: %s\n", stats.ClusterHealth.Status)
	fmt.Printf("   👥 Workers: %d total, %d healthy, %d degraded\n",
		stats.ClusterHealth.TotalWorkers, 
		stats.ClusterHealth.HealthyWorkers,
		stats.ClusterHealth.DegradedWorkers)
	fmt.Printf("   ⏱️  Uptime: %v\n", stats.ClusterHealth.Uptime)
	
	fmt.Println("\n⚡ System Performance:")
	fmt.Printf("   📈 Total QPS: %.2f\n", stats.SystemPerformance.TotalQPS)
	fmt.Printf("   ⏱️  Avg Response: %.2f ms\n", stats.SystemPerformance.AverageResponseTime)
	fmt.Printf("   🎯 Cluster Utilization: %.1f%%\n", stats.SystemPerformance.ClusterUtilization)
	fmt.Printf("   ⚖️  Load Balance Score: %.1f/100\n", stats.SystemPerformance.LoadBalance)
	
	fmt.Println("\n🔄 Query Coordination:")
	fmt.Printf("   📊 Queries Coordinated: %d\n", stats.QueryCoordination.QueriesCoordinated)
	fmt.Printf("   🧩 Avg Fragments/Query: %.1f\n", stats.QueryCoordination.AverageFragments)
	fmt.Printf("   👥 Avg Workers/Query: %.1f\n", stats.QueryCoordination.AverageWorkersUsed)
	fmt.Printf("   ⏱️  Coordination Overhead: %.2f ms\n", stats.QueryCoordination.CoordinationOverhead)
	
	fmt.Println("\n🎯 Optimization Metrics:")
	fmt.Printf("   💡 Optimization Rate: %.1f%%\n", stats.OptimizationMetrics.OptimizationRate)
	fmt.Printf("   💰 Avg Cost Reduction: %.1f%%\n", stats.OptimizationMetrics.AverageCostReduction)
}

func (d *MetricsDemo) demonstrateDashboard() {
	fmt.Println("\n--- Real-time Monitoring Dashboard ---")
	
	// Create dashboard components
	coordinatorMonitor := monitoring.NewCoordinatorMonitor()
	queryMonitor := monitoring.NewQueryMonitor()
	dashboard := monitoring.NewDashboard(coordinatorMonitor, queryMonitor)
	
	// Add worker monitors
	for _, workerID := range []string{"worker-1", "worker-2", "worker-3"} {
		monitor := monitoring.NewWorkerMonitor(workerID)
		dashboard.AddWorkerMonitor(workerID, monitor)
	}
	
	fmt.Println("🌐 Dashboard Features:")
	fmt.Println("   ✅ Real-time metrics streaming via Server-Sent Events")
	fmt.Println("   ✅ Interactive web interface")
	fmt.Println("   ✅ Multi-component monitoring (queries, workers, cluster)")
	fmt.Println("   ✅ Performance alerts and recommendations")
	fmt.Println("   ✅ Historical trend analysis")
	
	fmt.Println("\n📊 Available API Endpoints:")
	fmt.Println("   GET /              - Dashboard web interface")
	fmt.Println("   GET /api/overview  - System overview metrics")
	fmt.Println("   GET /api/cluster   - Cluster health status")
	fmt.Println("   GET /api/workers   - Worker statistics")
	fmt.Println("   GET /api/queries   - Query performance metrics")
	fmt.Println("   GET /api/metrics   - Raw metrics data")
	fmt.Println("   GET /api/alerts    - Active alerts")
	fmt.Println("   GET /api/stream    - Real-time metrics stream")
	
	// Start dashboard in background for demo
	go func() {
		fmt.Println("\n🚀 Starting dashboard on http://localhost:8091...")
		if err := dashboard.Start(8091); err != nil {
			fmt.Printf("Dashboard error: %v\n", err)
		}
	}()
	
	// Give dashboard time to start
	time.Sleep(1 * time.Second)
	fmt.Println("✅ Dashboard started successfully!")
	
	// Simulate live metrics for a few seconds
	fmt.Println("\n📈 Generating live metrics for demonstration...")
	for i := 0; i < 10; i++ {
		// Simulate some activity
		d.registry.Counter("demo_requests").Inc()
		d.registry.Gauge("demo_active_connections").Set(float64(15 + rand.Intn(10)))
		d.registry.Histogram("demo_response_time", []float64{10, 50, 100, 500}).Observe(float64(20 + rand.Intn(200)))
		
		time.Sleep(200 * time.Millisecond)
		fmt.Printf(".")
	}
	fmt.Println(" ✅ Live metrics generated!")
}

func (d *MetricsDemo) demonstrateMetricsExport() {
	fmt.Println("\n--- Metrics Export & Integration ---")
	
	// Create some sample metrics
	d.registry.Counter("export_demo_queries").Add(1500)
	d.registry.Counter("export_demo_errors").Add(25)
	d.registry.Gauge("export_demo_cpu").Set(78.5)
	d.registry.Gauge("export_demo_memory").Set(4096.0)
	
	responseHist := d.registry.Histogram("export_demo_response_time", 
		[]float64{10, 50, 100, 500, 1000})
	for _, val := range []float64{45, 123, 67, 234, 89, 456, 34, 567} {
		responseHist.Observe(val)
	}
	
	exporter := monitoring.NewMetricsExporter()
	
	// JSON Export
	fmt.Println("\n📤 JSON Metrics Export:")
	jsonData, err := exporter.ExportJSONMetrics()
	if err != nil {
		fmt.Printf("❌ JSON export error: %v\n", err)
	} else {
		fmt.Printf("✅ Exported %d bytes of JSON metrics\n", len(jsonData))
		fmt.Println("   📋 Usage: Import into custom dashboards, log aggregators")
		fmt.Println("   🔗 Compatible with: Elasticsearch, Splunk, custom analytics")
	}
	
	// Prometheus Export
	fmt.Println("\n📤 Prometheus Metrics Export:")
	prometheusData := exporter.ExportPrometheusMetrics()
	fmt.Printf("✅ Exported %d bytes of Prometheus metrics\n", len(prometheusData))
	fmt.Println("   📋 Usage: Scrape with Prometheus server")
	fmt.Println("   🔗 Compatible with: Prometheus, Grafana, AlertManager")
	fmt.Println("   📊 Automatic histogram buckets and metric metadata")
	
	// Show sample Prometheus output
	fmt.Println("\n📝 Sample Prometheus Format:")
	lines := []string{
		"export_demo_queries 1500 1234567890123",
		"export_demo_errors 25 1234567890123", 
		"export_demo_cpu 78.5 1234567890123",
		"export_demo_response_time_count 8 1234567890123",
		"export_demo_response_time_sum 1615.0 1234567890123",
		"export_demo_response_time_bucket{le=\"100\"} 4 1234567890123",
		"export_demo_response_time_bucket{le=\"500\"} 7 1234567890123",
		"export_demo_response_time_bucket{le=\"+Inf\"} 8 1234567890123",
	}
	for _, line := range lines {
		fmt.Printf("   %s\n", line)
	}
	
	// Performance Profiling
	fmt.Println("\n🔍 Performance Profiling:")
	profiler := monitoring.NewPerformanceProfiler()
	
	profileID := "metrics-demo-profile"
	profiler.StartProfile(profileID)
	
	// Simulate some work
	time.Sleep(150 * time.Millisecond)
	
	profile := profiler.StopProfile(profileID)
	if profile != nil {
		fmt.Printf("✅ Profile captured: %v duration\n", profile.Duration)
		fmt.Printf("   📊 Samples: %d\n", len(profile.Samples))
		fmt.Printf("   ⚡ Throughput: %.2f QPS\n", profile.Summary.ThroughputQPS)
	}
	
	// Global metrics overview
	fmt.Println("\n📊 Global Metrics Registry:")
	allMetrics := monitoring.GetAllMetrics()
	fmt.Printf("   📈 Total metrics: %d\n", len(allMetrics))
	
	typeCount := make(map[monitoring.MetricType]int)
	for _, metric := range allMetrics {
		typeCount[metric.Type]++
	}
	
	for metricType, count := range typeCount {
		fmt.Printf("   - %s: %d metrics\n", metricType, count)
	}
	
	fmt.Println("\n🎯 Integration Benefits:")
	fmt.Println("   ✅ Zero-overhead metrics collection")
	fmt.Println("   ✅ Multiple export formats for different tools")
	fmt.Println("   ✅ Real-time streaming capabilities")
	fmt.Println("   ✅ Comprehensive query execution visibility")
	fmt.Println("   ✅ Automated performance alerting")
	fmt.Println("   ✅ Historical analysis and trending")
}

func (d *MetricsDemo) simulateWorkerActivity(monitor *monitoring.WorkerMonitor, workerID string) {
	// Simulate background activity
	go func() {
		for i := 0; i < 5; i++ {
			queryID := fmt.Sprintf("%s-bg-query-%d", workerID, i)
			monitor.RecordQueryStart(queryID)
			
			time.Sleep(time.Duration(20+rand.Intn(60)) * time.Millisecond)
			
			success := rand.Float32() > 0.1
			rows := 50 + rand.Intn(200)
			bytes := int64(rows * (70 + rand.Intn(50)))
			
			monitor.RecordQueryEnd(queryID, success, rows, bytes)
			
			if rand.Float32() > 0.4 {
				monitor.RecordCacheHit()
			} else {
				monitor.RecordCacheMiss()
			}
			
			monitor.UpdateResourceMetrics()
			time.Sleep(100 * time.Millisecond)
		}
	}()
}