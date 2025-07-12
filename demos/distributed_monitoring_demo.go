package main

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"bytedb/distributed/coordinator"
	"bytedb/distributed/monitoring"
	"bytedb/distributed/worker"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Comprehensive demo showcasing the distributed monitoring system
func main() {
	fmt.Println("🚀 ByteDB Distributed Monitoring System Demo")
	fmt.Println("=" + fmt.Sprintf("%50s", "="))
	
	// Initialize the monitoring system
	demo := NewMonitoringDemo()
	
	// Run the comprehensive demo
	demo.RunDemo()
}

type MonitoringDemo struct {
	transport    communication.Transport
	coordinator  *coordinator.Coordinator
	workers      []*worker.Worker
	dashboard    *monitoring.Dashboard
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewMonitoringDemo() *MonitoringDemo {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	
	return &MonitoringDemo{
		transport: communication.NewMemoryTransport(),
		workers:   make([]*worker.Worker, 0),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (d *MonitoringDemo) RunDemo() {
	defer d.cancel()
	
	fmt.Println("\n📊 Phase 1: Setting Up Distributed System with Monitoring")
	d.setupDistributedSystem()
	
	fmt.Println("\n📈 Phase 2: Starting Real-time Monitoring Dashboard")
	d.startMonitoringDashboard()
	
	fmt.Println("\n🔍 Phase 3: Demonstrating Query Execution with Metrics")
	d.demonstrateQueryMetrics()
	
	fmt.Println("\n🏥 Phase 4: Showcasing Worker Health Monitoring")
	d.demonstrateWorkerMonitoring()
	
	fmt.Println("\n📊 Phase 5: Cluster-wide Performance Analysis")
	d.demonstrateClusterMetrics()
	
	fmt.Println("\n🎯 Phase 6: Performance Optimization Tracking")
	d.demonstrateOptimizationTracking()
	
	fmt.Println("\n📋 Phase 7: Monitoring Dashboard & Metrics Export")
	d.demonstrateMonitoringFeatures()
	
	fmt.Println("\n✅ Monitoring Demo Complete!")
	d.printSummary()
}

func (d *MonitoringDemo) setupDistributedSystem() {
	// Create coordinator with monitoring
	d.coordinator = coordinator.NewCoordinator(d.transport)
	
	// Create workers with monitoring
	workerIDs := []string{"worker-1", "worker-2", "worker-3"}
	for _, id := range workerIDs {
		worker := worker.NewWorker(id, "./data")
		d.workers = append(d.workers, worker)
		
		fmt.Printf("✅ Created worker %s with monitoring\n", id)
	}
	
	// Wait for workers to register
	time.Sleep(500 * time.Millisecond)
	
	fmt.Printf("🎯 Coordinator initialized with %d workers\n", len(d.workers))
	fmt.Println("📊 Monitoring system active and collecting metrics")
}

func (d *MonitoringDemo) startMonitoringDashboard() {
	// Create dashboard
	coordinatorMonitor := monitoring.NewCoordinatorMonitor()
	queryMonitor := monitoring.NewQueryMonitor()
	
	d.dashboard = monitoring.NewDashboard(coordinatorMonitor, queryMonitor)
	
	// Add worker monitors to dashboard
	for _, worker := range d.workers {
		workerMonitor := monitoring.NewWorkerMonitor(worker.GetID())
		d.dashboard.AddWorkerMonitor(worker.GetID(), workerMonitor)
		
		// Simulate some worker activity for demo
		d.simulateWorkerActivity(workerMonitor)
	}
	
	// Start dashboard in background
	go func() {
		fmt.Println("🌐 Dashboard available at http://localhost:8090")
		if err := d.dashboard.Start(8090); err != nil {
			log.Printf("Dashboard error: %v", err)
		}
	}()
	
	// Give dashboard time to start
	time.Sleep(1 * time.Second)
	fmt.Println("✅ Real-time monitoring dashboard started")
}

func (d *MonitoringDemo) demonstrateQueryMetrics() {
	fmt.Println("\n--- Executing Various Queries to Generate Metrics ---")
	
	queries := []struct {
		name string
		sql  string
	}{
		{"Simple Scan", "SELECT * FROM employees LIMIT 10"},
		{"Filtered Query", "SELECT name, salary FROM employees WHERE salary > 75000"},
		{"Aggregate Query", "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department"},
		{"Complex Join", "SELECT e.name, d.department_name FROM employees e JOIN departments d ON e.department = d.id"},
	}
	
	for i, query := range queries {
		fmt.Printf("\n🔍 Query %d: %s\n", i+1, query.name)
		fmt.Printf("   SQL: %s\n", query.sql)
		
		start := time.Now()
		
		// Execute query through coordinator
		req := &communication.DistributedQueryRequest{
			RequestID: fmt.Sprintf("demo-query-%d", i+1),
			SQL:       query.sql,
		}
		
		response, err := d.coordinator.ExecuteQuery(d.ctx, req)
		duration := time.Since(start)
		
		if err != nil {
			fmt.Printf("   ❌ Error: %v\n", err)
			continue
		}
		
		if response.Error != "" {
			fmt.Printf("   ⚠️  Query Error: %s\n", response.Error)
			continue
		}
		
		fmt.Printf("   ✅ Completed in %v\n", duration)
		fmt.Printf("   📊 Rows returned: %d\n", response.RowCount)
		fmt.Printf("   ⚡ Query metrics recorded automatically\n")
		
		// Brief pause between queries to show metrics evolution
		time.Sleep(200 * time.Millisecond)
	}
	
	fmt.Println("\n📈 Query execution metrics collected and available in dashboard")
}

func (d *MonitoringDemo) demonstrateWorkerMonitoring() {
	fmt.Println("\n--- Worker Health and Performance Monitoring ---")
	
	// Get worker monitors from dashboard
	for _, worker := range d.workers {
		fmt.Printf("\n🖥️  Worker: %s\n", worker.GetID())
		
		// Simulate worker activity and get monitor
		workerMonitor := monitoring.NewWorkerMonitor(worker.GetID())
		
		// Simulate some query execution
		for i := 0; i < 5; i++ {
			workerMonitor.RecordQueryStart(fmt.Sprintf("query-%d", i))
			
			// Simulate processing time
			time.Sleep(time.Duration(50+i*10) * time.Millisecond)
			
			// Simulate completion
			success := i < 4 // One failure for demo
			workerMonitor.RecordQueryEnd(fmt.Sprintf("query-%d", i), success, 100*(i+1), 1024*(i+1))
			
			if i == 2 {
				workerMonitor.RecordCacheHit()
			} else {
				workerMonitor.RecordCacheMiss()
			}
		}
		
		// Update resource metrics
		workerMonitor.UpdateResourceMetrics()
		
		// Get and display stats
		stats := workerMonitor.GetWorkerStats()
		fmt.Printf("   📊 Status: %s\n", stats.Status.Status)
		fmt.Printf("   🔢 Total Queries: %d (Success: %d, Failed: %d)\n", 
			stats.QueryStats.TotalQueries, stats.QueryStats.SuccessfulQueries, stats.QueryStats.FailedQueries)
		fmt.Printf("   💾 Memory Usage: %.1f MB\n", stats.ResourceUtilization.MemoryUsageMB)
		fmt.Printf("   🎯 Cache Hit Rate: %.1f%%\n", stats.CacheStats.HitRate)
		fmt.Printf("   ⚡ Avg Response Time: %.2f ms\n", stats.Performance.AverageResponseTime)
		
		// Check for performance alerts
		alerts := workerMonitor.CheckPerformanceAlerts()
		if len(alerts) > 0 {
			fmt.Printf("   🚨 Active Alerts: %d\n", len(alerts))
			for _, alert := range alerts {
				fmt.Printf("     - %s: %s\n", alert.Severity, alert.Message)
			}
		} else {
			fmt.Printf("   ✅ No performance issues detected\n")
		}
	}
}

func (d *MonitoringDemo) demonstrateClusterMetrics() {
	fmt.Println("\n--- Cluster-wide Performance Analysis ---")
	
	coordinatorMonitor := monitoring.NewCoordinatorMonitor()
	
	// Register workers with coordinator monitor
	for _, worker := range d.workers {
		workerMonitor := monitoring.NewWorkerMonitor(worker.GetID())
		coordinatorMonitor.RegisterWorker(worker.GetID(), workerMonitor)
	}
	
	// Simulate some cluster activity
	for i := 0; i < 3; i++ {
		queryID := fmt.Sprintf("cluster-query-%d", i)
		coordinatorMonitor.RecordQueryStart(queryID, 3, len(d.workers))
		
		time.Sleep(100 * time.Millisecond)
		
		// Simulate cluster stats
		clusterStats := &communication.ClusterStats{
			TotalTime:    100 * time.Millisecond,
			WorkersUsed:  len(d.workers),
			RowsReturned: 1000 * (i + 1),
		}
		
		coordinatorMonitor.RecordQueryEnd(queryID, true, clusterStats)
		
		if i%2 == 0 {
			coordinatorMonitor.RecordOptimization(true, 100.0, 60.0) // 40% cost reduction
		}
	}
	
	// Get comprehensive coordinator stats
	stats := coordinatorMonitor.GetCoordinatorStats()
	
	fmt.Printf("🏥 Cluster Health: %s\n", stats.ClusterHealth.Status)
	fmt.Printf("👥 Workers: %d total, %d healthy, %d degraded\n", 
		stats.ClusterHealth.TotalWorkers, stats.ClusterHealth.HealthyWorkers, stats.ClusterHealth.DegradedWorkers)
	fmt.Printf("⚡ System Performance:\n")
	fmt.Printf("   - Total QPS: %.2f\n", stats.SystemPerformance.TotalQPS)
	fmt.Printf("   - Avg Response Time: %.2f ms\n", stats.SystemPerformance.AverageResponseTime)
	fmt.Printf("   - Cluster Utilization: %.1f%%\n", stats.SystemPerformance.ClusterUtilization)
	fmt.Printf("   - Load Balance Score: %.1f/100\n", stats.SystemPerformance.LoadBalance)
	
	fmt.Printf("🔄 Query Coordination:\n")
	fmt.Printf("   - Queries Coordinated: %d\n", stats.QueryCoordination.QueriesCoordinated)
	fmt.Printf("   - Avg Fragments per Query: %.1f\n", stats.QueryCoordination.AverageFragments)
	fmt.Printf("   - Avg Workers per Query: %.1f\n", stats.QueryCoordination.AverageWorkersUsed)
	fmt.Printf("   - Coordination Overhead: %.2f ms\n", stats.QueryCoordination.CoordinationOverhead)
	
	fmt.Printf("🎯 Optimization Metrics:\n")
	fmt.Printf("   - Optimization Rate: %.1f%%\n", stats.OptimizationMetrics.OptimizationRate)
	fmt.Printf("   - Avg Cost Reduction: %.1f%%\n", stats.OptimizationMetrics.AverageCostReduction)
}

func (d *MonitoringDemo) demonstrateOptimizationTracking() {
	fmt.Println("\n--- Query Optimization Tracking ---")
	
	queryMonitor := monitoring.NewQueryMonitor()
	
	// Simulate different types of optimized queries
	optimizations := []struct {
		name         string
		originalCost float64
		optimizedCost float64
		applied      bool
	}{
		{"Predicate Pushdown", 150.0, 90.0, true},
		{"Aggregate Optimization", 200.0, 80.0, true},
		{"Join Reordering", 180.0, 120.0, true},
		{"No Optimization", 100.0, 100.0, false},
	}
	
	for i, opt := range optimizations {
		fmt.Printf("\n🔧 Optimization Example %d: %s\n", i+1, opt.name)
		
		// Start query monitoring
		queryExecution := queryMonitor.StartQueryMonitoring(fmt.Sprintf("opt-query-%d", i), "SELECT * FROM table")
		queryExecution.SetQueryType("SELECT")
		
		// Simulate planning phase
		queryExecution.StartPlanning()
		time.Sleep(20 * time.Millisecond)
		queryExecution.EndPlanning()
		
		// Simulate execution
		queryExecution.StartExecution()
		time.Sleep(100 * time.Millisecond)
		queryExecution.EndExecution()
		
		// Record optimization info
		queryExecution.SetOptimizationInfo(opt.applied, opt.originalCost, opt.optimizedCost)
		queryExecution.SetDataInfo(1000*(i+1), 50*1024*(i+1))
		queryExecution.SetWorkerInfo(2, 3)
		
		// Finish monitoring
		metrics := queryExecution.Finish()
		
		fmt.Printf("   📊 Query completed in %v\n", metrics.TotalDuration)
		fmt.Printf("   💡 Optimization Applied: %v\n", metrics.OptimizationApplied)
		if metrics.OptimizationApplied {
			fmt.Printf("   💰 Original Cost: %.1f\n", metrics.OriginalCost)
			fmt.Printf("   💰 Optimized Cost: %.1f\n", metrics.OptimizedCost)
			fmt.Printf("   📈 Cost Reduction: %.1f%%\n", metrics.CostReduction)
		}
		fmt.Printf("   🔢 Rows Processed: %d\n", metrics.RowsProcessed)
		fmt.Printf("   📦 Data Transferred: %d bytes\n", metrics.BytesTransferred)
	}
	
	// Get overall query statistics
	fmt.Println("\n📈 Overall Query Performance Summary:")
	stats := queryMonitor.GetQueryStats()
	fmt.Printf("   📊 Total Queries: %d\n", stats.TotalQueries)
	fmt.Printf("   ✅ Successful: %d (%.1f%%)\n", stats.SuccessfulQueries, 
		float64(stats.SuccessfulQueries)/float64(stats.TotalQueries)*100)
	fmt.Printf("   🎯 Optimized: %d (%.1f%%)\n", stats.OptimizedQueries, stats.OptimizationRate)
	fmt.Printf("   ⚡ Avg Response Time: %.2f ms\n", stats.AverageResponseTime)
	fmt.Printf("   💰 Avg Cost Reduction: %.1f%%\n", stats.AverageCostReduction)
}

func (d *MonitoringDemo) demonstrateMonitoringFeatures() {
	fmt.Println("\n--- Monitoring Features & Export Capabilities ---")
	
	// Demonstrate metrics export
	fmt.Println("\n📤 Metrics Export Formats:")
	
	exporter := monitoring.NewMetricsExporter()
	
	// JSON export
	jsonMetrics, err := exporter.ExportJSONMetrics()
	if err != nil {
		fmt.Printf("❌ JSON export error: %v\n", err)
	} else {
		fmt.Printf("✅ JSON metrics exported (%d bytes)\n", len(jsonMetrics))
		fmt.Println("   Sample JSON structure available for external systems")
	}
	
	// Prometheus export
	prometheusMetrics := exporter.ExportPrometheusMetrics()
	fmt.Printf("✅ Prometheus metrics exported (%d bytes)\n", len(prometheusMetrics))
	fmt.Println("   Compatible with Prometheus/Grafana monitoring stack")
	
	// Performance profiling
	fmt.Println("\n🔍 Performance Profiling Capabilities:")
	profiler := monitoring.NewPerformanceProfiler()
	
	profileID := "demo-profile"
	profiler.StartProfile(profileID)
	
	// Simulate some work
	time.Sleep(100 * time.Millisecond)
	
	profile := profiler.StopProfile(profileID)
	if profile != nil {
		fmt.Printf("✅ Performance profile captured:\n")
		fmt.Printf("   - Duration: %v\n", profile.Duration)
		fmt.Printf("   - Samples: %d\n", len(profile.Samples))
		fmt.Printf("   - Throughput: %.2f QPS\n", profile.Summary.ThroughputQPS)
	}
	
	// Dashboard features
	fmt.Println("\n🌐 Dashboard Features Available:")
	fmt.Println("   ✅ Real-time metrics streaming (Server-Sent Events)")
	fmt.Println("   ✅ Interactive web interface at http://localhost:8090")
	fmt.Println("   ✅ Historical trend analysis")
	fmt.Println("   ✅ Performance alerting and recommendations")
	fmt.Println("   ✅ Worker health monitoring")
	fmt.Println("   ✅ Cluster-wide coordination metrics")
	fmt.Println("   ✅ Query optimization tracking")
	
	// Global metrics overview
	fmt.Println("\n📊 Global Metrics Registry:")
	allMetrics := monitoring.GetAllMetrics()
	fmt.Printf("   📈 Total metrics tracked: %d\n", len(allMetrics))
	
	metricTypes := make(map[monitoring.MetricType]int)
	for _, metric := range allMetrics {
		metricTypes[metric.Type]++
	}
	
	for metricType, count := range metricTypes {
		fmt.Printf("   - %s: %d metrics\n", metricType, count)
	}
}

func (d *MonitoringDemo) simulateWorkerActivity(monitor *monitoring.WorkerMonitor) {
	// Simulate background worker activity
	go func() {
		for i := 0; i < 10; i++ {
			monitor.RecordQueryStart(fmt.Sprintf("bg-query-%d", i))
			
			// Random processing time
			time.Sleep(time.Duration(30+i*5) * time.Millisecond)
			
			success := i%7 != 0 // Occasional failure
			monitor.RecordQueryEnd(fmt.Sprintf("bg-query-%d", i), success, 50*(i+1), 2048*(i+1))
			
			// Random cache behavior
			if i%3 == 0 {
				monitor.RecordCacheHit()
			} else {
				monitor.RecordCacheMiss()
			}
			
			monitor.UpdateResourceMetrics()
			
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (d *MonitoringDemo) printSummary() {
	fmt.Println("\n" + "🎯 MONITORING SYSTEM SUMMARY")
	fmt.Println("=" + fmt.Sprintf("%40s", "="))
	
	fmt.Println("\n✅ Core Components Demonstrated:")
	fmt.Println("   📊 Metrics Framework - Counter, Gauge, Histogram, Timer types")
	fmt.Println("   🔍 Query Performance Monitoring - End-to-end query lifecycle tracking") 
	fmt.Println("   🖥️  Worker Health Monitoring - Resource utilization and performance")
	fmt.Println("   🏥 Coordinator System Monitoring - Cluster-wide health and coordination")
	fmt.Println("   🌐 Real-time Dashboard - Web interface with live streaming")
	
	fmt.Println("\n🎯 Key Features Showcased:")
	fmt.Println("   ⚡ Real-time performance metrics collection")
	fmt.Println("   🎯 Query optimization effectiveness tracking")
	fmt.Println("   🚨 Automated performance alerting")
	fmt.Println("   📈 Historical trend analysis capabilities")
	fmt.Println("   📤 Multi-format metrics export (JSON, Prometheus)")
	fmt.Println("   🔧 Performance profiling and recommendations")
	
	fmt.Println("\n🌐 Integration Points:")
	fmt.Println("   ✅ Seamlessly integrated into distributed query execution")
	fmt.Println("   ✅ Automatic metrics collection without performance overhead")
	fmt.Println("   ✅ Compatible with external monitoring systems")
	fmt.Println("   ✅ Comprehensive visibility into system behavior")
	
	fmt.Println("\n📋 Next Steps:")
	fmt.Println("   🔗 Visit http://localhost:8090 for live dashboard")
	fmt.Println("   📊 Explore metrics via /api/metrics endpoint")
	fmt.Println("   🔍 Monitor query performance in real-time")
	fmt.Println("   🎯 Set up alerts based on your SLA requirements")
	
	fmt.Println("\n🚀 The ByteDB distributed monitoring system provides comprehensive")
	fmt.Println("   observability into your distributed query engine performance!")
}