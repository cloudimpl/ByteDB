package monitoring

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Dashboard provides a web-based real-time monitoring interface
type Dashboard struct {
	coordinatorMonitor *CoordinatorMonitor
	queryMonitor       *QueryMonitor
	workerMonitors     map[string]*WorkerMonitor
	server             *http.Server
}

func NewDashboard(coordinator *CoordinatorMonitor, query *QueryMonitor) *Dashboard {
	return &Dashboard{
		coordinatorMonitor: coordinator,
		queryMonitor:       query,
		workerMonitors:     make(map[string]*WorkerMonitor),
	}
}

func (d *Dashboard) AddWorkerMonitor(workerID string, monitor *WorkerMonitor) {
	d.workerMonitors[workerID] = monitor
}

func (d *Dashboard) RemoveWorkerMonitor(workerID string) {
	delete(d.workerMonitors, workerID)
}

// Start the monitoring dashboard HTTP server
func (d *Dashboard) Start(port int) error {
	mux := http.NewServeMux()
	
	// Static dashboard routes
	mux.HandleFunc("/", d.handleDashboard)
	mux.HandleFunc("/api/overview", d.handleOverview)
	mux.HandleFunc("/api/cluster", d.handleCluster)
	mux.HandleFunc("/api/workers", d.handleWorkers)
	mux.HandleFunc("/api/queries", d.handleQueries)
	mux.HandleFunc("/api/metrics", d.handleMetrics)
	mux.HandleFunc("/api/alerts", d.handleAlerts)
	mux.HandleFunc("/api/performance", d.handlePerformance)
	
	// Real-time updates via Server-Sent Events
	mux.HandleFunc("/api/stream", d.handleStream)
	
	d.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	
	fmt.Printf("🌐 Monitoring dashboard starting on http://localhost:%d\n", port)
	return d.server.ListenAndServe()
}

func (d *Dashboard) Stop() error {
	if d.server != nil {
		return d.server.Close()
	}
	return nil
}

// Dashboard overview page
func (d *Dashboard) handleDashboard(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.New("dashboard").Parse(dashboardHTML))
	tmpl.Execute(w, nil)
}

// API endpoints for dashboard data

func (d *Dashboard) handleOverview(w http.ResponseWriter, r *http.Request) {
	overview := d.generateOverview()
	d.writeJSON(w, overview)
}

func (d *Dashboard) handleCluster(w http.ResponseWriter, r *http.Request) {
	clusterStats := d.coordinatorMonitor.GetCoordinatorStats()
	d.writeJSON(w, clusterStats.ClusterHealth)
}

func (d *Dashboard) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers := make(map[string]interface{})
	
	for workerID, monitor := range d.workerMonitors {
		workers[workerID] = monitor.GetWorkerStats()
	}
	
	d.writeJSON(w, workers)
}

func (d *Dashboard) handleQueries(w http.ResponseWriter, r *http.Request) {
	queryStats := d.queryMonitor.GetQueryStats()
	queryReport := d.queryMonitor.QueryPerformanceReport()
	
	response := map[string]interface{}{
		"stats":  queryStats,
		"report": queryReport,
	}
	
	d.writeJSON(w, response)
}

func (d *Dashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := GetAllMetrics()
	d.writeJSON(w, metrics)
}

func (d *Dashboard) handleAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := d.collectAllAlerts()
	d.writeJSON(w, alerts)
}

func (d *Dashboard) handlePerformance(w http.ResponseWriter, r *http.Request) {
	performance := d.generatePerformanceReport()
	d.writeJSON(w, performance)
}

// Real-time streaming endpoint
func (d *Dashboard) handleStream(w http.ResponseWriter, r *http.Request) {
	// Set headers for Server-Sent Events
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Create a ticker for periodic updates
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Send real-time data
			data := d.generateRealTimeData()
			jsonData, _ := json.Marshal(data)
			
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			
		case <-r.Context().Done():
			return
		}
	}
}

// Helper methods for generating dashboard data

func (d *Dashboard) generateOverview() map[string]interface{} {
	coordStats := d.coordinatorMonitor.GetCoordinatorStats()
	queryStats := d.queryMonitor.GetQueryStats()
	
	// Collect worker summary
	totalWorkers := len(d.workerMonitors)
	healthyWorkers := 0
	totalQPS := 0.0
	
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		if stats.Status.Status == "healthy" {
			healthyWorkers++
		}
		totalQPS += stats.Performance.QueriesPerSecond
	}
	
	return map[string]interface{}{
		"cluster_status":     coordStats.ClusterHealth.Status,
		"total_workers":      totalWorkers,
		"healthy_workers":    healthyWorkers,
		"total_queries":      queryStats.TotalQueries,
		"successful_queries": queryStats.SuccessfulQueries,
		"failed_queries":     queryStats.FailedQueries,
		"optimization_rate":  queryStats.OptimizationRate,
		"average_response_time": queryStats.AverageResponseTime,
		"total_qps":          totalQPS,
		"uptime":             time.Since(d.coordinatorMonitor.startTime).String(),
		"last_updated":       time.Now(),
	}
}

func (d *Dashboard) generateRealTimeData() map[string]interface{} {
	return map[string]interface{}{
		"timestamp": time.Now(),
		"overview":  d.generateOverview(),
		"cluster":   d.coordinatorMonitor.GetCoordinatorStats().ClusterHealth,
		"system_metrics": map[string]interface{}{
			"cpu_usage":    d.getSystemCPU(),
			"memory_usage": d.getSystemMemory(),
			"query_rate":   d.getQueryRate(),
			"throughput":   d.getSystemThroughput(),
		},
		"alerts": d.collectAllAlerts(),
	}
}

func (d *Dashboard) generatePerformanceReport() map[string]interface{} {
	coordStats := d.coordinatorMonitor.GetCoordinatorStats()
	queryReport := d.queryMonitor.QueryPerformanceReport()
	
	// Collect worker performance summaries
	workerPerformance := make(map[string]interface{})
	for workerID, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		workerPerformance[workerID] = map[string]interface{}{
			"qps":           stats.Performance.QueriesPerSecond,
			"response_time": stats.Performance.AverageResponseTime,
			"cpu_usage":     stats.ResourceUtilization.CPUUsagePercent,
			"memory_usage":  stats.ResourceUtilization.MemoryUsageMB,
			"cache_hit_rate": stats.CacheStats.HitRate,
		}
	}
	
	return map[string]interface{}{
		"system_performance":   coordStats.SystemPerformance,
		"query_performance":    queryReport,
		"worker_performance":   workerPerformance,
		"optimization_metrics": coordStats.OptimizationMetrics,
		"network_metrics":      coordStats.NetworkMetrics,
		"recommendations":      d.generateRecommendations(),
	}
}

func (d *Dashboard) collectAllAlerts() []interface{} {
	var allAlerts []interface{}
	
	// Collect coordinator alerts
	coordStats := d.coordinatorMonitor.GetCoordinatorStats()
	for _, alert := range coordStats.Alerts {
		allAlerts = append(allAlerts, alert)
	}
	
	// Collect worker alerts
	for workerID, monitor := range d.workerMonitors {
		alerts := monitor.CheckPerformanceAlerts()
		for _, alert := range alerts {
			allAlerts = append(allAlerts, map[string]interface{}{
				"worker_id":  workerID,
				"type":       alert.Type,
				"severity":   alert.Severity,
				"message":    alert.Message,
				"timestamp":  alert.Timestamp,
			})
		}
	}
	
	return allAlerts
}

func (d *Dashboard) generateRecommendations() []string {
	var recommendations []string
	
	// System-level recommendations
	coordStats := d.coordinatorMonitor.GetCoordinatorStats()
	if coordStats.SystemPerformance.LoadBalance < 70 {
		recommendations = append(recommendations, 
			"Poor load balancing detected. Consider redistributing queries or adding workers.")
	}
	
	if coordStats.SystemPerformance.ClusterUtilization > 85 {
		recommendations = append(recommendations,
			"High cluster utilization. Consider adding more workers.")
	}
	
	if coordStats.OptimizationMetrics.OptimizationRate < 60 {
		recommendations = append(recommendations,
			"Low optimization rate. Review query patterns and data partitioning.")
	}
	
	// Worker-level recommendations
	degradedWorkers := 0
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		if stats.Status.Status != "healthy" {
			degradedWorkers++
		}
	}
	
	if degradedWorkers > 0 {
		recommendations = append(recommendations,
			fmt.Sprintf("%d workers are not healthy. Check worker logs and resources.", degradedWorkers))
	}
	
	if len(recommendations) == 0 {
		recommendations = append(recommendations, 
			"System is performing well. Continue monitoring.")
	}
	
	return recommendations
}

// Helper methods for real-time metrics
func (d *Dashboard) getSystemCPU() float64 {
	total := 0.0
	count := 0
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		total += stats.ResourceUtilization.CPUUsagePercent
		count++
	}
	if count > 0 {
		return total / float64(count)
	}
	return 0
}

func (d *Dashboard) getSystemMemory() float64 {
	total := 0.0
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		total += stats.ResourceUtilization.MemoryUsageMB
	}
	return total
}

func (d *Dashboard) getQueryRate() float64 {
	total := 0.0
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		total += stats.Performance.QueriesPerSecond
	}
	return total
}

func (d *Dashboard) getSystemThroughput() float64 {
	total := 0.0
	for _, monitor := range d.workerMonitors {
		stats := monitor.GetWorkerStats()
		total += stats.Performance.ThroughputMBps
	}
	return total
}

func (d *Dashboard) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// MetricsExporter provides functionality to export metrics to external systems
type MetricsExporter struct {
	registry *MetricsRegistry
}

func NewMetricsExporter() *MetricsExporter {
	return &MetricsExporter{
		registry: GlobalRegistry,
	}
}

// Export metrics in Prometheus format
func (me *MetricsExporter) ExportPrometheusMetrics() string {
	metrics := me.registry.GetAllMetrics()
	var output string
	
	for _, metric := range metrics {
		// Convert metric to Prometheus format
		metricName := metric.Name
		if metric.Labels != nil && len(metric.Labels) > 0 {
			var labels []string
			for k, v := range metric.Labels {
				labels = append(labels, fmt.Sprintf(`%s="%s"`, k, v))
			}
			sort.Strings(labels)
			metricName = fmt.Sprintf("%s{%s}", metric.Name, 
				fmt.Sprintf("%s", labels))
		}
		
		switch metric.Type {
		case MetricTypeCounter, MetricTypeGauge:
			output += fmt.Sprintf("%s %v %d\n", 
				metricName, metric.Value, metric.Timestamp.Unix()*1000)
		case MetricTypeHistogram, MetricTypeTimer:
			if stats, ok := metric.Value.(HistogramStats); ok {
				output += fmt.Sprintf("%s_count %d %d\n", 
					metricName, stats.Count, metric.Timestamp.Unix()*1000)
				output += fmt.Sprintf("%s_sum %f %d\n", 
					metricName, stats.Sum, metric.Timestamp.Unix()*1000)
				
				for bucket, count := range stats.Buckets {
					output += fmt.Sprintf("%s_bucket{le=\"%s\"} %d %d\n", 
						metricName, bucket, count, metric.Timestamp.Unix()*1000)
				}
			}
		}
	}
	
	return output
}

// Export metrics in JSON format
func (me *MetricsExporter) ExportJSONMetrics() ([]byte, error) {
	metrics := me.registry.GetAllMetrics()
	return json.MarshalIndent(metrics, "", "  ")
}

// PerformanceProfiler provides detailed performance profiling
type PerformanceProfiler struct {
	profiles map[string]*ProfileData
	mutex    sync.Mutex
}

type ProfileData struct {
	StartTime    time.Time            `json:"start_time"`
	EndTime      time.Time            `json:"end_time"`
	Duration     time.Duration        `json:"duration"`
	Samples      []ProfileSample      `json:"samples"`
	Summary      ProfileSummary       `json:"summary"`
}

type ProfileSample struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
}

type ProfileSummary struct {
	TotalQueries     int64         `json:"total_queries"`
	AverageLatency   time.Duration `json:"average_latency"`
	P95Latency       time.Duration `json:"p95_latency"`
	ErrorRate        float64       `json:"error_rate"`
	ThroughputQPS    float64       `json:"throughput_qps"`
	ResourceUsage    map[string]float64 `json:"resource_usage"`
}

func NewPerformanceProfiler() *PerformanceProfiler {
	return &PerformanceProfiler{
		profiles: make(map[string]*ProfileData),
	}
}

func (pp *PerformanceProfiler) StartProfile(profileID string) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	
	pp.profiles[profileID] = &ProfileData{
		StartTime: time.Now(),
		Samples:   make([]ProfileSample, 0),
	}
}

func (pp *PerformanceProfiler) StopProfile(profileID string) *ProfileData {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	
	if profile, exists := pp.profiles[profileID]; exists {
		profile.EndTime = time.Now()
		profile.Duration = profile.EndTime.Sub(profile.StartTime)
		profile.Summary = pp.calculateSummary(profile)
		
		delete(pp.profiles, profileID)
		return profile
	}
	
	return nil
}

func (pp *PerformanceProfiler) calculateSummary(profile *ProfileData) ProfileSummary {
	// Calculate summary statistics from samples
	// This is a simplified implementation
	sampleCount := len(profile.Samples)
	
	avgLatency := time.Duration(0)
	if sampleCount > 0 {
		avgLatency = profile.Duration / time.Duration(sampleCount)
	}
	
	throughputQPS := 0.0
	if profile.Duration.Seconds() > 0 {
		throughputQPS = float64(sampleCount) / profile.Duration.Seconds()
	}
	
	return ProfileSummary{
		TotalQueries:  int64(sampleCount),
		AverageLatency: avgLatency,
		ThroughputQPS: throughputQPS,
	}
}

// Global instances
var GlobalDashboard *Dashboard
var GlobalMetricsExporter = NewMetricsExporter()
var GlobalProfiler = NewPerformanceProfiler()

const dashboardHTML = `
<!DOCTYPE html>
<html>
<head>
    <title>ByteDB Distributed Monitoring</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; margin: -20px -20px 20px -20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .metric { display: flex; justify-content: space-between; margin: 10px 0; }
        .metric-value { font-weight: bold; color: #2c3e50; }
        .status { padding: 4px 8px; border-radius: 4px; color: white; font-size: 12px; }
        .status.healthy { background: #27ae60; }
        .status.degraded { background: #f39c12; }
        .status.unhealthy { background: #e74c3c; }
        .refresh-btn { background: #3498db; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        .alert { background: #fee; border: 1px solid #fcc; padding: 10px; margin: 10px 0; border-radius: 4px; }
        #live-data { font-family: monospace; font-size: 12px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🗄️ ByteDB Distributed Query Engine Monitor</h1>
        <p>Real-time monitoring and performance analytics</p>
    </div>
    
    <div class="container">
        <button class="refresh-btn" onclick="refreshData()">🔄 Refresh Data</button>
        
        <div class="grid">
            <div class="card">
                <h3>📊 System Overview</h3>
                <div id="overview-data">Loading...</div>
            </div>
            
            <div class="card">
                <h3>🏥 Cluster Health</h3>
                <div id="cluster-data">Loading...</div>
            </div>
            
            <div class="card">
                <h3>⚡ Query Performance</h3>
                <div id="query-data">Loading...</div>
            </div>
            
            <div class="card">
                <h3>👥 Worker Status</h3>
                <div id="worker-data">Loading...</div>
            </div>
            
            <div class="card">
                <h3>🚨 Active Alerts</h3>
                <div id="alerts-data">Loading...</div>
            </div>
            
            <div class="card">
                <h3>📈 Live Metrics</h3>
                <div id="live-data">Connecting to live stream...</div>
            </div>
        </div>
    </div>

    <script>
        function refreshData() {
            loadOverview();
            loadCluster();
            loadQueries();
            loadWorkers();
            loadAlerts();
        }
        
        function loadOverview() {
            fetch('/api/overview')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('overview-data').innerHTML = ` + "`" + `
                        <div class="metric">
                            <span>Cluster Status:</span>
                            <span class="status ${data.cluster_status}">${data.cluster_status}</span>
                        </div>
                        <div class="metric">
                            <span>Total Workers:</span>
                            <span class="metric-value">${data.total_workers}</span>
                        </div>
                        <div class="metric">
                            <span>Healthy Workers:</span>
                            <span class="metric-value">${data.healthy_workers}</span>
                        </div>
                        <div class="metric">
                            <span>Total Queries:</span>
                            <span class="metric-value">${data.total_queries}</span>
                        </div>
                        <div class="metric">
                            <span>Success Rate:</span>
                            <span class="metric-value">${((data.successful_queries / data.total_queries) * 100).toFixed(1)}%</span>
                        </div>
                        <div class="metric">
                            <span>Optimization Rate:</span>
                            <span class="metric-value">${data.optimization_rate.toFixed(1)}%</span>
                        </div>
                        <div class="metric">
                            <span>Avg Response Time:</span>
                            <span class="metric-value">${data.average_response_time.toFixed(1)}ms</span>
                        </div>
                        <div class="metric">
                            <span>Uptime:</span>
                            <span class="metric-value">${data.uptime}</span>
                        </div>
                    ` + "`" + `;
                });
        }
        
        function loadCluster() {
            fetch('/api/cluster')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('cluster-data').innerHTML = ` + "`" + `
                        <div class="metric">
                            <span>Status:</span>
                            <span class="status ${data.status}">${data.status}</span>
                        </div>
                        <div class="metric">
                            <span>Total Workers:</span>
                            <span class="metric-value">${data.total_workers}</span>
                        </div>
                        <div class="metric">
                            <span>Healthy:</span>
                            <span class="metric-value">${data.healthy_workers}</span>
                        </div>
                        <div class="metric">
                            <span>Degraded:</span>
                            <span class="metric-value">${data.degraded_workers}</span>
                        </div>
                        <div class="metric">
                            <span>Offline:</span>
                            <span class="metric-value">${data.offline_workers}</span>
                        </div>
                        ${data.issues && data.issues.length > 0 ? 
                            ` + "`" + `<div class="alert">${data.issues.join('<br>')}</div>` + "`" + ` : ''}
                    ` + "`" + `;
                });
        }
        
        function loadQueries() {
            fetch('/api/queries')
                .then(r => r.json())
                .then(data => {
                    const stats = data.stats;
                    document.getElementById('query-data').innerHTML = ` + "`" + `
                        <div class="metric">
                            <span>Total Queries:</span>
                            <span class="metric-value">${stats.total_queries}</span>
                        </div>
                        <div class="metric">
                            <span>Successful:</span>
                            <span class="metric-value">${stats.successful_queries}</span>
                        </div>
                        <div class="metric">
                            <span>Failed:</span>
                            <span class="metric-value">${stats.failed_queries}</span>
                        </div>
                        <div class="metric">
                            <span>Optimized:</span>
                            <span class="metric-value">${stats.optimized_queries}</span>
                        </div>
                        <div class="metric">
                            <span>Avg Response:</span>
                            <span class="metric-value">${stats.average_response_time.toFixed(1)}ms</span>
                        </div>
                        <div class="metric">
                            <span>Optimization Rate:</span>
                            <span class="metric-value">${stats.optimization_rate.toFixed(1)}%</span>
                        </div>
                        <div class="metric">
                            <span>Avg Cost Reduction:</span>
                            <span class="metric-value">${stats.average_cost_reduction.toFixed(1)}%</span>
                        </div>
                    ` + "`" + `;
                });
        }
        
        function loadWorkers() {
            fetch('/api/workers')
                .then(r => r.json())
                .then(data => {
                    let html = '';
                    for (const [workerId, worker] of Object.entries(data)) {
                        html += ` + "`" + `
                            <div style="border: 1px solid #ddd; padding: 10px; margin: 5px 0; border-radius: 4px;">
                                <strong>${workerId}</strong>
                                <span class="status ${worker.status.status}">${worker.status.status}</span>
                                <div style="font-size: 12px; margin-top: 5px;">
                                    CPU: ${worker.resource_utilization.cpu_usage_percent.toFixed(1)}% | 
                                    Memory: ${worker.resource_utilization.memory_usage_mb.toFixed(0)}MB | 
                                    Active: ${worker.query_stats.active_queries} | 
                                    Cache: ${worker.cache_stats.hit_rate.toFixed(1)}%
                                </div>
                            </div>
                        ` + "`" + `;
                    }
                    document.getElementById('worker-data').innerHTML = html || 'No workers available';
                });
        }
        
        function loadAlerts() {
            fetch('/api/alerts')
                .then(r => r.json())
                .then(data => {
                    let html = '';
                    if (data && data.length > 0) {
                        data.forEach(alert => {
                            html += ` + "`" + `
                                <div class="alert">
                                    <strong>${alert.severity.toUpperCase()}:</strong> ${alert.message}
                                    ${alert.worker_id ? ` + "`" + ` (${alert.worker_id})` + "`" + ` : ''}
                                </div>
                            ` + "`" + `;
                        });
                    } else {
                        html = '<div style="color: #27ae60;">✅ No active alerts</div>';
                    }
                    document.getElementById('alerts-data').innerHTML = html;
                });
        }
        
        // Setup live streaming
        function setupLiveStream() {
            const eventSource = new EventSource('/api/stream');
            eventSource.onmessage = function(event) {
                const data = JSON.parse(event.data);
                document.getElementById('live-data').innerHTML = ` + "`" + `
                    <div><strong>Live Update:</strong> ${new Date().toLocaleTimeString()}</div>
                    <div>CPU: ${data.system_metrics.cpu_usage.toFixed(1)}%</div>
                    <div>Memory: ${data.system_metrics.memory_usage.toFixed(0)}MB</div>
                    <div>Query Rate: ${data.system_metrics.query_rate.toFixed(2)} QPS</div>
                    <div>Throughput: ${data.system_metrics.throughput.toFixed(2)} MB/s</div>
                ` + "`" + `;
            };
        }
        
        // Initialize dashboard
        refreshData();
        setupLiveStream();
        setInterval(refreshData, 10000); // Refresh every 10 seconds
    </script>
</body>
</html>
`