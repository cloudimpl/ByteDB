package main

import (
	"bytedb/core"
	"bytedb/distributed/communication"
	"bytedb/distributed/coordinator"
	"bytedb/distributed/worker"
	"context"
	"fmt"
	"log"
	"os"
	"time"
	
	"github.com/parquet-go/parquet-go"
)

// DistributedDemo demonstrates the distributed query execution
func DistributedDemo() {
	fmt.Println("\n🚀 ByteDB Distributed Query Engine Demo")
	fmt.Println("=====================================")
	
	// Ensure test data exists and is distributed
	generateDistributedSampleData()
	
	// Create memory transport for demo
	transport := communication.NewMemoryTransport()
	
	// Create coordinator
	coord := coordinator.NewCoordinator(transport)
	coordAddr := "coordinator:8080"
	transport.RegisterCoordinator(coordAddr, coord)
	
	fmt.Println("📡 Starting distributed cluster...")
	
	// Create and register workers with separate data directories
	workers := []*worker.Worker{
		worker.NewWorker("worker-1", "./data/worker-1"),
		worker.NewWorker("worker-2", "./data/worker-2"),
		worker.NewWorker("worker-3", "./data/worker-3"),
	}
	
	workerAddrs := []string{
		"worker-1:8081",
		"worker-2:8082", 
		"worker-3:8083",
	}
	
	// Register workers with transport
	for i, w := range workers {
		transport.RegisterWorker(workerAddrs[i], w)
	}
	
	// Register workers with coordinator
	ctx := context.Background()
	for i, w := range workers {
		info := &communication.WorkerInfo{
			ID:       w.GetID(),
			Address:  workerAddrs[i],
			DataPath: w.GetDataPath(),
			Status:   "active",
			Resources: communication.WorkerResources{
				CPUCores:    4,
				MemoryMB:    1024,
				DiskSpaceGB: 100,
			},
		}
		
		if err := coord.RegisterWorker(ctx, info); err != nil {
			log.Fatalf("Failed to register worker %s: %v", w.GetID(), err)
		}
		fmt.Printf("✅ Registered worker: %s\n", w.GetID())
	}
	
	// Show cluster status
	status, err := coord.GetClusterStatus(ctx)
	if err != nil {
		log.Fatalf("Failed to get cluster status: %v", err)
	}
	
	fmt.Printf("\n📊 Cluster Status:\n")
	fmt.Printf("   Total Workers: %d\n", status.TotalWorkers)
	fmt.Printf("   Active Workers: %d\n", status.ActiveWorkers)
	
	// Execute various distributed queries
	queries := []struct {
		name        string
		sql         string
		description string
	}{
		{
			name:        "Simple SELECT",
			sql:         "SELECT name, department FROM employees LIMIT 5",
			description: "Basic distributed scan across all workers",
		},
		{
			name:        "COUNT Aggregation",
			sql:         "SELECT COUNT(*) as total_employees FROM employees",
			description: "Distributed count aggregation",
		},
		{
			name:        "WHERE Filter",
			sql:         "SELECT name, salary FROM employees WHERE department = 'Engineering'",
			description: "Distributed filtering with WHERE clause",
		},
		{
			name:        "GROUP BY",
			sql:         "SELECT department, COUNT(*) as count FROM employees GROUP BY department",
			description: "Distributed grouping and aggregation",
		},
		{
			name:        "Complex Query",
			sql:         "SELECT department, AVG(salary) as avg_salary FROM employees WHERE salary > 60000 GROUP BY department",
			description: "Complex distributed query with filtering, grouping, and aggregation",
		},
	}
	
	fmt.Printf("\n🔍 Executing Distributed Queries:\n")
	fmt.Printf("================================\n")
	
	for i, query := range queries {
		fmt.Printf("\n%d. %s\n", i+1, query.name)
		fmt.Printf("   SQL: %s\n", query.sql)
		fmt.Printf("   Description: %s\n", query.description)
		
		startTime := time.Now()
		
		req := &communication.DistributedQueryRequest{
			SQL:       query.sql,
			RequestID: fmt.Sprintf("demo-query-%d", i+1),
			Timeout:   30 * time.Second,
		}
		
		response, err := coord.ExecuteQuery(ctx, req)
		if err != nil {
			fmt.Printf("   ❌ Error: %v\n", err)
			continue
		}
		
		if response.Error != "" {
			fmt.Printf("   ❌ Query Error: %s\n", response.Error)
			continue
		}
		
		duration := time.Since(startTime)
		
		fmt.Printf("   ✅ Success!\n")
		fmt.Printf("   📈 Results: %d rows in %v\n", response.Count, duration)
		fmt.Printf("   🔧 Workers Used: %d\n", response.Stats.WorkersUsed)
		fmt.Printf("   📊 Fragments: %d\n", response.Stats.TotalFragments)
		
		// Show first few result rows
		if len(response.Rows) > 0 {
			fmt.Printf("   📋 Sample Results:\n")
			fmt.Printf("      Columns: %v\n", response.Columns)
			
			maxRows := 3
			if len(response.Rows) < maxRows {
				maxRows = len(response.Rows)
			}
			
			for j := 0; j < maxRows; j++ {
				fmt.Printf("      Row %d: %v\n", j+1, response.Rows[j])
			}
			
			if len(response.Rows) > maxRows {
				fmt.Printf("      ... and %d more rows\n", len(response.Rows)-maxRows)
			}
		}
	}
	
	// Performance comparison
	fmt.Printf("\n⚡ Performance Comparison:\n")
	fmt.Printf("=========================\n")
	
	// Single node query
	singleEngine := NewTestQueryEngine()
	defer singleEngine.Close()
	
	testSQL := "SELECT department, COUNT(*) as count FROM employees GROUP BY department"
	
	// Single node timing
	singleStart := time.Now()
	_, err = singleEngine.Execute(testSQL)
	singleDuration := time.Since(singleStart)
	
	if err != nil {
		fmt.Printf("Single node error: %v\n", err)
	} else {
		fmt.Printf("🔄 Single Node: %v\n", singleDuration)
	}
	
	// Distributed timing
	distributedStart := time.Now()
	req := &communication.DistributedQueryRequest{
		SQL:       testSQL,
		RequestID: "perf-comparison",
		Timeout:   30 * time.Second,
	}
	
	response, err := coord.ExecuteQuery(ctx, req)
	distributedDuration := time.Since(distributedStart)
	
	if err != nil {
		fmt.Printf("Distributed error: %v\n", err)
	} else {
		fmt.Printf("🌐 Distributed: %v (across %d workers)\n", distributedDuration, response.Stats.WorkersUsed)
		
		if singleDuration > 0 {
			if distributedDuration < singleDuration {
				speedup := float64(singleDuration) / float64(distributedDuration)
				fmt.Printf("🚀 Speedup: %.2fx faster with distributed execution\n", speedup)
			} else {
				overhead := float64(distributedDuration) / float64(singleDuration)
				fmt.Printf("📊 Overhead: %.2fx due to distribution (expected for small datasets)\n", overhead)
			}
		}
	}
	
	// Cleanup
	fmt.Printf("\n🧹 Cleaning up...\n")
	
	for _, w := range workers {
		w.Shutdown(ctx)
	}
	
	coord.Shutdown(ctx)
	transport.Stop()
	
	fmt.Printf("✅ Demo completed successfully!\n")
	fmt.Printf("\n💡 Key Features Demonstrated:\n")
	fmt.Printf("   - Pluggable communication layer (in-memory for testing)\n")
	fmt.Printf("   - Automatic query fragmentation and distribution\n")
	fmt.Printf("   - Result aggregation across multiple workers\n")
	fmt.Printf("   - Health monitoring and cluster management\n")
	fmt.Printf("   - Support for complex SQL operations\n")
	fmt.Printf("   - Easy integration with existing ByteDB core\n")
	
	fmt.Printf("\n🔧 Next Steps:\n")
	fmt.Printf("   - Add gRPC transport for network communication\n")
	fmt.Printf("   - Implement data partitioning strategies\n")
	fmt.Printf("   - Add fault tolerance and recovery\n")
	fmt.Printf("   - Optimize query planning for distributed execution\n")
}

// generateDistributedSampleData generates sample data distributed across worker directories
func generateDistributedSampleData() {
	// Create worker directories
	workerDirs := []string{
		"./data/worker-1",
		"./data/worker-2",
		"./data/worker-3",
	}
	
	for _, dir := range workerDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}
	
	// First, generate complete data in main data directory
	generateSampleData()
	
	// Now distribute the data across workers
	distributeEmployeeData(workerDirs)
	
	fmt.Println("Sample data distributed across worker directories")
}

// distributeEmployeeData distributes employee data across worker directories
func distributeEmployeeData(workerDirs []string) {
	// Read the original employees data
	engine := NewTestQueryEngine()
	defer engine.Close()
	
	// Get all employees
	result, err := engine.Execute("SELECT * FROM employees")
	if err != nil {
		log.Fatalf("Failed to read employees: %v", err)
	}
	
	// Create separate parquet writers for each worker
	writers := make([]*parquet.Writer, len(workerDirs))
	files := make([]*os.File, len(workerDirs))
	
	for i, dir := range workerDirs {
		file, err := os.Create(fmt.Sprintf("%s/employees.parquet", dir))
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		files[i] = file
		
		writer := parquet.NewWriter(file, parquet.SchemaOf(&core.Employee{}))
		writers[i] = writer
	}
	
	// Distribute rows using round-robin partitioning
	for i, row := range result.Rows {
		workerIndex := i % len(workerDirs)
		
		// Convert row to Employee struct with proper type handling
		emp := core.Employee{
			ID:         getInt32(row["id"]),
			Name:       row["name"].(string),
			Department: row["department"].(string),
			Salary:     row["salary"].(float64),
			Age:        getInt32(row["age"]),
			HireDate:   row["hire_date"].(string),
		}
		
		if err := writers[workerIndex].Write(&emp); err != nil {
			log.Fatalf("Failed to write employee: %v", err)
		}
	}
	
	// Close all writers and files
	for i := range writers {
		if err := writers[i].Close(); err != nil {
			log.Fatalf("Failed to close writer: %v", err)
		}
		files[i].Close()
	}
	
	// Also copy other tables to each worker (for simplicity, we'll replicate small tables)
	for _, dir := range workerDirs {
		// Copy departments (small table, can be replicated)
		copyFile("./data/departments.parquet", fmt.Sprintf("%s/departments.parquet", dir))
		// Copy products (for join tests)
		copyFile("./data/products.parquet", fmt.Sprintf("%s/products.parquet", dir))
	}
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	
	err = os.WriteFile(dst, input, 0644)
	if err != nil {
		return err
	}
	
	return nil
}

// getInt32 safely converts interface{} to int32
func getInt32(val interface{}) int32 {
	switch v := val.(type) {
	case int32:
		return v
	case int64:
		return int32(v)
	case int:
		return int32(v)
	default:
		return 0
	}
}

// Run the demo if this file is executed directly
func init() {
	// This will run automatically when the package is imported
	// but you can also call DistributedDemo() directly from tests or main
}