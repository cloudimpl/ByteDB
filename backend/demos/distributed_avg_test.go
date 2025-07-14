package main

import (
	"bytedb/distributed/communication"
	"bytedb/distributed/coordinator"
	"bytedb/distributed/worker"
	"context"
	"fmt"
	"testing"
)

func TestDistributedAVGFix(t *testing.T) {
	// Ensure test data exists
	generateDistributedSampleData()
	
	// Setup
	transport := communication.NewMemoryTransport()
	coord := coordinator.NewCoordinator(transport)
	coordAddr := "coordinator:8080"
	transport.RegisterCoordinator(coordAddr, coord)
	
	// Create one worker
	w := worker.NewWorker("worker-1", "./data/worker-1")
	workerAddr := "worker-1:8081"
	transport.RegisterWorker(workerAddr, w)
	
	// Register worker
	ctx := context.Background()
	info := &communication.WorkerInfo{
		ID:       w.GetID(),
		Address:  workerAddr,
		DataPath: w.GetDataPath(),
		Status:   "active",
		Resources: communication.WorkerResources{
			CPUCores:    4,
			MemoryMB:    1024,
			DiskSpaceGB: 100,
		},
	}
	
	if err := coord.RegisterWorker(ctx, info); err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}
	
	// Execute AVG query
	req := &communication.DistributedQueryRequest{
		SQL:       "SELECT department, AVG(salary) as avg_salary FROM employees WHERE salary > 60000 GROUP BY department",
		RequestID: "test-avg-fix",
	}
	
	response, err := coord.ExecuteQuery(ctx, req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	
	if response.Error != "" {
		t.Fatalf("Query error: %s", response.Error)
	}
	
	// Check results
	fmt.Printf("\n=== AVG Query Results ===\n")
	fmt.Printf("Columns: %v\n", response.Columns)
	fmt.Printf("Row count: %d\n", response.Count)
	
	for i, row := range response.Rows {
		fmt.Printf("Row %d: %v\n", i+1, row)
	}
	
	// Verify columns are correct
	expectedCols := []string{"department", "avg_salary"}
	if len(response.Columns) != len(expectedCols) {
		t.Errorf("Expected columns %v, got %v", expectedCols, response.Columns)
	}
	
	for i, col := range expectedCols {
		if i < len(response.Columns) && response.Columns[i] != col {
			t.Errorf("Expected column[%d] = %s, got %s", i, col, response.Columns[i])
		}
	}
	
	// Check that we have avg_salary, not intermediate columns
	for _, row := range response.Rows {
		if _, hasSum := row["avg_salary_sum"]; hasSum {
			t.Error("Result contains intermediate column avg_salary_sum")
		}
		if _, hasCount := row["avg_salary_count"]; hasCount {
			t.Error("Result contains intermediate column avg_salary_count")
		}
		if _, hasAvg := row["avg_salary"]; !hasAvg {
			t.Error("Result missing avg_salary column")
		}
	}
	
	// Cleanup
	w.Shutdown(ctx)
	coord.Shutdown(ctx)
	transport.Stop()
}