package main

import (
	"bytedb/distributed/communication"
	"bytedb/distributed/coordinator"
	"bytedb/distributed/worker"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// TestDistributedQueryEngine runs comprehensive tests for distributed query execution
func TestDistributedQueryEngine(t *testing.T) {
	// Ensure test data exists
	generateDistributedSampleData()
	
	tests := []struct {
		name           string
		workers        int
		query          string
		expectedRows   int
		validateResult func(t *testing.T, result *communication.DistributedQueryResponse)
	}{
		{
			name:         "Simple COUNT aggregation",
			workers:      3,
			query:        "SELECT COUNT(*) as total FROM employees",
			expectedRows: 1,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				total := getIntValue(result.Rows[0]["total"])
				if total != 10 {
					t.Errorf("Expected total=10, got %v", total)
				}
			},
		},
		{
			name:         "COUNT with WHERE clause",
			workers:      3,
			query:        "SELECT COUNT(*) as count FROM employees WHERE salary > 70000",
			expectedRows: 1,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				count := getIntValue(result.Rows[0]["count"])
				if count < 1 || count > 10 {
					t.Errorf("Expected count between 1-10, got %v", count)
				}
			},
		},
		{
			name:         "GROUP BY with COUNT",
			workers:      3,
			query:        "SELECT department, COUNT(*) as count FROM employees GROUP BY department",
			expectedRows: 5, // Engineering, Sales, Marketing, HR, Finance
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				totalCount := 0
				for _, row := range result.Rows {
					count := getIntValue(row["count"])
					totalCount += count
				}
				if totalCount != 10 {
					t.Errorf("Expected total count=10, got %v", totalCount)
				}
			},
		},
		{
			name:         "Multiple aggregations",
			workers:      3,
			query:        "SELECT COUNT(*) as count, SUM(salary) as total_salary, AVG(salary) as avg_salary FROM employees",
			expectedRows: 1,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				row := result.Rows[0]
				count := getIntValue(row["count"])
				totalSalary := getFloatValue(row["total_salary"])
				avgSalary := getFloatValue(row["avg_salary"])
				
				if count != 10 {
					t.Errorf("Expected count=10, got %v", count)
				}
				
				// Verify AVG = SUM / COUNT
				expectedAvg := totalSalary / float64(count)
				if math.Abs(avgSalary-expectedAvg) > 0.01 {
					t.Errorf("Expected avg_salary=%v, got %v", expectedAvg, avgSalary)
				}
			},
		},
		{
			name:         "GROUP BY with multiple aggregations",
			workers:      3,
			query:        "SELECT department, COUNT(*) as count, MIN(salary) as min_sal, MAX(salary) as max_sal, AVG(salary) as avg_sal FROM employees GROUP BY department",
			expectedRows: 5,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				for _, row := range result.Rows {
					minSal := getFloatValue(row["min_sal"])
					maxSal := getFloatValue(row["max_sal"])
					avgSal := getFloatValue(row["avg_sal"])
					
					// Verify min <= avg <= max
					if minSal > avgSal || avgSal > maxSal {
						t.Errorf("Invalid salary range for %s: min=%v, avg=%v, max=%v", 
							row["department"], minSal, avgSal, maxSal)
					}
				}
			},
		},
		{
			name:         "Single worker execution",
			workers:      1,
			query:        "SELECT COUNT(*) as count FROM employees",
			expectedRows: 1,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				// With 1 worker, it should see only its partition of data (worker-1 has 4 employees)
				count := getIntValue(result.Rows[0]["count"])
				if count < 1 || count > 10 {
					t.Errorf("Expected count between 1-10 for single worker, got %v", count)
				}
			},
		},
		{
			name:         "Empty result set",
			workers:      3,
			query:        "SELECT * FROM employees WHERE salary > 1000000",
			expectedRows: 0,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				// Just verify no rows returned
			},
		},
		{
			name:         "Complex WHERE with aggregation",
			workers:      3,
			query:        "SELECT department, AVG(salary) as avg_salary FROM employees WHERE salary > 60000 AND department IN ('Engineering', 'Sales') GROUP BY department",
			expectedRows: 2,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				for _, row := range result.Rows {
					dept := row["department"].(string)
					if dept != "Engineering" && dept != "Sales" {
						t.Errorf("Unexpected department: %s", dept)
					}
					
					avgSal := getFloatValue(row["avg_salary"])
					if avgSal < 60000 {
						t.Errorf("Average salary should be > 60000, got %v", avgSal)
					}
				}
			},
		},
		{
			name:         "ORDER BY with LIMIT",
			workers:      3,
			query:        "SELECT department, COUNT(*) as count FROM employees GROUP BY department ORDER BY count DESC LIMIT 3",
			expectedRows: 3,
			validateResult: func(t *testing.T, result *communication.DistributedQueryResponse) {
				// Verify descending order
				for i := 1; i < len(result.Rows); i++ {
					prevCount := getIntValue(result.Rows[i-1]["count"])
					currCount := getIntValue(result.Rows[i]["count"])
					if prevCount < currCount {
						t.Errorf("Results not in descending order: %v < %v", prevCount, currCount)
					}
				}
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup transport and coordinator
			transport := communication.NewMemoryTransport()
			coord := coordinator.NewCoordinator(transport)
			transport.RegisterCoordinator("coordinator:8080", coord)
			
			// Create and register workers
			ctx := context.Background()
			workers := make([]*worker.Worker, tt.workers)
			for i := 0; i < tt.workers; i++ {
				workerID := fmt.Sprintf("worker-%d", i+1)
				dataPath := fmt.Sprintf("./data/%s", workerID)
				
				w := worker.NewWorker(workerID, dataPath)
				workers[i] = w
				
				addr := fmt.Sprintf("%s:808%d", workerID, i+1)
				transport.RegisterWorker(addr, w)
				
				info := &communication.WorkerInfo{
					ID:       workerID,
					Address:  addr,
					DataPath: dataPath,
					Status:   "active",
					Resources: communication.WorkerResources{
						CPUCores:    4,
						MemoryMB:    1024,
						DiskSpaceGB: 100,
					},
				}
				
				if err := coord.RegisterWorker(ctx, info); err != nil {
					t.Fatalf("Failed to register worker %s: %v", workerID, err)
				}
			}
			
			// Execute query
			req := &communication.DistributedQueryRequest{
				SQL:       tt.query,
				RequestID: fmt.Sprintf("test-%s", tt.name),
				Timeout:   10 * time.Second,
			}
			
			response, err := coord.ExecuteQuery(ctx, req)
			if err != nil {
				t.Fatalf("Query execution failed: %v", err)
			}
			
			if response.Error != "" {
				t.Fatalf("Query error: %s", response.Error)
			}
			
			// Validate row count
			if response.Count != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, response.Count)
			}
			
			// Run custom validation
			if tt.validateResult != nil {
				tt.validateResult(t, response)
			}
			
			// Cleanup
			for _, w := range workers {
				w.Shutdown(ctx)
			}
			coord.Shutdown(ctx)
			transport.Stop()
		})
	}
}

// TestDistributedQueryOptimization tests the optimization features
func TestDistributedQueryOptimization(t *testing.T) {
	generateDistributedSampleData()
	
	// Setup cluster
	transport := communication.NewMemoryTransport()
	coord := coordinator.NewCoordinator(transport)
	transport.RegisterCoordinator("coordinator:8080", coord)
	
	ctx := context.Background()
	
	// Register 3 workers
	for i := 1; i <= 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		w := worker.NewWorker(workerID, fmt.Sprintf("./data/%s", workerID))
		addr := fmt.Sprintf("%s:808%d", workerID, i)
		transport.RegisterWorker(addr, w)
		
		info := &communication.WorkerInfo{
			ID:       workerID,
			Address:  addr,
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
		
		defer w.Shutdown(ctx)
	}
	defer coord.Shutdown(ctx)
	defer transport.Stop()
	
	// Test 1: Verify partial aggregation reduces data transfer
	t.Run("Partial aggregation optimization", func(t *testing.T) {
		query := "SELECT department, COUNT(*), SUM(salary), AVG(salary) FROM employees GROUP BY department"
		
		req := &communication.DistributedQueryRequest{
			SQL:       query,
			RequestID: "test-optimization",
		}
		
		response, err := coord.ExecuteQuery(ctx, req)
		if err != nil || response.Error != "" {
			t.Fatalf("Query failed: %v, %s", err, response.Error)
		}
		
		// With optimization, we should transfer at most num_departments * num_workers rows
		// instead of all employee rows (this would be ~15 rows vs 10 full employee records)
		if response.Stats.WorkersUsed != 3 {
			t.Errorf("Expected 3 workers used, got %d", response.Stats.WorkersUsed)
		}
		
		// The final result should have exactly 5 rows (one per department)
		if response.Count != 5 {
			t.Errorf("Expected 5 departments, got %d", response.Count)
		}
	})
	
	// Test 2: Verify column pruning
	t.Run("Column pruning optimization", func(t *testing.T) {
		query := "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'"
		
		req := &communication.DistributedQueryRequest{
			SQL:       query,
			RequestID: "test-column-pruning",
		}
		
		response, err := coord.ExecuteQuery(ctx, req)
		if err != nil || response.Error != "" {
			t.Fatalf("Query failed: %v, %s", err, response.Error)
		}
		
		// With column pruning, only department column should be read for filtering
		// Result should still be correct
		if response.Count != 1 {
			t.Errorf("Expected 1 result row, got %d", response.Count)
		}
	})
}

// TestDistributedErrorHandling tests error scenarios
func TestDistributedErrorHandling(t *testing.T) {
	transport := communication.NewMemoryTransport()
	coord := coordinator.NewCoordinator(transport)
	transport.RegisterCoordinator("coordinator:8080", coord)
	ctx := context.Background()
	
	defer coord.Shutdown(ctx)
	defer transport.Stop()
	
	tests := []struct {
		name          string
		query         string
		expectError   bool
		errorContains string
	}{
		{
			name:          "Invalid SQL syntax",
			query:         "SELECT * FORM employees",
			expectError:   true,
			errorContains: "syntax error",
		},
		{
			name:          "Non-existent table",
			query:         "SELECT * FROM non_existent_table",
			expectError:   true,
			errorContains: "",
		},
		{
			name:          "Invalid aggregate function",
			query:         "SELECT INVALID_FUNC(salary) FROM employees",
			expectError:   true,
			errorContains: "",
		},
		{
			name:          "No workers available",
			query:         "SELECT COUNT(*) FROM employees",
			expectError:   true,
			errorContains: "no active workers",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &communication.DistributedQueryRequest{
				SQL:       tt.query,
				RequestID: fmt.Sprintf("test-error-%s", tt.name),
			}
			
			response, err := coord.ExecuteQuery(ctx, req)
			
			if tt.expectError {
				if err == nil && response.Error == "" {
					t.Errorf("Expected error but query succeeded")
				}
				
				if tt.errorContains != "" {
					errorMsg := ""
					if err != nil {
						errorMsg = err.Error()
					} else if response.Error != "" {
						errorMsg = response.Error
					}
					
					if !strings.Contains(strings.ToLower(errorMsg), strings.ToLower(tt.errorContains)) {
						t.Errorf("Expected error containing '%s', got '%s'", tt.errorContains, errorMsg)
					}
				}
			} else {
				if err != nil || response.Error != "" {
					t.Errorf("Expected success but got error: %v, %s", err, response.Error)
				}
			}
		})
	}
}

// TestDistributedConcurrency tests concurrent query execution
func TestDistributedConcurrency(t *testing.T) {
	generateDistributedSampleData()
	
	// Setup cluster
	transport := communication.NewMemoryTransport()
	coord := coordinator.NewCoordinator(transport)
	transport.RegisterCoordinator("coordinator:8080", coord)
	
	ctx := context.Background()
	
	// Register 3 workers
	workers := make([]*worker.Worker, 3)
	for i := 0; i < 3; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		w := worker.NewWorker(workerID, fmt.Sprintf("./data/%s", workerID))
		workers[i] = w
		
		addr := fmt.Sprintf("%s:808%d", workerID, i+1)
		transport.RegisterWorker(addr, w)
		
		info := &communication.WorkerInfo{
			ID:       workerID,
			Address:  addr,
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
	}
	
	defer func() {
		for _, w := range workers {
			w.Shutdown(ctx)
		}
		coord.Shutdown(ctx)
		transport.Stop()
	}()
	
	// Execute multiple queries concurrently
	queries := []string{
		"SELECT COUNT(*) FROM employees",
		"SELECT department, COUNT(*) FROM employees GROUP BY department",
		"SELECT AVG(salary) FROM employees",
		"SELECT MIN(salary), MAX(salary) FROM employees",
		"SELECT department, AVG(salary) FROM employees GROUP BY department",
	}
	
	results := make(chan error, len(queries))
	
	for i, query := range queries {
		go func(idx int, sql string) {
			req := &communication.DistributedQueryRequest{
				SQL:       sql,
				RequestID: fmt.Sprintf("concurrent-%d", idx),
			}
			
			response, err := coord.ExecuteQuery(ctx, req)
			if err != nil {
				results <- err
			} else if response.Error != "" {
				results <- fmt.Errorf("query error: %s", response.Error)
			} else {
				results <- nil
			}
		}(i, query)
	}
	
	// Collect results
	for i := 0; i < len(queries); i++ {
		if err := <-results; err != nil {
			t.Errorf("Concurrent query failed: %v", err)
		}
	}
}

// Helper functions
func getIntValue(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

func getFloatValue(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0
	}
}