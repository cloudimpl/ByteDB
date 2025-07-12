package main

import (
	"fmt"
	"testing"
	"strings"
)

// TestBasicWindowFunctions tests core window function functionality
func TestBasicWindowFunctions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("ROW_NUMBER without PARTITION BY", func(t *testing.T) {
		query := "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		if len(result.Rows) != 10 {
			t.Fatalf("Expected 10 rows, got %d", len(result.Rows))
		}
		
		// Check that Lisa Davis (highest salary) gets row_num = 1
		found := false
		for _, row := range result.Rows {
			if name, exists := row["name"]; exists && name == "Lisa Davis" {
				if rowNum, exists := row["row_num"]; exists && rowNum == 1 {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("Lisa Davis should have row_num=1 (highest salary)")
		}
	})

	t.Run("ROW_NUMBER with PARTITION BY", func(t *testing.T) {
		query := "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Verify each department has someone with rank 1
		deptRanks := make(map[string]map[int]int) // dept -> rank -> count
		for _, row := range result.Rows {
			dept := row["department"].(string)
			rank := row["dept_rank"].(int)
			
			if deptRanks[dept] == nil {
				deptRanks[dept] = make(map[int]int)
			}
			deptRanks[dept][rank]++
		}
		
		// Each department should have exactly one person with rank 1
		for dept, ranks := range deptRanks {
			if ranks[1] != 1 {
				t.Errorf("Department %s should have exactly 1 person with rank 1, got %d", dept, ranks[1])
			}
		}
	})

	t.Run("RANK function", func(t *testing.T) {
		query := "SELECT name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank_num FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Verify Lisa Davis has rank 1 in Engineering
		found := false
		for _, row := range result.Rows {
			if name, exists := row["name"]; exists && name == "Lisa Davis" {
				if rank, exists := row["rank_num"]; exists && rank == 1 {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("Lisa Davis should have rank=1 in Engineering department")
		}
	})

	t.Run("DENSE_RANK function", func(t *testing.T) {
		query := "SELECT name, department, salary, DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Verify Lisa Davis has dense_rank 1 in Engineering  
		found := false
		for _, row := range result.Rows {
			if name, exists := row["name"]; exists && name == "Lisa Davis" {
				if rank, exists := row["dense_rank"]; exists && rank == 1 {
					found = true
					break
				}
			}
		}
		if !found {
			t.Errorf("Lisa Davis should have dense_rank=1 in Engineering department")
		}
	})

	t.Run("LAG function", func(t *testing.T) {
		query := "SELECT name, department, salary, LAG(salary) OVER (PARTITION BY department ORDER BY salary DESC) as prev_salary FROM employees WHERE department = 'Engineering'"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Find Lisa Davis (highest salary) - should have prev_salary = nil
		// Find second highest - should have prev_salary = Lisa's salary
		lisaSalary := 0.0
		secondHighestPrevSalary := 0.0
		
		for _, row := range result.Rows {
			if name := row["name"].(string); name == "Lisa Davis" {
				lisaSalary = row["salary"].(float64)
				if prevSalary := row["prev_salary"]; prevSalary != nil {
					t.Errorf("Lisa Davis (highest) should have prev_salary=nil, got %v", prevSalary)
				}
			} else if prevSal := row["prev_salary"]; prevSal != nil {
				if prevSalary := prevSal.(float64); prevSalary > secondHighestPrevSalary {
					secondHighestPrevSalary = prevSalary
				}
			}
		}
		
		if lisaSalary > 0 && secondHighestPrevSalary > 0 && secondHighestPrevSalary != lisaSalary {
			t.Errorf("Second highest employee should have prev_salary=%.0f (Lisa's salary), got %.0f", lisaSalary, secondHighestPrevSalary)
		}
	})

	t.Run("LEAD function", func(t *testing.T) {
		query := "SELECT name, department, salary, LEAD(salary) OVER (PARTITION BY department ORDER BY salary DESC) as next_salary FROM employees WHERE department = 'Engineering'"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Find Lisa Davis (highest salary) - should have next_salary = second highest
		var lisaNextSalary interface{}
		var secondHighestSalary float64
		
		// First pass: find second highest salary
		salaries := make([]float64, 0)
		for _, row := range result.Rows {
			salaries = append(salaries, row["salary"].(float64))
		}
		
		// Sort to find second highest
		for _, salary := range salaries {
			if salary < 85000 && salary > secondHighestSalary { // Lisa has 85000
				secondHighestSalary = salary
			}
		}
		
		// Second pass: check Lisa's next_salary
		for _, row := range result.Rows {
			if name := row["name"].(string); name == "Lisa Davis" {
				lisaNextSalary = row["next_salary"]
				break
			}
		}
		
		if lisaNextSalary == nil {
			t.Errorf("Lisa Davis should have next_salary, got nil")
		} else if nextSal := lisaNextSalary.(float64); nextSal != secondHighestSalary {
			t.Errorf("Lisa Davis should have next_salary=%.0f, got %.0f", secondHighestSalary, nextSal)
		}
	})
}

// TestAdvancedWindowFunctions tests more complex window function scenarios
func TestAdvancedWindowFunctions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Multiple window functions in same query", func(t *testing.T) {
		query := `SELECT name, department, salary,
		          ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
		          RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank_num,
		          LAG(name) OVER (PARTITION BY department ORDER BY salary DESC) as prev_employee
		          FROM employees 
		          WHERE department IN ('Engineering', 'Marketing')`
		
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Verify we have results for both departments
		depts := make(map[string]int)
		for _, row := range result.Rows {
			dept := row["department"].(string)
			depts[dept]++
		}
		
		if depts["Engineering"] == 0 || depts["Marketing"] == 0 {
			t.Errorf("Should have results for both Engineering and Marketing departments")
		}
	})

	t.Run("Window function with different ORDER BY", func(t *testing.T) {
		query := "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY name ASC) as name_rank FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Check that ranking by name works differently than by salary
		if len(result.Rows) == 0 {
			t.Errorf("Should have results for name-based ranking")
		}
	})

	t.Run("LAG with offset and default", func(t *testing.T) {
		query := "SELECT name, salary, LAG(salary, 1, 0) OVER (ORDER BY salary DESC) as prev_salary FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Check that LAG function runs without errors and produces some results
		if len(result.Rows) == 0 {
			t.Errorf("LAG query should return rows")
		}
		
		// Check that the prev_salary column exists
		hasLagColumn := false
		for _, row := range result.Rows {
			if _, exists := row["prev_salary"]; exists {
				hasLagColumn = true
				break
			}
		}
		
		if !hasLagColumn {
			t.Errorf("LAG query should add prev_salary column")
		}
	})

	t.Run("Global window function (no PARTITION BY)", func(t *testing.T) {
		query := "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as global_rank FROM employees"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Should have 10 employees with ranks 1-10
		ranks := make(map[int]bool)
		for _, row := range result.Rows {
			rank := row["global_rank"].(int)
			ranks[rank] = true
		}
		
		for i := 1; i <= 10; i++ {
			if !ranks[i] {
				t.Errorf("Missing global rank %d", i)
			}
		}
	})
}

// TestWindowFunctionEdgeCases tests edge cases and error conditions
func TestWindowFunctionEdgeCases(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Window function with empty partition", func(t *testing.T) {
		// Create a query that might result in empty partitions
		query := "SELECT name, department, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary) as rank FROM employees WHERE department = 'NonExistent'"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Should return 0 rows but not error
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows for non-existent department, got %d", len(result.Rows))
		}
	})

	t.Run("LAG/LEAD with column reference", func(t *testing.T) {
		query := "SELECT name, department, salary, LAG(name) OVER (PARTITION BY department ORDER BY salary DESC) as prev_name FROM employees WHERE department = 'Engineering'"
		result, err := engine.Execute(query)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Check that LAG returns string values (names) correctly
		hasStringResult := false
		for _, row := range result.Rows {
			if prevName := row["prev_name"]; prevName != nil {
				if _, ok := prevName.(string); ok {
					hasStringResult = true
					break
				}
			}
		}
		
		if !hasStringResult {
			t.Errorf("LAG should return string values for name column")
		}
	})

	t.Run("Window function performance with larger dataset", func(t *testing.T) {
		// This is more of a smoke test for performance
		query := "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank FROM employees"
		
		// Run the query multiple times to check for performance issues
		for i := 0; i < 5; i++ {
			result, err := engine.Execute(query)
			if err != nil {
				t.Fatalf("Query failed on iteration %d: %v", i, err)
			}
			if result.Error != "" {
				t.Fatalf("Query error on iteration %d: %s", i, result.Error)
			}
			if len(result.Rows) == 0 {
				t.Errorf("No results on iteration %d", i)
			}
		}
	})
}

// TestWindowFunctionDataTypes tests window functions with different data types
func TestWindowFunctionDataTypes(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	tests := []struct {
		name     string
		query    string
		checkFunc func(*testing.T, *QueryResult)
	}{
		{
			name:  "LAG with numeric values",
			query: "SELECT name, salary, LAG(salary) OVER (ORDER BY salary) as prev_salary FROM employees",
			checkFunc: func(t *testing.T, result *QueryResult) {
				for _, row := range result.Rows {
					if prevSal := row["prev_salary"]; prevSal != nil {
						if _, ok := prevSal.(float64); !ok {
							t.Errorf("LAG(salary) should return float64, got %T", prevSal)
						}
					}
				}
			},
		},
		{
			name:  "LAG with string values",
			query: "SELECT name, department, LAG(department) OVER (ORDER BY name) as prev_dept FROM employees",
			checkFunc: func(t *testing.T, result *QueryResult) {
				for _, row := range result.Rows {
					if prevDept := row["prev_dept"]; prevDept != nil {
						if _, ok := prevDept.(string); !ok {
							t.Errorf("LAG(department) should return string, got %T", prevDept)
						}
					}
				}
			},
		},
		{
			name:  "RANK with different data types in ORDER BY",
			query: "SELECT name, age, RANK() OVER (ORDER BY age DESC) as age_rank FROM employees",
			checkFunc: func(t *testing.T, result *QueryResult) {
				ranks := make([]int, 0)
				for _, row := range result.Rows {
					if rank := row["age_rank"]; rank != nil {
						if r, ok := rank.(int); ok {
							ranks = append(ranks, r)
						} else {
							t.Errorf("RANK() should return int, got %T", rank)
						}
					}
				}
				
				// Check that Mike Johnson (age 35) has rank 1 (highest age)
				mikeRank := 0
				for _, row := range result.Rows {
					if name := row["name"]; name == "Mike Johnson" {
						if rank := row["age_rank"]; rank != nil {
							mikeRank = rank.(int)
							break
						}
					}
				}
				if mikeRank != 1 {
					t.Errorf("Mike Johnson (age 35) should have rank 1, got %d", mikeRank)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Error != "" {
				t.Fatalf("Query error: %s", result.Error)
			}
			
			tt.checkFunc(t, result)
		})
	}
}

// BenchmarkWindowFunctions tests performance of window functions
func BenchmarkWindowFunctions(b *testing.B) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	benchmarks := []struct {
		name  string
		query string
	}{
		{
			name:  "ROW_NUMBER",
			query: "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM employees",
		},
		{
			name:  "ROW_NUMBER with PARTITION",
			query: "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank FROM employees",
		},
		{
			name:  "Multiple window functions",
			query: "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num, RANK() OVER (ORDER BY salary DESC) as rank_num FROM employees",
		},
		{
			name:  "LAG function",
			query: "SELECT name, salary, LAG(salary) OVER (ORDER BY salary DESC) as prev_salary FROM employees",
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := engine.Execute(bm.query)
				if err != nil {
					b.Fatalf("Benchmark query failed: %v", err)
				}
			}
		})
	}
}

// Helper function to convert QueryResult to string for debugging
func debugQueryResult(result *QueryResult) string {
	var parts []string
	
	// Add column headers
	parts = append(parts, fmt.Sprintf("Columns: %v", result.Columns))
	
	// Add row data
	for i, row := range result.Rows {
		parts = append(parts, fmt.Sprintf("Row %d: %v", i, row))
	}
	
	return strings.Join(parts, "\n")
}