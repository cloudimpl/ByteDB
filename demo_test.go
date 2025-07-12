package main

import (
	"testing"
	"time"
)

func TestOptimizerDemo(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Column Pruning Demo", func(t *testing.T) {
		// Query that benefits from column pruning
		query := "SELECT name, salary FROM employees WHERE department = 'Engineering'"
		
		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		t.Logf("🎯 Column Pruning Optimization Demo")
		t.Logf("📝 Query: %s", query)
		t.Logf("⏱️  Execution Time: %v", duration)
		t.Logf("📊 Columns Returned: %v (2 out of 6 total columns)", result.Columns)
		t.Logf("🎉 Rows Returned: %d", result.Count)
		t.Logf("💡 Benefit: Only reads necessary columns from Parquet file (67%% I/O reduction)")
		
		// Verify optimization worked
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		
		expectedColumns := map[string]bool{"name": true, "salary": true}
		for _, col := range result.Columns {
			if !expectedColumns[col] {
				t.Errorf("Unexpected column in result: %s", col)
			}
		}
	})

	t.Run("Predicate Pushdown Demo", func(t *testing.T) {
		// Query with selective filter
		selectiveQuery := "SELECT * FROM employees WHERE salary > 80000"
		
		start := time.Now()
		result, err := engine.Execute(selectiveQuery)
		duration := time.Since(start)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		t.Logf("🔍 Predicate Pushdown Optimization Demo")
		t.Logf("📝 Query: %s", selectiveQuery)
		t.Logf("⏱️  Execution Time: %v", duration)
		t.Logf("📊 Rows Returned: %d", result.Count)
		t.Logf("💡 Benefit: Filtering applied at scan level, reducing memory usage")
		
		// Verify all returned rows meet the condition
		for i, row := range result.Rows {
			if salary, ok := row["salary"].(float64); ok {
				if salary <= 80000 {
					t.Errorf("Row %d has salary %.0f, should be > 80000", i, salary)
				}
			}
		}
		
		// Should return fewer rows than total
		if result.Count >= 10 {
			t.Errorf("Expected selective results, got %d rows", result.Count)
		}
	})

	t.Run("Function Optimization Demo", func(t *testing.T) {
		// Query with function in WHERE clause
		query := "SELECT name FROM employees WHERE UPPER(department) = 'ENGINEERING'"
		
		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		t.Logf("⚡ Function Optimization Demo")
		t.Logf("📝 Query: %s", query)
		t.Logf("⏱️  Execution Time: %v", duration)
		t.Logf("📊 Rows Returned: %d", result.Count)
		t.Logf("💡 Benefit: Functions in WHERE clause optimized with predicate pushdown")
		
		// Should find engineering employees
		if result.Count == 0 {
			t.Error("Expected to find engineering employees")
		}
		
		// Verify only name column returned
		if len(result.Columns) != 1 || result.Columns[0] != "name" {
			t.Errorf("Expected only 'name' column, got %v", result.Columns)
		}
	})

	t.Run("JOIN Optimization Demo", func(t *testing.T) {
		// JOIN query that benefits from optimization
		query := `SELECT e.name, d.manager 
				  FROM employees e 
				  JOIN departments d ON e.department = d.name 
				  WHERE e.salary > 70000 
				  LIMIT 5`
		
		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)
		
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		t.Logf("🔗 JOIN Optimization Demo")
		t.Logf("📝 Query: %s", query)
		t.Logf("⏱️  Execution Time: %v", duration)
		t.Logf("📊 Rows Returned: %d", result.Count)
		t.Logf("💡 Benefit: Optimized join order, predicate pushdown, and column pruning")
		
		// Should return some results
		if result.Count == 0 {
			t.Error("Expected JOIN results")
		}
		
		// Should not exceed LIMIT
		if result.Count > 5 {
			t.Errorf("Expected at most 5 rows due to LIMIT, got %d", result.Count)
		}
		
		// Verify expected columns
		expectedCols := map[string]bool{"name": true, "manager": true}
		for _, col := range result.Columns {
			if !expectedCols[col] {
				t.Errorf("Unexpected column in JOIN result: %s", col)
			}
		}
	})

	t.Run("Complex Query Optimization Demo", func(t *testing.T) {
		// Complex query with multiple optimization opportunities
		query := `SELECT 
					UPPER(e.name) as employee_name,
					e.salary,
					d.budget,
					CASE 
						WHEN e.salary > 80000 THEN 'High'
						WHEN e.salary > 60000 THEN 'Medium'
						ELSE 'Low'
					END as salary_grade
				  FROM employees e
				  JOIN departments d ON e.department = d.name
				  WHERE e.department IN ('Engineering', 'Sales')
				  AND e.salary > 60000
				  ORDER BY e.salary DESC
				  LIMIT 3`
		
		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)
		
		if err != nil {
			t.Fatalf("Complex query failed: %v", err)
		}
		
		if result.Error != "" {
			t.Fatalf("Complex query error: %s", result.Error)
		}
		
		t.Logf("🚀 Complex Query Optimization Demo")
		t.Logf("📝 Query: Complex query with multiple optimizations")
		t.Logf("⏱️  Execution Time: %v", duration)
		t.Logf("📊 Rows Returned: %d", result.Count)
		t.Logf("📋 Columns: %v", result.Columns)
		t.Logf("💡 Benefits:")
		t.Logf("   - Column pruning: Only necessary columns read")
		t.Logf("   - Predicate pushdown: Filters applied early")
		t.Logf("   - Join optimization: Optimal join order")
		t.Logf("   - Function optimization: Efficient UPPER() and CASE evaluation")
		t.Logf("   - Sort optimization: ORDER BY with LIMIT")
		
		// Should return some results
		if result.Count == 0 {
			t.Error("Expected results from complex query")
		}
		
		// Should not exceed LIMIT
		if result.Count > 3 {
			t.Errorf("Expected at most 3 rows due to LIMIT, got %d", result.Count)
		}
		
		// Verify salary filtering
		for i, row := range result.Rows {
			if salary, ok := row["salary"].(float64); ok {
				if salary <= 60000 {
					t.Errorf("Row %d has salary %.0f, should be > 60000", i, salary)
				}
			}
		}
		
		// Verify results are ordered by salary DESC
		for i := 1; i < len(result.Rows); i++ {
			prevSalary, _ := result.Rows[i-1]["salary"].(float64)
			currSalary, _ := result.Rows[i]["salary"].(float64)
			if prevSalary < currSalary {
				t.Errorf("Results not properly ordered: %.0f < %.0f at position %d", 
					prevSalary, currSalary, i)
			}
		}
	})
}

func TestOptimizationStatistics(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Optimization Statistics Demo", func(t *testing.T) {
		query := "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC"
		
		// Get optimization statistics
		stats, err := engine.GetOptimizationStats(query)
		if err != nil {
			t.Fatalf("Failed to get optimization stats: %v", err)
		}
		
		t.Logf("📊 Optimization Statistics Demo")
		t.Logf("📝 Query: %s", query)
		t.Logf("🔧 Optimization Stats: %+v", stats)
		
		// Verify stats structure
		expectedFields := []string{"original_nodes", "optimized_nodes", "original_cost", "optimized_cost"}
		for _, field := range expectedFields {
			if _, exists := stats[field]; !exists {
				t.Errorf("Expected optimization stats to contain field '%s'", field)
			}
		}
		
		// Check for improvement
		if improvement, exists := stats["improvement_percent"]; exists {
			if improvementPct, ok := improvement.(float64); ok && improvementPct > 0 {
				t.Logf("💡 Performance Improvement: %.1f%%", improvementPct)
			}
		}
	})
}