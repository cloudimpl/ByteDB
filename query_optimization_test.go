package main

import (
	"testing"
	"time"

	"bytedb/core"
)

func TestQueryOptimization(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Column Pruning Optimization", func(t *testing.T) {
		// Query that only needs specific columns
		query := "SELECT name, salary FROM employees WHERE department = 'Engineering'"

		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Query returned error: %s", result.Error)
		}

		// Verify only requested columns are returned
		expectedColumns := []string{"name", "salary"}
		if len(result.Columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
		}

		for i, expected := range expectedColumns {
			if i < len(result.Columns) && result.Columns[i] != expected {
				t.Errorf("Expected column %d to be '%s', got '%s'", i, expected, result.Columns[i])
			}
		}

		t.Logf("Column pruning query completed in %v", duration)
		t.Logf("Returned %d rows with %d columns", result.Count, len(result.Columns))
	})

	t.Run("Predicate Pushdown Effectiveness", func(t *testing.T) {
		// Query with selective WHERE clause
		selectiveQuery := "SELECT * FROM employees WHERE id = 1"

		start := time.Now()
		selectiveResult, err := engine.Execute(selectiveQuery)
		selectiveDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Selective query failed: %v", err)
		}

		// Query without WHERE clause (full table scan)
		scanQuery := "SELECT * FROM employees"

		start = time.Now()
		scanResult, err := engine.Execute(scanQuery)
		scanDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Scan query failed: %v", err)
		}

		// Selective query should return fewer rows
		if selectiveResult.Count >= scanResult.Count {
			t.Errorf("Expected selective query to return fewer rows. Selective: %d, Scan: %d",
				selectiveResult.Count, scanResult.Count)
		}

		// Selective query should be faster (though timing can be variable in tests)
		t.Logf("Selective query (%d rows): %v", selectiveResult.Count, selectiveDuration)
		t.Logf("Full scan query (%d rows): %v", scanResult.Count, scanDuration)

		// Verify selective query returned exactly 1 row
		if selectiveResult.Count != 1 {
			t.Errorf("Expected selective query to return 1 row, got %d", selectiveResult.Count)
		}
	})

	t.Run("Function Optimization in WHERE", func(t *testing.T) {
		// Query with function in WHERE clause
		query := "SELECT name FROM employees WHERE UPPER(department) = 'ENGINEERING'"

		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Function query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Function query returned error: %s", result.Error)
		}

		// Should return engineering employees
		if result.Count == 0 {
			t.Error("Expected to find engineering employees")
		}

		t.Logf("Function WHERE query completed in %v", duration)
		t.Logf("Found %d engineering employees", result.Count)
	})

	t.Run("JOIN Optimization", func(t *testing.T) {
		// JOIN query that should benefit from optimization
		query := `
			SELECT e.name, e.salary, d.manager 
			FROM employees e 
			JOIN departments d ON e.department = d.name 
			WHERE e.salary > 70000 
			ORDER BY e.salary DESC 
			LIMIT 5
		`

		start := time.Now()
		result, err := engine.Execute(query)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("JOIN query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("JOIN query returned error: %s", result.Error)
		}

		// Verify results
		if result.Count == 0 {
			t.Error("Expected JOIN results")
		}

		// Verify columns
		expectedColumns := []string{"name", "salary", "manager"}
		for _, expectedCol := range expectedColumns {
			found := false
			for _, actualCol := range result.Columns {
				if actualCol == expectedCol {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected column '%s' not found in result", expectedCol)
			}
		}

		t.Logf("JOIN query completed in %v", duration)
		t.Logf("Returned %d rows", result.Count)
	})
}

func TestOptimizationRules(t *testing.T) {
	t.Run("Predicate Pushdown Rule", func(t *testing.T) {
		rule := &core.PredicatePushdownRule{}

		// Test rule name
		if rule.Name() != "PredicatePushdown" {
			t.Errorf("Expected rule name 'PredicatePushdown', got '%s'", rule.Name())
		}

		// Test rule cost
		if rule.Cost() != 1 {
			t.Errorf("Expected rule cost 1, got %d", rule.Cost())
		}

		// Create a simple plan for testing
		plan := &core.QueryPlan{
			Root: &core.PlanNode{
				Type: core.PlanNodeFilter,
				FilterConditions: []core.WhereCondition{
					{Column: "salary", Operator: ">", Value: 70000},
				},
				Children: []*core.PlanNode{
					{
						Type:      core.PlanNodeScan,
						TableName: "employees",
					},
				},
			},
		}

		// Apply the rule
		optimizedPlan, changed := rule.Apply(plan)

		if !changed {
			t.Error("Expected predicate pushdown rule to make changes")
		}

		if optimizedPlan == nil {
			t.Error("Expected optimized plan to be returned")
		}

		t.Logf("Predicate pushdown rule applied successfully")
	})

	t.Run("Column Pruning Rule", func(t *testing.T) {
		rule := &core.ColumnPruningRule{}

		// Test basic properties
		if rule.Name() != "ColumnPruning" {
			t.Errorf("Expected rule name 'ColumnPruning', got '%s'", rule.Name())
		}

		// Create a plan with unnecessary columns
		plan := &core.QueryPlan{
			Root: &core.PlanNode{
				Type:    core.PlanNodeProject,
				Columns: []string{"name", "salary"},
				Children: []*core.PlanNode{
					{
						Type:      core.PlanNodeScan,
						TableName: "employees",
						Columns:   []string{"*"}, // All columns
					},
				},
			},
		}

		// Apply the rule
		optimizedPlan, changed := rule.Apply(plan)

		if !changed {
			t.Error("Expected column pruning rule to make changes")
		}

		// Check that scan node now has pruned columns
		scanNode := optimizedPlan.Root.Children[0]
		if len(scanNode.Columns) == 1 && scanNode.Columns[0] == "*" {
			t.Error("Expected scan node columns to be pruned")
		}

		t.Logf("Column pruning rule applied successfully")
	})

	t.Run("Join Order Optimization Rule", func(t *testing.T) {
		rule := &core.JoinOrderOptimizationRule{}

		if rule.Name() != "JoinOrderOptimization" {
			t.Errorf("Expected rule name 'JoinOrderOptimization', got '%s'", rule.Name())
		}

		// Create a plan with suboptimal join order (small table on left, large on right)
		plan := &core.QueryPlan{
			Root: &core.PlanNode{
				Type: core.PlanNodeJoin,
				Children: []*core.PlanNode{
					{Type: core.PlanNodeScan, TableName: "small_table", Rows: 100},
					{Type: core.PlanNodeScan, TableName: "large_table", Rows: 10000},
				},
			},
		}

		// Apply the rule
		optimizedPlan, changed := rule.Apply(plan)

		if !changed {
			t.Error("Expected join order optimization rule to make changes")
		}

		// Check that smaller table is now on the right (build side)
		rightChild := optimizedPlan.Root.Children[1]
		if rightChild.TableName != "small_table" {
			t.Error("Expected smaller table to be on the right side of join")
		}

		t.Logf("Join order optimization rule applied successfully")
	})
}

func TestOptimizationStats(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Get Optimization Statistics", func(t *testing.T) {
		query := "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC"

		// Get optimization stats
		stats, err := engine.GetOptimizationStats(query)
		if err != nil {
			t.Fatalf("Failed to get optimization stats: %v", err)
		}

		// Verify stats structure
		if stats == nil {
			t.Error("Expected optimization stats to be returned")
		}

		// Check for expected fields
		expectedFields := []string{"original_nodes", "optimized_nodes", "original_cost", "optimized_cost"}
		for _, field := range expectedFields {
			if _, exists := stats[field]; !exists {
				t.Errorf("Expected optimization stats to contain field '%s'", field)
			}
		}

		t.Logf("Optimization stats: %+v", stats)
	})
}
