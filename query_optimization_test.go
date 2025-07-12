package main

import (
	"strings"
	"testing"
	"encoding/json"
	"math"
)

func TestQueryPlan(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Simple SELECT Plan", func(t *testing.T) {
		query := "EXPLAIN SELECT name, salary FROM employees WHERE department = 'Engineering'"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Check that we got a plan
		if result.Count != 1 {
			t.Errorf("Expected 1 row with query plan, got %d", result.Count)
		}

		// Verify the plan contains expected nodes
		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "TableScan") {
			t.Errorf("Expected TableScan in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "Filter") {
			t.Errorf("Expected Filter in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "Project") {
			t.Errorf("Expected Project in plan, got: %s", planText)
		}
	})

	t.Run("Aggregate Query Plan", func(t *testing.T) {
		query := "EXPLAIN SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "Aggregate") {
			t.Errorf("Expected Aggregate in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "GROUP BY department") {
			t.Errorf("Expected GROUP BY in plan, got: %s", planText)
		}
	})

	t.Run("JOIN Query Plan", func(t *testing.T) {
		query := "EXPLAIN SELECT e.name, d.name FROM employees e JOIN departments d ON e.department = d.name"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "Join") {
			t.Errorf("Expected Join in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "Inner Join") {
			t.Errorf("Expected Inner Join in plan, got: %s", planText)
		}
	})

	t.Run("CTE Query Plan", func(t *testing.T) {
		query := `EXPLAIN WITH high_earners AS (
			SELECT name, salary FROM employees WHERE salary > 70000
		)
		SELECT * FROM high_earners`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "CTE") {
			t.Errorf("Expected CTE in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "high_earners") {
			t.Errorf("Expected CTE name in plan, got: %s", planText)
		}
	})

	t.Run("Window Function Query Plan", func(t *testing.T) {
		query := `EXPLAIN SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank FROM employees`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "Window") {
			t.Errorf("Expected Window in plan, got: %s", planText)
		}
	})

	t.Run("Complex Query Plan with Sort and Limit", func(t *testing.T) {
		query := `EXPLAIN SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC LIMIT 5`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		if !strings.Contains(planText, "Sort") {
			t.Errorf("Expected Sort in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "Limit") {
			t.Errorf("Expected Limit in plan, got: %s", planText)
		}
		if !strings.Contains(planText, "salary DESC") {
			t.Errorf("Expected sort direction in plan, got: %s", planText)
		}
	})
}

func TestExplainOptions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("EXPLAIN with JSON format", func(t *testing.T) {
		query := "EXPLAIN (FORMAT JSON) SELECT name FROM employees"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Check that output is valid JSON
		planJSON := result.Rows[0]["QUERY PLAN"].(string)
		var planData map[string]interface{}
		if err := json.Unmarshal([]byte(planJSON), &planData); err != nil {
			t.Errorf("Plan output is not valid JSON: %v", err)
		}

		// Verify JSON structure
		if _, ok := planData["node_type"]; !ok {
			t.Error("JSON plan missing node_type")
		}
		if _, ok := planData["cost"]; !ok {
			t.Error("JSON plan missing cost")
		}
		if _, ok := planData["rows"]; !ok {
			t.Error("JSON plan missing rows")
		}
	})

	t.Run("EXPLAIN ANALYZE", func(t *testing.T) {
		query := "EXPLAIN ANALYZE SELECT name FROM employees WHERE salary > 70000"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		// ANALYZE should add actual execution statistics
		if !strings.Contains(planText, "actual") {
			t.Errorf("Expected actual statistics in ANALYZE output, got: %s", planText)
		}
	})

	t.Run("EXPLAIN without costs", func(t *testing.T) {
		query := "EXPLAIN (COSTS FALSE) SELECT name FROM employees"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		planText := result.Rows[0]["QUERY PLAN"].(string)
		// Should not contain cost information
		if strings.Contains(planText, "cost=") {
			t.Errorf("Expected no cost information with COSTS FALSE, got: %s", planText)
		}
	})
}

func TestStatisticsCollection(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Collect Table Statistics", func(t *testing.T) {
		stats, err := engine.planner.statsCollector.CollectTableStats("employees")
		if err != nil {
			t.Fatalf("Failed to collect stats: %v", err)
		}

		// Verify basic statistics
		if stats.TableName != "employees" {
			t.Errorf("Expected table name 'employees', got '%s'", stats.TableName)
		}
		if stats.RowCount != 10 {
			t.Errorf("Expected row count %d, got %d", 10, stats.RowCount)
		}
		if stats.SizeBytes <= 0 {
			t.Error("Expected positive table size")
		}

		// Verify column statistics
		if len(stats.ColumnStats) == 0 {
			t.Error("Expected column statistics")
		}

		// Check specific column stats
		if salaryStats, ok := stats.ColumnStats["salary"]; ok {
			if salaryStats.DistinctCount <= 0 {
				t.Error("Expected positive distinct count for salary")
			}
			if salaryStats.MinValue == nil || salaryStats.MaxValue == nil {
				t.Error("Expected min/max values for salary")
			}
			if salaryStats.DataType != "float" {
				t.Errorf("Expected salary data type 'float', got '%s'", salaryStats.DataType)
			}
		} else {
			t.Error("Missing statistics for salary column")
		}
	})

	t.Run("Statistics Persistence", func(t *testing.T) {
		// Collect and save stats
		stats1, err := engine.planner.statsCollector.CollectTableStats("departments")
		if err != nil {
			t.Fatalf("Failed to collect stats: %v", err)
		}

		// Clear cache to force reload from disk
		engine.planner.statsCollector.cache = make(map[string]*TableStatistics)

		// Load stats from disk
		stats2 := engine.planner.statsCollector.GetTableStats("departments")
		if stats2 == nil {
			t.Fatal("Failed to load persisted statistics")
		}

		// Verify loaded stats match original
		if stats2.RowCount != stats1.RowCount {
			t.Errorf("Row count mismatch: expected %d, got %d", stats1.RowCount, stats2.RowCount)
		}
		if stats2.TableName != stats1.TableName {
			t.Errorf("Table name mismatch: expected %s, got %s", stats1.TableName, stats2.TableName)
		}
	})
}

func TestCostEstimation(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	testCases := []struct {
		name          string
		query         string
		expectedNodes []string
		costOrder     []string // Expected nodes in ascending cost order
	}{
		{
			name:          "Simple scan cheaper than filtered scan",
			query:         "SELECT * FROM employees",
			expectedNodes: []string{"TableScan"},
			costOrder:     []string{"TableScan"},
		},
		{
			name:          "Filter adds cost",
			query:         "SELECT * FROM employees WHERE salary > 70000",
			expectedNodes: []string{"TableScan", "Filter"},
			costOrder:     []string{"TableScan", "Filter"},
		},
		{
			name:          "Sort is expensive",
			query:         "SELECT * FROM employees ORDER BY salary",
			expectedNodes: []string{"TableScan", "Sort"},
			costOrder:     []string{"TableScan", "Sort"},
		},
		{
			name:          "Aggregation cost",
			query:         "SELECT department, AVG(salary) FROM employees GROUP BY department",
			expectedNodes: []string{"TableScan", "Aggregate"},
			costOrder:     []string{"TableScan", "Aggregate"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsedQuery, err := engine.parser.Parse(tc.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			plan, err := engine.planner.CreatePlan(parsedQuery, engine)
			if err != nil {
				t.Fatalf("Failed to create plan: %v", err)
			}

			// Verify plan has expected structure
			if plan.Root == nil {
				t.Fatal("Plan has no root node")
			}

			// Check that costs are calculated
			if plan.Root.Cost <= 0 {
				t.Error("Root node has no cost estimate")
			}

			// Verify cost increases up the tree
			var checkCostIncrease func(node *PlanNode)
			checkCostIncrease = func(node *PlanNode) {
				for _, child := range node.Children {
					if child.Cost >= node.Cost {
						t.Errorf("Child cost (%f) >= parent cost (%f)", child.Cost, node.Cost)
					}
					checkCostIncrease(child)
				}
			}
			checkCostIncrease(plan.Root)
		})
	}
}

func TestQueryPlannerSelectivity(t *testing.T) {
	planner := NewQueryPlanner()

	t.Run("Equality selectivity", func(t *testing.T) {
		cond := WhereCondition{
			Column:   "department",
			Operator: "=",
			Value:    "Engineering",
		}
		
		// Without statistics
		sel := planner.estimateConditionSelectivity(cond, nil)
		if sel != 0.1 {
			t.Errorf("Expected default equality selectivity 0.1, got %f", sel)
		}

		// With statistics
		stats := &NodeStatistics{
			DistinctValues: map[string]int64{
				"department": 5,
			},
		}
		sel = planner.estimateConditionSelectivity(cond, stats)
		expected := 1.0 / 5.0
		if sel != expected {
			t.Errorf("Expected selectivity %f, got %f", expected, sel)
		}
	})

	t.Run("Range selectivity", func(t *testing.T) {
		cond := WhereCondition{
			Column:   "salary",
			Operator: ">",
			Value:    70000,
		}
		
		sel := planner.estimateConditionSelectivity(cond, nil)
		if sel != 0.3 {
			t.Errorf("Expected default range selectivity 0.3, got %f", sel)
		}
	})

	t.Run("IN selectivity", func(t *testing.T) {
		cond := WhereCondition{
			Column:    "department",
			Operator:  "IN",
			ValueList: []interface{}{"Engineering", "Sales", "Marketing"},
		}
		
		// Without statistics
		sel := planner.estimateConditionSelectivity(cond, nil)
		expected := 3 * 0.1
		if math.Abs(sel - expected) > 0.0001 {
			t.Errorf("Expected IN selectivity %f, got %f", expected, sel)
		}

		// With statistics
		stats := &NodeStatistics{
			DistinctValues: map[string]int64{
				"department": 10,
			},
		}
		sel = planner.estimateConditionSelectivity(cond, stats)
		expected = 3.0 / 10.0
		if math.Abs(sel - expected) > 0.0001 {
			t.Errorf("Expected IN selectivity %f, got %f", expected, sel)
		}
	})

	t.Run("Complex condition selectivity", func(t *testing.T) {
		// Test AND conditions
		andCond := WhereCondition{
			IsComplex: true,
			LogicalOp: "AND",
			Left: &WhereCondition{
				Column:   "department",
				Operator: "=",
				Value:    "Engineering",
			},
			Right: &WhereCondition{
				Column:   "salary",
				Operator: ">",
				Value:    70000,
			},
		}
		
		sel := planner.estimateConditionSelectivity(andCond, nil)
		expected := 0.1 * 0.3 // AND multiplies selectivities
		if sel != expected {
			t.Errorf("Expected AND selectivity %f, got %f", expected, sel)
		}

		// Test OR conditions
		orCond := WhereCondition{
			IsComplex: true,
			LogicalOp: "OR",
			Left: &WhereCondition{
				Column:   "department",
				Operator: "=",
				Value:    "Engineering",
			},
			Right: &WhereCondition{
				Column:   "department",
				Operator: "=",
				Value:    "Sales",
			},
		}
		
		sel = planner.estimateConditionSelectivity(orCond, nil)
		expected = 0.1 + 0.1 - (0.1 * 0.1) // OR formula
		if sel != expected {
			t.Errorf("Expected OR selectivity %f, got %f", expected, sel)
		}
	})
}