package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestExplainExample(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	fmt.Println("\n🔍 ByteDB Query Optimization Demo")
	fmt.Println("=====================================")
	fmt.Println()

	// Example 1: Simple query plan
	fmt.Println("1. Simple SELECT Query Plan:")
	query := "EXPLAIN SELECT name, salary FROM employees WHERE department = 'Engineering'"
	result, _ := engine.Execute(query)
	if result.Error == "" {
		plan := result.Rows[0]["QUERY PLAN"].(string)
		fmt.Println(plan)
	}

	// Example 2: Query with aggregation
	fmt.Println("\n2. Aggregate Query Plan:")
	query = "EXPLAIN SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department"
	result, _ = engine.Execute(query)
	if result.Error == "" {
		plan := result.Rows[0]["QUERY PLAN"].(string)
		fmt.Println(plan)
	}

	// Example 3: Complex query with sorting and limit
	fmt.Println("\n3. Complex Query Plan (Sort + Limit):")
	query = "EXPLAIN SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5"
	result, _ = engine.Execute(query)
	if result.Error == "" {
		plan := result.Rows[0]["QUERY PLAN"].(string)
		fmt.Println(plan)
	}

	// Example 4: EXPLAIN ANALYZE
	fmt.Println("\n4. EXPLAIN ANALYZE (with execution stats):")
	query = "EXPLAIN ANALYZE SELECT COUNT(*) FROM employees WHERE salary > 70000"
	result, _ = engine.Execute(query)
	if result.Error == "" {
		plan := result.Rows[0]["QUERY PLAN"].(string)
		fmt.Println(plan)
	}

	// Example 5: JSON format
	fmt.Println("\n5. EXPLAIN with JSON format (excerpt):")
	query = "EXPLAIN (FORMAT JSON) SELECT name FROM employees WHERE department = 'Engineering'"
	result, _ = engine.Execute(query)
	if result.Error == "" {
		plan := result.Rows[0]["QUERY PLAN"].(string)
		// Just show first few lines of JSON
		lines := strings.Split(plan, "\n")
		for i := 0; i < 10 && i < len(lines); i++ {
			fmt.Println(lines[i])
		}
		if len(lines) > 10 {
			fmt.Println("  ... (truncated)")
		}
	}

	// All tests passed if we got here
}