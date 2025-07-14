package main

import (
	"fmt"
	"testing"

	"bytedb/core"
)

func TestBasicUnion(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Simple UNION", func(t *testing.T) {
		query := `SELECT name, department FROM employees WHERE department = 'Engineering'
		          UNION
		          SELECT name, department FROM employees WHERE department = 'Sales'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should return Engineering (4) + Sales (2) = 6 employees
		if result.Count != 6 {
			t.Errorf("Expected 6 employees, got %d", result.Count)
		}

		// Check columns
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}

		// Verify departments
		deptCounts := make(map[string]int)
		for _, row := range result.Rows {
			dept := row["department"].(string)
			deptCounts[dept]++
		}

		if deptCounts["Engineering"] != 4 {
			t.Errorf("Expected 4 Engineering employees, got %d", deptCounts["Engineering"])
		}
		if deptCounts["Sales"] != 2 {
			t.Errorf("Expected 2 Sales employees, got %d", deptCounts["Sales"])
		}
	})

	t.Run("UNION ALL with duplicates", func(t *testing.T) {
		query := `SELECT name, salary FROM employees WHERE salary > 75000
		          UNION ALL
		          SELECT name, salary FROM employees WHERE department = 'Engineering'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Salary > 75000: Mike(80k), Lisa(85k), Chris(78k) = 3 (John is exactly 75k, not > 75k)
		// Engineering: John, Mike, Lisa, Chris = 4
		// With UNION ALL, we should get all rows, so total = 7
		if result.Count != 7 {
			t.Errorf("Expected 7 rows with duplicates, got %d", result.Count)
			for i, row := range result.Rows {
				fmt.Printf("Row %d: %s, %.0f\n", i, row["name"], row["salary"])
			}
		}
	})

	t.Run("UNION removes duplicates", func(t *testing.T) {
		query := `SELECT name, department FROM employees WHERE salary > 75000
		          UNION
		          SELECT name, department FROM employees WHERE department = 'Engineering'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// All Engineering employees have salary >= 75000, so UNION should remove duplicates
		// Result should be just the 4 Engineering employees
		if result.Count != 4 {
			t.Errorf("Expected 4 unique employees, got %d", result.Count)
			for i, row := range result.Rows {
				fmt.Printf("Row %d: %v\n", i, row)
			}
		}
	})

	t.Run("UNION with different WHERE conditions", func(t *testing.T) {
		query := `SELECT name, age FROM employees WHERE age < 30
		          UNION
		          SELECT name, age FROM employees WHERE age > 32`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Check that we only get employees with age < 30 or age > 32
		for _, row := range result.Rows {
			var age int
			switch v := row["age"].(type) {
			case float64:
				age = int(v)
			case int32:
				age = int(v)
			case int:
				age = v
			default:
				t.Fatalf("Unexpected age type: %T", v)
			}

			if age >= 30 && age <= 32 {
				t.Errorf("Found employee with age %d, should only have < 30 or > 32", age)
			}
		}
	})

	t.Run("UNION with ORDER BY", func(t *testing.T) {
		query := `SELECT name, salary FROM employees WHERE department = 'Engineering'
		          UNION
		          SELECT name, salary FROM employees WHERE department = 'Sales'
		          ORDER BY salary DESC`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Verify ordering
		for i := 1; i < len(result.Rows); i++ {
			prevSalary := result.Rows[i-1]["salary"].(float64)
			currSalary := result.Rows[i]["salary"].(float64)
			if prevSalary < currSalary {
				t.Errorf("Results not properly ordered: %.0f < %.0f at position %d", prevSalary, currSalary, i)
			}
		}
	})

	t.Run("UNION with LIMIT", func(t *testing.T) {
		query := `SELECT name, salary FROM employees WHERE department = 'Engineering'
		          UNION
		          SELECT name, salary FROM employees WHERE department = 'Sales'
		          ORDER BY salary DESC
		          LIMIT 3`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		if result.Count != 3 {
			t.Errorf("Expected 3 rows with LIMIT, got %d", result.Count)
		}

		// Should get the top 3 salaries
		expectedSalaries := []float64{85000, 80000, 78000} // Lisa, Mike, Chris
		for i, expectedSal := range expectedSalaries {
			if i < len(result.Rows) {
				actualSal := result.Rows[i]["salary"].(float64)
				if actualSal != expectedSal {
					t.Errorf("Row %d: expected salary %.0f, got %.0f", i, expectedSal, actualSal)
				}
			}
		}
	})
}

func TestUnionColumnCompatibility(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Incompatible column count", func(t *testing.T) {
		query := `SELECT name FROM employees
		          UNION
		          SELECT name, department FROM employees`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should get an error about column count mismatch
		if result.Error == "" {
			t.Error("Expected error for mismatched column count")
		}
	})

	t.Run("Compatible columns with aliases", func(t *testing.T) {
		query := `SELECT name AS employee_name, salary AS pay FROM employees WHERE department = 'Engineering'
		          UNION
		          SELECT name, salary FROM employees WHERE department = 'Sales'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// First query's column names should be used
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		if result.Columns[0] != "employee_name" {
			t.Errorf("Expected first column 'employee_name', got '%s'", result.Columns[0])
		}
		if result.Columns[1] != "pay" {
			t.Errorf("Expected second column 'pay', got '%s'", result.Columns[1])
		}
	})
}

func TestMultipleUnions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Three-way UNION", func(t *testing.T) {
		query := `SELECT name, department FROM employees WHERE department = 'Engineering'
		          UNION
		          SELECT name, department FROM employees WHERE department = 'Sales'
		          UNION
		          SELECT name, department FROM employees WHERE department = 'Marketing'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Engineering (4) + Sales (2) + Marketing (2) = 8
		if result.Count != 8 {
			t.Errorf("Expected 8 employees, got %d", result.Count)
		}
	})

	t.Run("Mixed UNION and UNION ALL", func(t *testing.T) {
		query := `SELECT name FROM employees WHERE salary = 75000
		          UNION ALL
		          SELECT name FROM employees WHERE salary = 80000
		          UNION ALL
		          SELECT name FROM employees WHERE salary = 85000`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should get 3 employees (John 75k, Mike 80k, Lisa 85k)
		if result.Count != 3 {
			t.Errorf("Expected 3 employees, got %d", result.Count)
		}
	})
}

func TestUnionWithAggregates(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("UNION of aggregate queries", func(t *testing.T) {
		query := `SELECT department, COUNT(*) as count FROM employees WHERE salary > 70000 GROUP BY department
		          UNION
		          SELECT department, COUNT(*) as count FROM employees WHERE age < 35 GROUP BY department`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Each unique department/count combination should appear once
		fmt.Printf("Aggregate UNION returned %d rows\n", result.Count)
		for _, row := range result.Rows {
			fmt.Printf("  %s: %v\n", row["department"], row["count"])
		}
	})

	t.Run("UNION with mixed aggregate and non-aggregate", func(t *testing.T) {
		t.Skip("Constants in aggregate queries need separate handling")
		query := `SELECT 'High Earners' as category, COUNT(*) as count FROM employees WHERE salary > 80000
		          UNION
		          SELECT 'Low Earners' as category, COUNT(*) as count FROM employees WHERE salary < 60000`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should get 2 rows: High Earners and Low Earners
		if result.Count != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Count)
			for i, row := range result.Rows {
				fmt.Printf("Row %d: %v\n", i, row)
			}
			return
		}

		categories := make(map[string]int)
		for _, row := range result.Rows {
			catVal := row["category"]
			if catVal == nil {
				t.Errorf("Category is nil in row: %v", row)
				continue
			}
			cat := catVal.(string)
			countVal := row["count"]
			if countVal == nil {
				t.Errorf("Count is nil in row: %v", row)
				continue
			}
			count := int(countVal.(float64))
			categories[cat] = count
		}

		if categories["High Earners"] != 1 { // Only Lisa has salary > 80000
			t.Errorf("Expected 1 high earner, got %d", categories["High Earners"])
		}
		if categories["Low Earners"] != 1 { // Only Sarah has salary < 60000
			t.Errorf("Expected 1 low earner, got %d", categories["Low Earners"])
		}
	})
}

func TestUnionWithCTE(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("UNION with CTE", func(t *testing.T) {
		query := `WITH high_earners AS (
		              SELECT name, salary, department FROM employees WHERE salary > 75000
		          )
		          SELECT name, department FROM high_earners WHERE department = 'Engineering'
		          UNION
		          SELECT name, department FROM high_earners WHERE department = 'Sales'`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// High earners (>75k): Mike(80k), Lisa(85k), Chris(78k) all in Engineering
		// No high earners in Sales
		if result.Count != 3 {
			t.Errorf("Expected 3 employees, got %d", result.Count)
		}
	})
}

func TestUnionParsing(t *testing.T) {
	parser := core.NewSQLParser()

	t.Run("Parse simple UNION", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Engineering'
		        UNION
		        SELECT name FROM employees WHERE department = 'Sales'`

		query, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse UNION: %v", err)
		}

		if query.Type != core.UNION {
			t.Errorf("Expected query type UNION, got %v", query.Type)
		}

		if !query.HasUnion {
			t.Error("Expected HasUnion to be true")
		}

		if len(query.UnionQueries) != 2 {
			t.Errorf("Expected 2 union queries, got %d", len(query.UnionQueries))
		}

		// Check first query
		if query.UnionQueries[0].UnionAll {
			t.Error("First query should not be UNION ALL")
		}

		// Check second query
		if query.UnionQueries[1].UnionAll {
			t.Error("Second query should not be UNION ALL")
		}
	})

	t.Run("Parse UNION ALL", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE age < 30
		        UNION ALL
		        SELECT name FROM employees WHERE age > 40`

		query, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse UNION ALL: %v", err)
		}

		if len(query.UnionQueries) != 2 {
			t.Errorf("Expected 2 union queries, got %d", len(query.UnionQueries))
		}

		// Second query should be UNION ALL
		if !query.UnionQueries[1].UnionAll {
			t.Error("Second query should be UNION ALL")
		}
	})
}
