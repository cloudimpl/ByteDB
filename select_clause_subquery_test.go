package main

import (
	"fmt"
	"testing"
)

func TestSelectClauseSubqueries(t *testing.T) {
	// Generate sample data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	
	t.Run("Simple Scalar Subquery in SELECT", func(t *testing.T) {
		// Get total employee count as a column
		sql := `SELECT name, salary, (SELECT COUNT(*) FROM employees) as total_employees FROM employees LIMIT 3`
		
		fmt.Println("\nSimple Scalar Subquery in SELECT:")
		fmt.Printf("SQL: %s\n", sql)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}
		
		fmt.Printf("Results: %d rows\n", result.Count)
		for i, row := range result.Rows {
			fmt.Printf("Row %d: ", i+1)
			for _, col := range result.Columns {
				fmt.Printf("%s=%v ", col, row[col])
			}
			fmt.Println()
		}
		
		if result.Count != 3 {
			t.Errorf("Expected 3 rows, got %d", result.Count)
		}
		
		// Check that total_employees column exists and has correct value
		if len(result.Rows) > 0 {
			if totalEmp, exists := result.Rows[0]["total_employees"]; exists {
				// Convert to int for comparison
				var totalEmpInt int
				switch v := totalEmp.(type) {
				case int:
					totalEmpInt = v
				case int32:
					totalEmpInt = int(v)
				case int64:
					totalEmpInt = int(v)
				case float64:
					totalEmpInt = int(v)
				default:
					t.Errorf("Unexpected type for total_employees: %T", totalEmp)
					return
				}
				
				if totalEmpInt != 10 {
					t.Errorf("Expected total_employees=10, got %v (type: %T)", totalEmp, totalEmp)
				} else {
					fmt.Printf("✓ total_employees value correct: %d\n", totalEmpInt)
				}
			} else {
				t.Error("total_employees column not found")
			}
		}
	})
	
	t.Run("Correlated Scalar Subquery in SELECT", func(t *testing.T) {
		// Get average department salary for each employee
		sql := `SELECT name, department, salary, 
				(SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department = e.department) as avg_dept_salary 
				FROM employees e LIMIT 5`
		
		fmt.Println("\nCorrelated Scalar Subquery in SELECT:")
		fmt.Printf("SQL: %s\n", sql)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}
		
		fmt.Printf("Results: %d rows\n", result.Count)
		for i, row := range result.Rows {
			fmt.Printf("Row %d: ", i+1)
			for _, col := range result.Columns {
				fmt.Printf("%s=%v ", col, row[col])
			}
			fmt.Println()
		}
		
		if result.Count != 5 {
			t.Errorf("Expected 5 rows, got %d", result.Count)
		}
		
		// Check that avg_dept_salary column exists
		if len(result.Rows) > 0 {
			if _, exists := result.Rows[0]["avg_dept_salary"]; !exists {
				t.Error("avg_dept_salary column not found")
			}
		}
	})
	
	t.Run("Multiple Subqueries in SELECT", func(t *testing.T) {
		// Multiple subquery columns
		sql := `SELECT name, 
				(SELECT COUNT(*) FROM employees) as total_employees,
				(SELECT COUNT(*) FROM departments) as total_departments,
				(SELECT MAX(salary) FROM employees) as max_salary
				FROM employees LIMIT 2`
		
		fmt.Println("\nMultiple Subqueries in SELECT:")
		fmt.Printf("SQL: %s\n", sql)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}
		
		fmt.Printf("Results: %d rows\n", result.Count)
		for i, row := range result.Rows {
			fmt.Printf("Row %d: ", i+1)
			for _, col := range result.Columns {
				fmt.Printf("%s=%v ", col, row[col])
			}
			fmt.Println()
		}
		
		if result.Count != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Count)
		}
		
		// Verify all subquery columns exist
		expectedCols := []string{"name", "total_employees", "total_departments", "max_salary"}
		for _, expectedCol := range expectedCols {
			found := false
			for _, actualCol := range result.Columns {
				if actualCol == expectedCol {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected column %s not found", expectedCol)
			}
		}
	})
	
	t.Run("Subquery with Aggregation", func(t *testing.T) {
		// Subquery using aggregation functions
		sql := `SELECT department, 
				(SELECT COUNT(*) FROM employees e2 WHERE e2.department = e.department) as dept_employee_count
				FROM employees e 
				GROUP BY department`
		
		fmt.Println("\nSubquery with Aggregation:")
		fmt.Printf("SQL: %s\n", sql)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}
		
		fmt.Printf("Results: %d rows\n", result.Count)
		for i, row := range result.Rows {
			fmt.Printf("Row %d: ", i+1)
			for _, col := range result.Columns {
				fmt.Printf("%s=%v ", col, row[col])
			}
			fmt.Println()
		}
		
		// Should have one row per department
		if result.Count == 0 {
			t.Error("Expected some grouped results")
		}
	})
	
	t.Run("Complex Correlated Subquery", func(t *testing.T) {
		// Get count of employees in departments with higher budget
		sql := `SELECT d.name as dept_name, d.budget,
				(SELECT COUNT(*) FROM employees e WHERE e.department = d.name) as employee_count,
				(SELECT COUNT(*) FROM departments d2 WHERE d2.budget > d.budget) as higher_budget_depts
				FROM departments d LIMIT 3`
		
		fmt.Println("\nComplex Correlated Subquery:")
		fmt.Printf("SQL: %s\n", sql)
		
		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}
		
		fmt.Printf("Results: %d rows\n", result.Count)
		for i, row := range result.Rows {
			fmt.Printf("Row %d: ", i+1)
			for _, col := range result.Columns {
				fmt.Printf("%s=%v ", col, row[col])
			}
			fmt.Println()
		}
		
		if result.Count == 0 {
			t.Error("Expected some department results")
		}
	})
}

func TestSelectClauseSubqueryParsing(t *testing.T) {
	parser := NewSQLParser()
	
	t.Run("Parse Simple Scalar Subquery in SELECT", func(t *testing.T) {
		sql := "SELECT name, (SELECT COUNT(*) FROM departments) as dept_count FROM employees"
		
		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse SELECT clause subquery: %v", err)
		}
		
		fmt.Printf("\nSELECT Clause Subquery Parsing:\n")
		fmt.Printf("Main query table: %s\n", parsed.TableName)
		fmt.Printf("Columns: %d\n", len(parsed.Columns))
		
		// Check for subquery column
		var subqueryCol *Column
		for i := range parsed.Columns {
			if parsed.Columns[i].Subquery != nil {
				subqueryCol = &parsed.Columns[i]
				break
			}
		}
		
		if subqueryCol == nil {
			t.Error("No subquery column found")
		} else {
			fmt.Printf("Subquery column name: %s\n", subqueryCol.Name)
			fmt.Printf("Subquery column alias: %s\n", subqueryCol.Alias)
			fmt.Printf("Has subquery: %v\n", subqueryCol.Subquery != nil)
			
			if subqueryCol.Subquery != nil {
				fmt.Printf("Subquery table: %s\n", subqueryCol.Subquery.TableName)
				fmt.Printf("Subquery is aggregate: %v\n", subqueryCol.Subquery.IsAggregate)
				fmt.Printf("Subquery aggregates: %d\n", len(subqueryCol.Subquery.Aggregates))
			}
		}
	})
	
	t.Run("Parse Correlated Subquery in SELECT", func(t *testing.T) {
		sql := "SELECT name, (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e.department) as avg_dept_salary FROM employees e"
		
		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse correlated SELECT clause subquery: %v", err)
		}
		
		fmt.Printf("\nCorrelated SELECT Clause Subquery Parsing:\n")
		fmt.Printf("Main query alias: %s\n", parsed.TableAlias)
		
		// Find subquery column
		var subqueryCol *Column
		for i := range parsed.Columns {
			if parsed.Columns[i].Subquery != nil {
				subqueryCol = &parsed.Columns[i]
				break
			}
		}
		
		if subqueryCol != nil && subqueryCol.Subquery != nil {
			fmt.Printf("Subquery is correlated: %v\n", subqueryCol.Subquery.IsCorrelated)
			fmt.Printf("Correlated columns: %v\n", subqueryCol.Subquery.CorrelatedColumns)
		} else {
			t.Error("Subquery column or subquery not found")
		}
	})
}