package main

import (
	"fmt"
	"testing"
)

func TestCorrelatedSubqueries(t *testing.T) {
	// Generate sample data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	
	t.Run("Correlated Scalar Subquery", func(t *testing.T) {
		// Find employees who earn more than the average salary in their department
		sql := `SELECT e.name, e.department, e.salary 
				FROM employees e 
				WHERE e.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department = e.department)`
		
		fmt.Println("\nCorrelated Scalar Subquery:")
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
			if i < 5 { // Show first 5 rows
				fmt.Printf("Row %d: ", i+1)
				for _, col := range result.Columns {
					fmt.Printf("%s=%v ", col, row[col])
				}
				fmt.Println()
			}
		}
		
		if result.Count == 0 {
			t.Error("Expected some employees earning above department average")
		}
	})
	
	t.Run("Correlated EXISTS Subquery", func(t *testing.T) {
		// Find departments that have employees with salary > 70000
		sql := `SELECT d.name, d.budget 
				FROM departments d 
				WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department = d.name AND e.salary > 70000)`
		
		fmt.Println("\nCorrelated EXISTS Subquery:")
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
			t.Error("Expected some departments with high-earning employees")
		}
	})
	
	t.Run("Correlated NOT EXISTS Subquery", func(t *testing.T) {
		// Find departments that have no employees with salary > 80000
		sql := `SELECT d.name, d.budget 
				FROM departments d 
				WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.department = d.name AND e.salary > 80000)`
		
		fmt.Println("\nCorrelated NOT EXISTS Subquery:")
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
	})
	
	t.Run("Correlated IN Subquery", func(t *testing.T) {
		// Find employees whose salary matches any manager's budget threshold (scaled down by 1000)
		sql := `SELECT e.name, e.salary 
				FROM employees e 
				WHERE e.salary IN (SELECT d.budget / 1000 FROM departments d WHERE d.manager = e.name)`
		
		fmt.Println("\nCorrelated IN Subquery:")
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
	})
	
	t.Run("Complex Correlated Subquery", func(t *testing.T) {
		// Find employees who work in departments where the employee count is higher than average
		sql := `SELECT e.name, e.department 
				FROM employees e 
				WHERE EXISTS (
					SELECT 1 FROM departments d 
					WHERE d.name = e.department 
					AND d.employee_count > (SELECT AVG(d2.employee_count) FROM departments d2)
				)`
		
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
			if i < 5 { // Show first 5 rows
				fmt.Printf("Row %d: ", i+1)
				for _, col := range result.Columns {
					fmt.Printf("%s=%v ", col, row[col])
				}
				fmt.Println()
			}
		}
	})
}

func TestCorrelatedSubqueryParsing(t *testing.T) {
	parser := NewSQLParser()
	
	t.Run("Parse Correlated Scalar Subquery", func(t *testing.T) {
		sql := "SELECT name FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e.department)"
		
		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse correlated subquery: %v", err)
		}
		
		fmt.Printf("\nCorrelated Subquery Parsing:\n")
		fmt.Printf("Main query table: %s\n", parsed.TableName)
		fmt.Printf("Main query alias: %s\n", parsed.TableAlias)
		fmt.Printf("WHERE conditions: %d\n", len(parsed.Where))
		
		if len(parsed.Where) > 0 {
			condition := parsed.Where[0]
			fmt.Printf("Condition operator: %s\n", condition.Operator)
			fmt.Printf("Has subquery: %v\n", condition.Subquery != nil)
			
			if condition.Subquery != nil {
				fmt.Printf("Subquery table: %s\n", condition.Subquery.TableName)
				fmt.Printf("Subquery alias: %s\n", condition.Subquery.TableAlias)
				fmt.Printf("Subquery WHERE conditions: %d\n", len(condition.Subquery.Where))
				
				// Debug subquery WHERE conditions
				for i, subCond := range condition.Subquery.Where {
					fmt.Printf("  Subquery WHERE[%d]: %s.%s %s %v (table: %s)\n", 
						i, subCond.TableName, subCond.Column, subCond.Operator, subCond.Value, subCond.TableName)
				}
				
				fmt.Printf("Subquery is correlated: %v\n", condition.Subquery.IsCorrelated)
				fmt.Printf("Correlated columns: %v\n", condition.Subquery.CorrelatedColumns)
			}
		}
	})
}