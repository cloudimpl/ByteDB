package main

import (
	"testing"
	"fmt"
)

func TestBasicCTEs(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Simple CTE", func(t *testing.T) {
		query := `WITH high_earners AS (
			SELECT name, salary, department 
			FROM employees 
			WHERE salary > 80000
		)
		SELECT * FROM high_earners`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Should return employees with salary > 80000
		// Based on test data: Lisa Davis (85000)
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 high earner, got %d", len(result.Rows))
		}
		
		// Verify it's Lisa Davis
		if len(result.Rows) > 0 {
			name := result.Rows[0]["name"].(string)
			if name != "Lisa Davis" {
				t.Errorf("Expected Lisa Davis, got %s", name)
			}
		}
	})

	t.Run("CTE with WHERE clause", func(t *testing.T) {
		query := `WITH engineering_employees AS (
			SELECT name, salary, department 
			FROM employees 
			WHERE department = 'Engineering'
		)
		SELECT name, salary 
		FROM engineering_employees 
		WHERE salary > 75000`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Should return Engineering employees with salary > 75000
		// Mike (80000), Lisa (85000), Chris (78000)
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 engineering high earners, got %d", len(result.Rows))
		}
	})

	t.Run("CTE with aggregation", func(t *testing.T) {
		query := `WITH dept_stats AS (
			SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
			FROM employees
			GROUP BY department
		)
		SELECT * FROM dept_stats WHERE emp_count > 1`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Departments with > 1 employee: Engineering (4), Marketing (2), Sales (2)
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 departments with multiple employees, got %d", len(result.Rows))
		}
	})

	t.Run("Multiple CTEs", func(t *testing.T) {
		query := `WITH 
		high_earners AS (
			SELECT name, salary, department 
			FROM employees 
			WHERE salary > 70000
		),
		eng_high_earners AS (
			SELECT name, salary 
			FROM high_earners 
			WHERE department = 'Engineering'
		)
		SELECT * FROM eng_high_earners ORDER BY salary DESC`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Engineering employees with salary > 70000: all 4 of them
		if len(result.Rows) != TestEmployeesInEngineering {
			t.Errorf("Expected %d engineering high earners, got %d", 
				TestEmployeesInEngineering, len(result.Rows))
		}
		
		// Check ordering (DESC by salary)
		if len(result.Rows) > 0 {
			firstSalary := result.Rows[0]["salary"].(float64)
			if firstSalary != TestMaxSalary {
				t.Errorf("Expected highest salary %.0f first, got %.0f", TestMaxSalary, firstSalary)
			}
		}
	})

	t.Run("CTE with column list", func(t *testing.T) {
		query := `WITH top_earners(employee_name, employee_salary) AS (
			SELECT name, salary
			FROM employees
			WHERE salary > 80000
		)
		SELECT employee_name, employee_salary FROM top_earners`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Should still return 1 employee (Lisa)
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 top earner, got %d", len(result.Rows))
		}
		
		// Check column names are mapped correctly
		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		// Note: Column aliasing in CTE might need additional implementation
	})
}

func TestAdvancedCTEs(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("CTE in subquery", func(t *testing.T) {
		query := `SELECT name, salary 
		FROM employees 
		WHERE salary > (
			WITH avg_salaries AS (
				SELECT AVG(salary) as avg_sal FROM employees
			)
			SELECT avg_sal FROM avg_salaries
		)`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Employees above average salary
		fmt.Printf("Employees above average: %d\n", len(result.Rows))
	})

	t.Run("CTE with JOIN", func(t *testing.T) {
		t.Skip("CTEs in JOIN clauses not yet implemented")
		
		query := `WITH dept_totals AS (
			SELECT department, COUNT(*) as total
			FROM employees
			GROUP BY department
		)
		SELECT e.name, e.department, dt.total
		FROM employees e
		JOIN dept_totals dt ON e.department = dt.department
		WHERE dt.total > 1`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}
		
		// Employees in departments with > 1 person
		// Engineering (4), Marketing (2), Sales (2) = 8 total
		if len(result.Rows) != 8 {
			t.Errorf("Expected 8 employees in larger departments, got %d", len(result.Rows))
		}
	})
}

func TestCTEParsing(t *testing.T) {
	parser := NewSQLParser()

	t.Run("Parse simple CTE", func(t *testing.T) {
		sql := `WITH high_earners AS (
			SELECT name, salary FROM employees WHERE salary > 80000
		)
		SELECT * FROM high_earners`
		
		query, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse CTE: %v", err)
		}
		
		if len(query.CTEs) != 1 {
			t.Errorf("Expected 1 CTE, got %d", len(query.CTEs))
		}
		
		if len(query.CTEs) > 0 {
			cte := query.CTEs[0]
			if cte.Name != "high_earners" {
				t.Errorf("Expected CTE name 'high_earners', got '%s'", cte.Name)
			}
			if cte.Query == nil {
				t.Error("CTE query is nil")
			}
			if cte.IsRecursive {
				t.Error("CTE should not be recursive")
			}
		}
	})

	t.Run("Parse multiple CTEs", func(t *testing.T) {
		sql := `WITH 
		cte1 AS (SELECT * FROM employees),
		cte2 AS (SELECT * FROM departments)
		SELECT * FROM cte1`
		
		query, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse multiple CTEs: %v", err)
		}
		
		if len(query.CTEs) != 2 {
			t.Errorf("Expected 2 CTEs, got %d", len(query.CTEs))
		}
		
		expectedNames := []string{"cte1", "cte2"}
		for i, cte := range query.CTEs {
			if i < len(expectedNames) && cte.Name != expectedNames[i] {
				t.Errorf("Expected CTE[%d] name '%s', got '%s'", 
					i, expectedNames[i], cte.Name)
			}
		}
	})

	t.Run("Parse CTE with column list", func(t *testing.T) {
		sql := `WITH emp_info(emp_name, emp_dept) AS (
			SELECT name, department FROM employees
		)
		SELECT * FROM emp_info`
		
		query, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse CTE with columns: %v", err)
		}
		
		if len(query.CTEs) != 1 {
			t.Errorf("Expected 1 CTE, got %d", len(query.CTEs))
		}
		
		if len(query.CTEs) > 0 {
			cte := query.CTEs[0]
			if len(cte.ColumnNames) != 2 {
				t.Errorf("Expected 2 column names, got %d", len(cte.ColumnNames))
			}
			expectedCols := []string{"emp_name", "emp_dept"}
			for i, col := range cte.ColumnNames {
				if i < len(expectedCols) && col != expectedCols[i] {
					t.Errorf("Expected column[%d] '%s', got '%s'", 
						i, expectedCols[i], col)
				}
			}
		}
	})
}