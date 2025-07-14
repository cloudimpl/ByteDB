package main

import (
	"fmt"
	"testing"

	"bytedb/core"
)

func TestAdvancedWhereClauses(t *testing.T) {
	// Generate sample data
	generateSampleData()

	engine := core.NewQueryEngine("./data")

	t.Run("AND Logical Operator", func(t *testing.T) {
		sql := `SELECT name, department, salary FROM employees WHERE department = 'Engineering' AND salary > 70000`

		fmt.Println("\nAND Logical Operator:")
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

		// Should find Engineering employees with salary > 70000
		if result.Count == 0 {
			t.Error("Expected some Engineering employees with high salary")
		}

		// Verify all results match both conditions
		for _, row := range result.Rows {
			if dept, exists := row["department"]; exists {
				if dept != "Engineering" {
					t.Errorf("Expected department=Engineering, got %v", dept)
				}
			}
			if salaryVal, exists := row["salary"]; exists {
				// Convert salary to comparable format
				var salary float64
				switch v := salaryVal.(type) {
				case int:
					salary = float64(v)
				case int32:
					salary = float64(v)
				case int64:
					salary = float64(v)
				case float64:
					salary = v
				}
				if salary <= 70000 {
					t.Errorf("Expected salary > 70000, got %v", salary)
				}
			}
		}
	})

	t.Run("OR Logical Operator", func(t *testing.T) {
		sql := `SELECT name, department FROM employees WHERE department = 'HR' OR department = 'Finance'`

		fmt.Println("\nOR Logical Operator:")
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

		// Should find HR or Finance employees
		if result.Count == 0 {
			t.Error("Expected some HR or Finance employees")
		}

		// Verify all results match either condition
		for _, row := range result.Rows {
			if dept, exists := row["department"]; exists {
				if dept != "HR" && dept != "Finance" {
					t.Errorf("Expected department=HR or Finance, got %v", dept)
				}
			}
		}
	})

	t.Run("BETWEEN Operator", func(t *testing.T) {
		sql := `SELECT name, salary FROM employees WHERE salary BETWEEN 60000 AND 80000`

		fmt.Println("\nBETWEEN Operator:")
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

		// Should find employees with salaries between 60000 and 80000
		if result.Count == 0 {
			t.Error("Expected some employees with salary between 60000 and 80000")
		}

		// Verify all results are within range
		for _, row := range result.Rows {
			if salaryVal, exists := row["salary"]; exists {
				var salary float64
				switch v := salaryVal.(type) {
				case int:
					salary = float64(v)
				case int32:
					salary = float64(v)
				case int64:
					salary = float64(v)
				case float64:
					salary = v
				}
				if salary < 60000 || salary > 80000 {
					t.Errorf("Expected salary between 60000 and 80000, got %v", salary)
				}
			}
		}
	})

	t.Run("NOT BETWEEN Operator", func(t *testing.T) {
		sql := `SELECT name, salary FROM employees WHERE salary NOT BETWEEN 60000 AND 70000`

		fmt.Println("\nNOT BETWEEN Operator:")
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

		// Should find employees with salaries outside the range
		if result.Count == 0 {
			t.Error("Expected some employees with salary not between 60000 and 70000")
		}

		// Verify all results are outside range
		for _, row := range result.Rows {
			if salaryVal, exists := row["salary"]; exists {
				var salary float64
				switch v := salaryVal.(type) {
				case int:
					salary = float64(v)
				case int32:
					salary = float64(v)
				case int64:
					salary = float64(v)
				case float64:
					salary = v
				}
				if salary >= 60000 && salary <= 70000 {
					t.Errorf("Expected salary not between 60000 and 70000, got %v", salary)
				}
			}
		}
	})

	t.Run("Complex AND/OR Combination", func(t *testing.T) {
		sql := `SELECT name, department, salary FROM employees WHERE (department = 'Engineering' AND salary > 75000) OR (department = 'Sales' AND salary > 70000)`

		fmt.Println("\nComplex AND/OR Combination:")
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

		// Verify all results match the complex condition
		for _, row := range result.Rows {
			dept, deptExists := row["department"]
			salaryVal, salaryExists := row["salary"]

			if !deptExists || !salaryExists {
				t.Error("Missing department or salary data")
				continue
			}

			var salary float64
			switch v := salaryVal.(type) {
			case int:
				salary = float64(v)
			case int32:
				salary = float64(v)
			case int64:
				salary = float64(v)
			case float64:
				salary = v
			}

			// Should match either: (Engineering AND salary > 75000) OR (Sales AND salary > 70000)
			validCondition := (dept == "Engineering" && salary > 75000) ||
				(dept == "Sales" && salary > 70000)

			if !validCondition {
				t.Errorf("Row doesn't match complex condition: dept=%v, salary=%v", dept, salary)
			}
		}
	})

	t.Run("Multiple Conditions with AND", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Engineering' AND salary > 70000 AND age < 35`

		fmt.Println("\nMultiple Conditions with AND:")
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

		// This is a more restrictive query, so fewer results expected
		fmt.Printf("Found %d employees matching all three conditions\n", result.Count)
	})
}

func TestAdvancedWhereClauseParsing(t *testing.T) {
	parser := core.NewSQLParser()

	t.Run("Parse AND Condition", func(t *testing.T) {
		sql := "SELECT name FROM employees WHERE department = 'Engineering' AND salary > 70000"

		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse AND condition: %v", err)
		}

		fmt.Printf("\nAND Condition Parsing:\n")
		fmt.Printf("WHERE conditions: %d\n", len(parsed.Where))

		if len(parsed.Where) > 0 {
			condition := parsed.Where[0]
			fmt.Printf("Is complex: %v\n", condition.IsComplex)
			fmt.Printf("Logical operator: %s\n", condition.LogicalOp)

			if condition.IsComplex {
				fmt.Printf("Left condition: %s %s %v\n", condition.Left.Column, condition.Left.Operator, condition.Left.Value)
				fmt.Printf("Right condition: %s %s %v\n", condition.Right.Column, condition.Right.Operator, condition.Right.Value)
			}
		}
	})

	t.Run("Parse BETWEEN Condition", func(t *testing.T) {
		sql := "SELECT name FROM employees WHERE salary BETWEEN 60000 AND 80000"

		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatalf("Failed to parse BETWEEN condition: %v", err)
		}

		fmt.Printf("\nBETWEEN Condition Parsing:\n")
		fmt.Printf("WHERE conditions: %d\n", len(parsed.Where))

		if len(parsed.Where) > 0 {
			condition := parsed.Where[0]
			fmt.Printf("Column: %s\n", condition.Column)
			fmt.Printf("Operator: %s\n", condition.Operator)
			fmt.Printf("Value from: %v\n", condition.ValueFrom)
			fmt.Printf("Value to: %v\n", condition.ValueTo)
		}
	})
}

func TestAdvancedWhereClausesComprehensive(t *testing.T) {
	// Generate sample data
	generateSampleData()

	engine := core.NewQueryEngine("./data")

	t.Run("Simple AND with Two Conditions", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary > 70000 AND department = 'Engineering'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Simple AND: Found %d Engineering employees with salary > 70000\n", result.Count)

		// Verify all results match both conditions
		for _, row := range result.Rows {
			if dept, exists := row["department"]; exists && dept != "Engineering" {
				t.Errorf("Expected Engineering department, got %v", dept)
			}
			// This validates that AND logic works correctly
		}

		if result.Count == 0 {
			t.Error("Expected some results for Engineering employees with high salary")
		}
	})

	t.Run("Simple OR with Two Conditions", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Marketing' OR salary > 80000`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Simple OR: Found %d employees (Marketing OR salary > 80000)\n", result.Count)

		if result.Count == 0 {
			t.Error("Expected some results for Marketing employees or high salary")
		}
	})

	t.Run("Three Conditions with AND", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Engineering' AND salary > 70000 AND age > 25`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Three AND conditions: Found %d employees\n", result.Count)
		// This might return 0 results depending on data, which is valid
	})

	t.Run("Three Conditions with OR", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'HR' OR department = 'Finance' OR salary < 60000`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Three OR conditions: Found %d employees\n", result.Count)

		if result.Count == 0 {
			t.Error("Expected some results for HR, Finance, or low salary employees")
		}
	})

	t.Run("Mixed AND/OR without Parentheses", func(t *testing.T) {
		// This tests operator precedence (AND typically has higher precedence than OR)
		sql := `SELECT name FROM employees WHERE department = 'Engineering' AND salary > 75000 OR department = 'Sales'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Mixed AND/OR: Found %d employees\n", result.Count)

		// Should find: (Engineering AND salary > 75000) OR (all Sales employees)
		if result.Count == 0 {
			t.Error("Expected some results for mixed AND/OR condition")
		}
	})

	t.Run("String Comparison with AND", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE name = 'John Doe' AND department = 'Engineering'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("String comparison AND: Found %d employees\n", result.Count)

		// Should find exactly John Doe if he's in Engineering
		if result.Count > 1 {
			t.Error("Expected at most 1 result for specific name and department")
		}
	})

	t.Run("Numeric Ranges with OR", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary < 60000 OR salary > 80000`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Numeric ranges OR: Found %d employees\n", result.Count)

		// Verify all results are either < 60000 or > 80000
		for _, row := range result.Rows {
			if salaryVal, exists := row["salary"]; exists {
				var salary float64
				switch v := salaryVal.(type) {
				case int:
					salary = float64(v)
				case int32:
					salary = float64(v)
				case int64:
					salary = float64(v)
				case float64:
					salary = v
				}

				if salary >= 60000 && salary <= 80000 {
					t.Errorf("Found salary %v in excluded range [60000, 80000]", salary)
				}
			}
		}
	})

	t.Run("Edge Case - All AND Conditions False", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'NonExistent' AND salary > 1000000`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		if result.Count != 0 {
			t.Errorf("Expected 0 results for impossible conditions, got %d", result.Count)
		}

		fmt.Printf("Edge case AND (all false): Correctly returned %d results\n", result.Count)
	})

	t.Run("Edge Case - All OR Conditions True", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary >= 0 OR department != 'NonExistent'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		// Should return all employees (both conditions are likely true for all)
		fmt.Printf("Edge case OR (all true): Found %d employees\n", result.Count)

		if result.Count == 0 {
			t.Error("Expected all employees when all OR conditions are true")
		}
	})
}

func TestWhereClauseOperatorCombinations(t *testing.T) {
	// Generate sample data
	generateSampleData()

	engine := core.NewQueryEngine("./data")

	t.Run("Equality with AND", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Engineering' AND name = 'John Doe'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Equality AND: Found %d employees\n", result.Count)
	})

	t.Run("Greater Than with OR", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary > 80000 OR age > 40`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Greater than OR: Found %d employees\n", result.Count)
	})

	t.Run("Less Than with AND", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary < 70000 AND age < 35`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Less than AND: Found %d employees\n", result.Count)
	})

	t.Run("Not Equal with OR", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department != 'Engineering' OR salary != 75000`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Not equal OR: Found %d employees\n", result.Count)

		// Should find most employees (those not in Engineering OR not earning 75000)
		if result.Count == 0 {
			t.Error("Expected many results for not equal OR condition")
		}
	})

	t.Run("Mixed Comparison Operators", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary >= 70000 AND salary <= 80000 AND department = 'Engineering'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Mixed operators: Found %d employees\n", result.Count)

		// Verify all results are Engineering employees with salary in [70000, 80000]
		for _, row := range result.Rows {
			if dept, exists := row["department"]; exists && dept != "Engineering" {
				t.Errorf("Expected Engineering department, got %v", dept)
			}
			if salaryVal, exists := row["salary"]; exists {
				var salary float64
				switch v := salaryVal.(type) {
				case int:
					salary = float64(v)
				case int32:
					salary = float64(v)
				case int64:
					salary = float64(v)
				case float64:
					salary = v
				}

				if salary < 70000 || salary > 80000 {
					t.Errorf("Expected salary in [70000, 80000], got %v", salary)
				}
			}
		}
	})
}

func TestWhereClauseDataTypes(t *testing.T) {
	// Generate sample data
	generateSampleData()

	engine := core.NewQueryEngine("./data")

	t.Run("String AND String", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE department = 'Engineering' AND name = 'John Doe'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("String AND String: Found %d employees\n", result.Count)
	})

	t.Run("Integer OR Integer", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE age = 30 OR age = 25`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Integer OR Integer: Found %d employees\n", result.Count)
	})

	t.Run("Float AND String", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE salary > 75000.0 AND department = 'Engineering'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Float AND String: Found %d employees\n", result.Count)
	})

	t.Run("Multiple Data Types with OR", func(t *testing.T) {
		sql := `SELECT name FROM employees WHERE age < 30 OR salary > 80000 OR department = 'HR'`

		result, err := engine.Execute(sql)
		if err != nil {
			t.Errorf("Query execution failed: %v", err)
			return
		}
		if result.Error != "" {
			t.Errorf("Query failed: %s", result.Error)
			return
		}

		fmt.Printf("Multiple data types OR: Found %d employees\n", result.Count)
	})
}
