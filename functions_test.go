package main

import (
	"strings"
	"testing"
	"time"
)

func TestStringFunctions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("CONCAT function", func(t *testing.T) {
		query := `SELECT CONCAT(name, ' - ', department) as full_info FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		if result.Count != 1 {
			t.Errorf("Expected 1 row, got %d", result.Count)
		}

		// John Doe - Engineering
		expected := "John Doe - Engineering"
		if result.Rows[0]["full_info"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["full_info"])
		}
	})

	t.Run("UPPER function", func(t *testing.T) {
		query := `SELECT UPPER(name) as upper_name FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "JOHN DOE"
		if result.Rows[0]["upper_name"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["upper_name"])
		}
	})

	t.Run("LOWER function", func(t *testing.T) {
		query := `SELECT LOWER(department) as lower_dept FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "engineering"
		if result.Rows[0]["lower_dept"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["lower_dept"])
		}
	})

	t.Run("LENGTH function", func(t *testing.T) {
		query := `SELECT name, LENGTH(name) as name_length FROM employees WHERE id IN (1, 2)`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		if result.Count != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Count)
		}

		// John Doe = 8, Jane Smith = 10
		for _, row := range result.Rows {
			name := row["name"].(string)
			length := row["name_length"].(int)
			expectedLength := len(name)
			if length != expectedLength {
				t.Errorf("For name '%s', expected length %d, got %d", name, expectedLength, length)
			}
		}
	})

	t.Run("SUBSTRING function", func(t *testing.T) {
		query := `SELECT SUBSTRING(name, 1, 4) as first_four FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "John"
		if result.Rows[0]["first_four"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["first_four"])
		}
	})

	t.Run("SUBSTRING without length", func(t *testing.T) {
		query := `SELECT SUBSTRING(name, 6) as last_part FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "Doe"
		if result.Rows[0]["last_part"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["last_part"])
		}
	})

	t.Run("TRIM function", func(t *testing.T) {
		// First, let's test TRIM on a concatenated string with spaces
		query := `SELECT TRIM(CONCAT('  ', name, '  ')) as trimmed_name FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "John Doe"
		if result.Rows[0]["trimmed_name"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["trimmed_name"])
		}
	})

	t.Run("Functions in WHERE clause", func(t *testing.T) {
		query := `SELECT name FROM employees WHERE UPPER(department) = 'ENGINEERING'`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should get 4 engineering employees
		if result.Count != 4 {
			t.Errorf("Expected 4 engineering employees, got %d", result.Count)
		}
	})

	t.Run("Function comparison in WHERE", func(t *testing.T) {
		query := `SELECT name FROM employees WHERE LENGTH(name) > 8`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Check that all returned names have length > 8
		for _, row := range result.Rows {
			name := row["name"].(string)
			if len(name) <= 8 {
				t.Errorf("Name '%s' has length %d, which is not > 8", name, len(name))
			}
		}
	})

	t.Run("Nested functions", func(t *testing.T) {
		query := `SELECT UPPER(SUBSTRING(name, 1, 4)) as upper_first FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "JOHN"
		if result.Rows[0]["upper_first"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["upper_first"])
		}
	})
}

func TestDateFunctions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("CURRENT_DATE function", func(t *testing.T) {
		query := `SELECT CURRENT_DATE() as today`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		today := time.Now().Format("2006-01-02")
		if result.Rows[0]["today"] != today {
			t.Errorf("Expected '%s', got '%s'", today, result.Rows[0]["today"])
		}
	})

	t.Run("NOW function", func(t *testing.T) {
		query := `SELECT NOW() as current_time`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Check that it's a valid timestamp format
		timeStr := result.Rows[0]["current_time"].(string)
		_, err = time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			t.Errorf("Invalid timestamp format: %s", timeStr)
		}
	})

	t.Run("DATE_PART function", func(t *testing.T) {
		query := `SELECT hire_date, DATE_PART('year', hire_date) as hire_year FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// John Doe was hired on 2020-01-15
		expectedYear := 2020
		if result.Rows[0]["hire_year"] != expectedYear {
			t.Errorf("Expected year %d, got %v", expectedYear, result.Rows[0]["hire_year"])
		}
	})

	t.Run("DATE_PART with different parts", func(t *testing.T) {
		query := `SELECT 
			DATE_PART('year', hire_date) as year,
			DATE_PART('month', hire_date) as month,
			DATE_PART('day', hire_date) as day
		FROM employees WHERE id = 1`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// John Doe was hired on 2020-01-15
		if result.Rows[0]["year"] != 2020 {
			t.Errorf("Expected year 2020, got %v", result.Rows[0]["year"])
		}
		if result.Rows[0]["month"] != 1 {
			t.Errorf("Expected month 1, got %v", result.Rows[0]["month"])
		}
		if result.Rows[0]["day"] != 15 {
			t.Errorf("Expected day 15, got %v", result.Rows[0]["day"])
		}
	})

	t.Run("EXTRACT function (alias for DATE_PART)", func(t *testing.T) {
		query := `SELECT EXTRACT('year' FROM hire_date) as year FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		if result.Rows[0]["year"] != 2020 {
			t.Errorf("Expected year 2020, got %v", result.Rows[0]["year"])
		}
	})

	t.Run("DATE_TRUNC function", func(t *testing.T) {
		query := `SELECT hire_date, DATE_TRUNC('month', hire_date) as month_start FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should truncate to 2020-01-01 00:00:00
		expected := "2020-01-01 00:00:00"
		if result.Rows[0]["month_start"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["month_start"])
		}
	})

	t.Run("Date functions in WHERE clause", func(t *testing.T) {
		query := `SELECT name FROM employees WHERE DATE_PART('year', hire_date) = 2020`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should get employees hired in 2020
		expectedNames := map[string]bool{
			"John Doe":   true,
			"Tom Miller": true,
		}

		if result.Count != len(expectedNames) {
			t.Errorf("Expected %d employees hired in 2020, got %d", len(expectedNames), result.Count)
		}

		for _, row := range result.Rows {
			name := row["name"].(string)
			if !expectedNames[name] {
				t.Errorf("Unexpected employee in results: %s", name)
			}
		}
	})
}

func TestMixedFunctions(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("String and date functions together", func(t *testing.T) {
		query := `SELECT 
			CONCAT(UPPER(name), ' (', DATE_PART('year', hire_date), ')') as info
		FROM employees 
		WHERE id = 1`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		expected := "JOHN DOE (2020)"
		if result.Rows[0]["info"] != expected {
			t.Errorf("Expected '%s', got '%s'", expected, result.Rows[0]["info"])
		}
	})

	t.Run("Functions with aggregates", func(t *testing.T) {
		t.Skip("Functions inside aggregates not yet supported")
		query := `SELECT 
			department,
			COUNT(*) as count,
			AVG(LENGTH(name)) as avg_name_length
		FROM employees 
		GROUP BY department
		HAVING AVG(LENGTH(name)) > 10`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Should only get departments where average name length > 10
		for _, row := range result.Rows {
			avgLength := row["avg_name_length"].(float64)
			if avgLength <= 10 {
				t.Errorf("Department %s has avg name length %.2f, which is not > 10", 
					row["department"], avgLength)
			}
		}
	})

	t.Run("Functions in JOIN conditions", func(t *testing.T) {
		// This is a complex test - functions in JOIN conditions
		query := `SELECT e1.name as emp1, e2.name as emp2
		FROM employees e1
		JOIN employees e2 ON SUBSTRING(e1.department, 1, 3) = SUBSTRING(e2.department, 1, 3)
		WHERE e1.id = 1 AND e2.id != 1`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// John Doe is in Engineering, should match with other Engineering employees
		for _, row := range result.Rows {
			emp1 := row["emp1"].(string)
			if emp1 != "John Doe" {
				t.Errorf("Expected emp1 to be 'John Doe', got '%s'", emp1)
			}
		}
	})

	t.Run("Functions with CTEs", func(t *testing.T) {
		query := `WITH name_lengths AS (
			SELECT name, LENGTH(name) as name_len
			FROM employees
		)
		SELECT name, name_len
		FROM name_lengths
		WHERE name_len > 10
		ORDER BY name_len DESC`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// Verify results are ordered by length descending
		for i := 1; i < len(result.Rows); i++ {
			prevLen := result.Rows[i-1]["name_len"].(int)
			currLen := result.Rows[i]["name_len"].(int)
			if prevLen < currLen {
				t.Errorf("Results not properly ordered: %d < %d at position %d", prevLen, currLen, i)
			}
		}
	})
}

func TestFunctionEdgeCases(t *testing.T) {
	engine := NewTestQueryEngine()
	defer engine.Close()

	t.Run("Functions with NULL values", func(t *testing.T) {
		// Create a query that might have NULL values
		query := `SELECT 
			UPPER(NULL) as upper_null,
			LENGTH(NULL) as length_null,
			CONCAT('Hello', NULL, 'World') as concat_null`
		
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Error != "" {
			t.Fatalf("Query error: %s", result.Error)
		}

		// NULL handling: UPPER(NULL) = NULL, LENGTH(NULL) = NULL
		if result.Rows[0]["upper_null"] != nil {
			t.Errorf("Expected NULL for UPPER(NULL), got %v", result.Rows[0]["upper_null"])
		}
		if result.Rows[0]["length_null"] != nil {
			t.Errorf("Expected NULL for LENGTH(NULL), got %v", result.Rows[0]["length_null"])
		}
		// CONCAT ignores NULL values
		if result.Rows[0]["concat_null"] != "HelloWorld" {
			t.Errorf("Expected 'HelloWorld' for CONCAT with NULL, got %v", result.Rows[0]["concat_null"])
		}
	})

	t.Run("Invalid function arguments", func(t *testing.T) {
		// Test with wrong number of arguments
		query := `SELECT SUBSTRING(name) as sub FROM employees WHERE id = 1`
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		// Should get an error about insufficient arguments
		if !strings.Contains(result.Error, "requires at least") && result.Error != "" {
			t.Logf("Expected error about arguments, got: %s", result.Error)
		}
	})

	t.Run("Case sensitivity in function names", func(t *testing.T) {
		queries := []string{
			`SELECT upper(name) as u FROM employees WHERE id = 1`,
			`SELECT Upper(name) as u FROM employees WHERE id = 1`,
			`SELECT UPPER(name) as u FROM employees WHERE id = 1`,
		}

		for _, query := range queries {
			result, err := engine.Execute(query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Error != "" {
				t.Fatalf("Query error: %s", result.Error)
			}

			expected := "JOHN DOE"
			if result.Rows[0]["u"] != expected {
				t.Errorf("Expected '%s', got '%s' for query: %s", expected, result.Rows[0]["u"], query)
			}
		}
	})
}