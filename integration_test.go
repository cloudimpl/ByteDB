package main

import (
	"testing"
	"time"
)

// Integration tests that verify the entire system works end-to-end
func TestEndToEndQueries(t *testing.T) {
	// Generate test data
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	tests := []struct {
		name         string
		sql          string
		expectedRows int
		shouldError  bool
	}{
		{
			name:         "Basic SELECT with LIMIT",
			sql:          "SELECT * FROM employees LIMIT 2;",
			expectedRows: 2,
			shouldError:  false,
		},
		{
			name:         "Numeric WHERE clause",
			sql:          "SELECT name FROM employees WHERE salary > 80000;",
			expectedRows: 1, // Only Lisa Davis (85000) - Mike Johnson is exactly 80000
			shouldError:  false,
		},
		{
			name:         "String WHERE clause",
			sql:          "SELECT name FROM employees WHERE department = 'Engineering';",
			expectedRows: 4,
			shouldError:  false,
		},
		{
			name:         "Column selection",
			sql:          "SELECT name, salary FROM employees LIMIT 1;",
			expectedRows: 1,
			shouldError:  false,
		},
		{
			name:         "Greater than operator",
			sql:          "SELECT name FROM products WHERE price > 100;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "Less than operator",
			sql:          "SELECT name FROM products WHERE price < 30;",
			expectedRows: 5,
			shouldError:  false,
		},
		{
			name:         "Greater than or equal operator",
			sql:          "SELECT name FROM products WHERE price >= 199.99;",
			expectedRows: 3, // Laptop (999.99), Monitor (299.99), Desk Chair (199.99)
			shouldError:  false,
		},
		{
			name:         "Empty result set",
			sql:          "SELECT * FROM products WHERE price > 10000;",
			expectedRows: 0,
			shouldError:  false,
		},
		{
			name:         "Type coercion - int comparison",
			sql:          "SELECT name FROM products WHERE price > 199;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "Type coercion - float comparison",
			sql:          "SELECT name FROM products WHERE price > 199.99;",
			expectedRows: 2, // Laptop (999.99), Monitor (299.99) - Desk Chair is exactly 199.99
			shouldError:  false,
		},
		{
			name:        "Invalid table name",
			sql:         "SELECT * FROM invalid_table;",
			shouldError: true,
		},
		{
			name:         "ORDER BY salary DESC",
			sql:          "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "ORDER BY salary ASC",
			sql:          "SELECT name, salary FROM employees ORDER BY salary ASC LIMIT 3;",
			expectedRows: 3,
			shouldError:  false,
		},
		{
			name:         "Multi-column ORDER BY",
			sql:          "SELECT name, department, salary FROM employees ORDER BY department ASC, salary DESC;",
			expectedRows: 10, // All employees
			shouldError:  false,
		},
		{
			name:         "ORDER BY with WHERE",
			sql:          "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;",
			expectedRows: 4,
			shouldError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.sql)
			
			if err != nil {
				t.Errorf("Unexpected error from Execute: %v", err)
				return
			}

			if tt.shouldError {
				if result.Error == "" {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if result.Error != "" {
				t.Errorf("Unexpected error in result: %s", result.Error)
				return
			}

			if result.Count != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, result.Count)
				t.Logf("SQL: %s", tt.sql)
				t.Logf("Result: %+v", result)
			}
		})
	}
}

// Test that verifies specific query results are correct
func TestQueryResultAccuracy(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test engineering employees
	result, err := engine.Execute("SELECT name FROM employees WHERE department = 'Engineering';")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	expectedEngineers := map[string]bool{
		"John Doe":       true,
		"Mike Johnson":   true,
		"Lisa Davis":     true,
		"Chris Anderson": true,
	}

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 engineering employees, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		name, exists := row["name"]
		if !exists {
			t.Errorf("Row missing name column: %+v", row)
			continue
		}
		
		nameStr, ok := name.(string)
		if !ok {
			t.Errorf("Name is not a string: %v (%T)", name, name)
			continue
		}
		
		if !expectedEngineers[nameStr] {
			t.Errorf("Unexpected engineer: %s", nameStr)
		}
	}

	// Test high-priced products
	result, err = engine.Execute("SELECT name, price FROM products WHERE price > 100;")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	expectedExpensive := map[string]float64{
		"Laptop":     999.99,
		"Monitor":    299.99,
		"Desk Chair": 199.99,
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 expensive products, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		name := row["name"].(string)
		price := row["price"].(float64)
		
		expectedPrice, exists := expectedExpensive[name]
		if !exists {
			t.Errorf("Unexpected expensive product: %s", name)
			continue
		}
		
		if price != expectedPrice {
			t.Errorf("Price mismatch for %s: expected %.2f, got %.2f", name, expectedPrice, price)
		}
		
		if price <= 100 {
			t.Errorf("Product %s should have price > 100, got %.2f", name, price)
		}
	}
}

// Test that verifies ORDER BY results are correctly sorted
func TestOrderByFunctionality(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test ORDER BY salary DESC
	result, err := engine.Execute("SELECT name, salary FROM employees ORDER BY salary DESC;")
	if err != nil {
		t.Fatalf("Failed to execute ORDER BY query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) == 0 {
		t.Fatal("No results returned")
	}

	// Verify descending order
	for i := 1; i < len(result.Rows); i++ {
		prevSalary := result.Rows[i-1]["salary"].(float64)
		currSalary := result.Rows[i]["salary"].(float64)
		
		if prevSalary < currSalary {
			t.Errorf("ORDER BY DESC failed: salary[%d]=%.2f should be >= salary[%d]=%.2f", 
				i-1, prevSalary, i, currSalary)
		}
	}

	// Test ORDER BY salary ASC
	result, err = engine.Execute("SELECT name, salary FROM employees ORDER BY salary ASC;")
	if err != nil {
		t.Fatalf("Failed to execute ORDER BY ASC query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Verify ascending order
	for i := 1; i < len(result.Rows); i++ {
		prevSalary := result.Rows[i-1]["salary"].(float64)
		currSalary := result.Rows[i]["salary"].(float64)
		
		if prevSalary > currSalary {
			t.Errorf("ORDER BY ASC failed: salary[%d]=%.2f should be <= salary[%d]=%.2f", 
				i-1, prevSalary, i, currSalary)
		}
	}

	// Test multi-column ORDER BY: department ASC, salary DESC
	result, err = engine.Execute("SELECT name, department, salary FROM employees ORDER BY department ASC, salary DESC;")
	if err != nil {
		t.Fatalf("Failed to execute multi-column ORDER BY query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Verify multi-column ordering
	for i := 1; i < len(result.Rows); i++ {
		prevDept := result.Rows[i-1]["department"].(string)
		currDept := result.Rows[i]["department"].(string)
		prevSalary := result.Rows[i-1]["salary"].(float64)
		currSalary := result.Rows[i]["salary"].(float64)
		
		if prevDept < currDept {
			// Previous department comes first alphabetically - correct
			continue
		} else if prevDept == currDept {
			// Same department - salary should be in DESC order
			if prevSalary < currSalary {
				t.Errorf("Multi-column ORDER BY failed: in dept '%s', salary[%d]=%.2f should be >= salary[%d]=%.2f", 
					currDept, i-1, prevSalary, i, currSalary)
			}
		} else {
			// Previous department comes after current - this is wrong
			t.Errorf("Multi-column ORDER BY failed: department '%s' should come before '%s'", 
				currDept, prevDept)
		}
	}

	// Test ORDER BY with WHERE clause
	result, err = engine.Execute("SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;")
	if err != nil {
		t.Fatalf("Failed to execute ORDER BY with WHERE query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Verify all results are from Engineering and sorted by salary DESC
	for i, row := range result.Rows {
		// Can't check department since it's not in SELECT, but we can verify salary ordering
		if i > 0 {
			prevSalary := result.Rows[i-1]["salary"].(float64)
			currSalary := row["salary"].(float64)
			
			if prevSalary < currSalary {
				t.Errorf("ORDER BY with WHERE failed: salary[%d]=%.2f should be >= salary[%d]=%.2f", 
					i-1, prevSalary, i, currSalary)
			}
		}
	}

	// Expected 4 engineering employees
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 engineering employees, got %d", len(result.Rows))
	}
}

// Test aggregate functions (COUNT, SUM, AVG, MIN, MAX)
func TestAggregateFunctions(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test simple COUNT(*)
	result, err := engine.Execute("SELECT COUNT(*) FROM employees;")
	if err != nil {
		t.Fatalf("Failed to execute COUNT(*) query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 result row for COUNT(*), got %d", len(result.Rows))
	}

	count := result.Rows[0]["count"].(float64)
	if count != 10 {
		t.Errorf("Expected COUNT(*) = 10, got %.0f", count)
	}

	// Test multiple aggregates
	result, err = engine.Execute("SELECT COUNT(*), AVG(salary), MAX(salary), MIN(salary), SUM(salary) FROM employees;")
	if err != nil {
		t.Fatalf("Failed to execute multiple aggregates query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 result row for aggregates, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	
	// Verify results
	expectedCount := 10.0
	expectedSum := 710000.0 // Sum of all salaries
	expectedAvg := expectedSum / expectedCount
	expectedMax := 85000.0 // Lisa Davis
	expectedMin := 55000.0 // Sarah Wilson

	if row["count"].(float64) != expectedCount {
		t.Errorf("Expected COUNT(*) = %.0f, got %.0f", expectedCount, row["count"].(float64))
	}

	if row["sum_salary"].(float64) != expectedSum {
		t.Errorf("Expected SUM(salary) = %.0f, got %.0f", expectedSum, row["sum_salary"].(float64))
	}

	avgSalary := row["avg_salary"].(float64)
	if avgSalary != expectedAvg {
		t.Errorf("Expected AVG(salary) = %.0f, got %.0f", expectedAvg, avgSalary)
	}

	if row["max_salary"].(float64) != expectedMax {
		t.Errorf("Expected MAX(salary) = %.0f, got %.0f", expectedMax, row["max_salary"].(float64))
	}

	if row["min_salary"].(float64) != expectedMin {
		t.Errorf("Expected MIN(salary) = %.0f, got %.0f", expectedMin, row["min_salary"].(float64))
	}
}

// Test GROUP BY functionality
func TestGroupByFunctionality(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test GROUP BY with COUNT
	result, err := engine.Execute("SELECT department, COUNT(*) FROM employees GROUP BY department;")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should have 5 departments: Engineering, Marketing, HR, Sales, Finance
	if len(result.Rows) != 5 {
		t.Fatalf("Expected 5 departments, got %d", len(result.Rows))
	}

	// Build a map to check department counts
	deptCounts := make(map[string]float64)
	for _, row := range result.Rows {
		dept := row["department"].(string)
		count := row["count"].(float64)
		deptCounts[dept] = count
	}

	// Expected department counts based on sample data
	expected := map[string]float64{
		"Engineering": 4,
		"Marketing":   2,
		"HR":          1,
		"Sales":       2,
		"Finance":     1,
	}

	for dept, expectedCount := range expected {
		if actualCount, exists := deptCounts[dept]; !exists {
			t.Errorf("Department %s missing from results", dept)
		} else if actualCount != expectedCount {
			t.Errorf("Department %s: expected count %.0f, got %.0f", dept, expectedCount, actualCount)
		}
	}

	// Test GROUP BY with multiple aggregates
	result, err = engine.Execute("SELECT department, COUNT(*), AVG(salary), MAX(salary) FROM employees GROUP BY department ORDER BY department;")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY with multiple aggregates: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 5 {
		t.Fatalf("Expected 5 departments, got %d", len(result.Rows))
	}

	// Verify Engineering department stats (should be first after ORDER BY)
	found := false
	for _, row := range result.Rows {
		if row["department"].(string) == "Engineering" {
			found = true
			if row["count"].(float64) != 4 {
				t.Errorf("Engineering count: expected 4, got %.0f", row["count"].(float64))
			}
			// Engineering average: (75000 + 80000 + 85000 + 78000) / 4 = 79500
			avgSalary := row["avg_salary"].(float64)
			if avgSalary != 79500 {
				t.Errorf("Engineering avg salary: expected 79500, got %.0f", avgSalary)
			}
			if row["max_salary"].(float64) != 85000 {
				t.Errorf("Engineering max salary: expected 85000, got %.0f", row["max_salary"].(float64))
			}
			break
		}
	}
	
	if !found {
		t.Error("Engineering department not found in GROUP BY results")
	}
}

// Test aggregate functions with WHERE clause
func TestAggregatesWithWhere(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test COUNT with WHERE
	result, err := engine.Execute("SELECT COUNT(*) FROM employees WHERE salary > 70000;")
	if err != nil {
		t.Fatalf("Failed to execute COUNT with WHERE: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Employees with salary > 70000: John Doe (75000), Mike Johnson (80000), 
	// Lisa Davis (85000), Chris Anderson (78000), Maria Rodriguez (72000) = 5 employees
	count := result.Rows[0]["count"].(float64)
	if count != 5 {
		t.Errorf("Expected COUNT(*) with WHERE salary > 70000 = 5, got %.0f", count)
	}

	// Test GROUP BY with WHERE
	result, err = engine.Execute("SELECT department, COUNT(*) FROM employees WHERE salary > 60000 GROUP BY department ORDER BY department;")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY with WHERE: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should exclude employees with salary <= 60000 (Sarah Wilson: 55000, Tom Miller: 62000 should be excluded)
	// Actually Tom Miller (62000) should be included since 62000 > 60000
	// So we exclude only Sarah Wilson (55000)
	
	deptCounts := make(map[string]float64)
	for _, row := range result.Rows {
		dept := row["department"].(string)
		count := row["count"].(float64)
		deptCounts[dept] = count
	}

	// Expected counts for employees with salary > 60000
	expected := map[string]float64{
		"Engineering": 4, // All engineering employees have > 60000
		"Marketing":   2, // Jane Smith (65000), Tom Miller (62000)
		"Sales":       2, // David Brown (70000), Maria Rodriguez (72000)
		"Finance":     1, // Anna Garcia (68000)
		// HR should be missing since Sarah Wilson (55000) <= 60000
	}

	for dept, expectedCount := range expected {
		if actualCount, exists := deptCounts[dept]; !exists {
			t.Errorf("Department %s missing from filtered results", dept)
		} else if actualCount != expectedCount {
			t.Errorf("Department %s with salary > 60000: expected count %.0f, got %.0f", dept, expectedCount, actualCount)
		}
	}

	// HR should not be in the results
	if _, exists := deptCounts["HR"]; exists {
		t.Error("HR department should not be in results (no employees with salary > 60000)")
	}
}

// Test IN operator functionality
func TestInOperator(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test IN operator with string values
	result, err := engine.Execute("SELECT name, department FROM employees WHERE department IN ('Engineering', 'Sales');")
	if err != nil {
		t.Fatalf("Failed to execute IN query with strings: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return Engineering (4) + Sales (2) = 6 employees
	if len(result.Rows) != 6 {
		t.Errorf("Expected 6 employees from Engineering and Sales, got %d", len(result.Rows))
	}

	// Verify all returned employees are from the specified departments
	validDepts := map[string]bool{"Engineering": true, "Sales": true}
	for _, row := range result.Rows {
		dept := row["department"].(string)
		if !validDepts[dept] {
			t.Errorf("Found employee from unexpected department: %s", dept)
		}
	}

	// Test IN operator with numeric values
	result, err = engine.Execute("SELECT name, salary FROM employees WHERE salary IN (75000, 80000, 85000);")
	if err != nil {
		t.Fatalf("Failed to execute IN query with numbers: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return John Doe (75000), Mike Johnson (80000), Lisa Davis (85000)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 employees with specified salaries, got %d", len(result.Rows))
	}

	expectedSalaries := map[float64]bool{75000: true, 80000: true, 85000: true}
	for _, row := range result.Rows {
		salary := row["salary"].(float64)
		if !expectedSalaries[salary] {
			t.Errorf("Found employee with unexpected salary: %.0f", salary)
		}
	}

	// Test IN operator with float values (products table)
	result, err = engine.Execute("SELECT name, price FROM products WHERE price IN (29.99, 199.99, 999.99);")
	if err != nil {
		t.Fatalf("Failed to execute IN query with floats: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return Mouse (29.99), Desk Chair (199.99), Laptop (999.99)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 products with specified prices, got %d", len(result.Rows))
	}

	expectedPrices := map[float64]bool{29.99: true, 199.99: true, 999.99: true}
	for _, row := range result.Rows {
		price := row["price"].(float64)
		if !expectedPrices[price] {
			t.Errorf("Found product with unexpected price: %.2f", price)
		}
	}

	// Test IN operator with single value
	result, err = engine.Execute("SELECT name FROM employees WHERE department IN ('HR');")
	if err != nil {
		t.Fatalf("Failed to execute IN query with single value: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return only Sarah Wilson from HR
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 HR employee, got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		name := result.Rows[0]["name"].(string)
		if name != "Sarah Wilson" {
			t.Errorf("Expected Sarah Wilson from HR, got %s", name)
		}
	}

	// Test IN operator with no matches
	result, err = engine.Execute("SELECT name FROM employees WHERE department IN ('NonExistent', 'AlsoNonExistent');")
	if err != nil {
		t.Fatalf("Failed to execute IN query with no matches: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return no results
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 employees for non-existent departments, got %d", len(result.Rows))
	}
}

// Test IN operator with ORDER BY and other clauses
func TestInOperatorWithOtherClauses(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test IN with ORDER BY
	result, err := engine.Execute("SELECT name, department, salary FROM employees WHERE department IN ('Engineering', 'Marketing') ORDER BY salary DESC;")
	if err != nil {
		t.Fatalf("Failed to execute IN with ORDER BY: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return Engineering (4) + Marketing (2) = 6 employees, ordered by salary DESC
	if len(result.Rows) != 6 {
		t.Errorf("Expected 6 employees from Engineering and Marketing, got %d", len(result.Rows))
	}

	// Verify ordering (salaries should be in descending order)
	for i := 1; i < len(result.Rows); i++ {
		prevSalary := result.Rows[i-1]["salary"].(float64)
		currSalary := result.Rows[i]["salary"].(float64)
		
		if prevSalary < currSalary {
			t.Errorf("ORDER BY DESC failed with IN: salary[%d]=%.0f should be >= salary[%d]=%.0f", 
				i-1, prevSalary, i, currSalary)
		}
	}

	// Test IN with LIMIT
	result, err = engine.Execute("SELECT name, department FROM employees WHERE department IN ('Engineering', 'Sales', 'Marketing') ORDER BY name LIMIT 3;")
	if err != nil {
		t.Fatalf("Failed to execute IN with LIMIT: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return exactly 3 results due to LIMIT
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 employees due to LIMIT, got %d", len(result.Rows))
	}

	// Test IN with COUNT aggregate
	result, err = engine.Execute("SELECT COUNT(*) FROM employees WHERE department IN ('Engineering', 'Sales');")
	if err != nil {
		t.Fatalf("Failed to execute COUNT with IN: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	count := result.Rows[0]["count"].(float64)
	if count != 6 {
		t.Errorf("Expected COUNT(*) with IN = 6, got %.0f", count)
	}

	// Test IN with GROUP BY
	result, err = engine.Execute("SELECT department, COUNT(*) FROM employees WHERE department IN ('Engineering', 'Sales', 'Marketing') GROUP BY department ORDER BY department;")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY with IN: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 3 departments: Engineering, Marketing, Sales
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 departments in GROUP BY with IN, got %d", len(result.Rows))
	}

	// Build counts map to verify
	deptCounts := make(map[string]float64)
	for _, row := range result.Rows {
		dept := row["department"].(string)
		count := row["count"].(float64)
		deptCounts[dept] = count
	}

	expected := map[string]float64{
		"Engineering": 4,
		"Marketing":   2,
		"Sales":       2,
	}

	for dept, expectedCount := range expected {
		if actualCount, exists := deptCounts[dept]; !exists {
			t.Errorf("Department %s missing from GROUP BY with IN results", dept)
		} else if actualCount != expectedCount {
			t.Errorf("Department %s: expected count %.0f, got %.0f", dept, expectedCount, actualCount)
		}
	}
}

// Test enhanced LIKE pattern matching
func TestLikePatterns(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test prefix matching
	result, err := engine.Execute("SELECT name FROM employees WHERE name LIKE 'J%';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE prefix query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return John Doe and Jane Smith
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 employees starting with 'J', got %d", len(result.Rows))
	}

	expectedNames := map[string]bool{"John Doe": true, "Jane Smith": true}
	for _, row := range result.Rows {
		name := row["name"].(string)
		if !expectedNames[name] {
			t.Errorf("Unexpected employee name: %s", name)
		}
	}

	// Test substring matching
	result, err = engine.Execute("SELECT name FROM employees WHERE name LIKE '%John%';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE substring query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return John Doe and Mike Johnson
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 employees containing 'John', got %d", len(result.Rows))
	}

	expectedJohns := map[string]bool{"John Doe": true, "Mike Johnson": true}
	for _, row := range result.Rows {
		name := row["name"].(string)
		if !expectedJohns[name] {
			t.Errorf("Unexpected employee name for John pattern: %s", name)
		}
	}

	// Test single character wildcard
	result, err = engine.Execute("SELECT name FROM employees WHERE name LIKE '_ohn%';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE single char query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return only John Doe (first char matches, then "ohn")
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 employee matching '_ohn%%', got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		name := result.Rows[0]["name"].(string)
		if name != "John Doe" {
			t.Errorf("Expected John Doe for '_ohn%%' pattern, got %s", name)
		}
	}

	// Test exact match
	result, err = engine.Execute("SELECT name FROM employees WHERE name LIKE 'John Doe';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE exact match query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 employee for exact match, got %d", len(result.Rows))
	}

	// Test no matches
	result, err = engine.Execute("SELECT name FROM employees WHERE name LIKE 'Z%';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE no match query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 employees starting with 'Z', got %d", len(result.Rows))
	}

	// Test with products table
	result, err = engine.Execute("SELECT name, price FROM products WHERE name LIKE '%top%';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE on products: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return Laptop
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 product containing 'top', got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		name := result.Rows[0]["name"].(string)
		if name != "Laptop" {
			t.Errorf("Expected Laptop for '%%top%%' pattern, got %s", name)
		}
	}
}

// Test LIKE with other SQL clauses
func TestLikeWithOtherClauses(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test LIKE with ORDER BY
	result, err := engine.Execute("SELECT name, salary FROM employees WHERE name LIKE '%a%' ORDER BY salary DESC;")
	if err != nil {
		t.Fatalf("Failed to execute LIKE with ORDER BY: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return employees with 'a' in name, ordered by salary DESC
	if len(result.Rows) == 0 {
		t.Error("Expected some employees with 'a' in name")
	}

	// Verify ordering (salaries should be in descending order)
	for i := 1; i < len(result.Rows); i++ {
		prevSalary := result.Rows[i-1]["salary"].(float64)
		currSalary := result.Rows[i]["salary"].(float64)
		
		if prevSalary < currSalary {
			t.Errorf("ORDER BY DESC failed with LIKE: salary[%d]=%.0f should be >= salary[%d]=%.0f", 
				i-1, prevSalary, i, currSalary)
		}
	}

	// Test LIKE with COUNT aggregate
	result, err = engine.Execute("SELECT COUNT(*) FROM employees WHERE name LIKE '%e%';")
	if err != nil {
		t.Fatalf("Failed to execute COUNT with LIKE: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	count := result.Rows[0]["count"].(float64)
	// Names with 'e': John Doe, Jane Smith, Mike Johnson, Anna Garcia, Chris Anderson, Maria Rodriguez
	// That's at least 6 employees
	if count < 6 {
		t.Errorf("Expected at least 6 employees with 'e' in name, got %.0f", count)
	}

	// Test LIKE with LIMIT
	result, err = engine.Execute("SELECT name FROM employees WHERE name LIKE '%o%' ORDER BY name LIMIT 2;")
	if err != nil {
		t.Fatalf("Failed to execute LIKE with LIMIT: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return exactly 2 results due to LIMIT
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 employees due to LIMIT, got %d", len(result.Rows))
		// Debug: show what we actually got
		for _, row := range result.Rows {
			t.Logf("Got employee: %s", row["name"].(string))
		}
	}

	// Test LIKE with GROUP BY
	result, err = engine.Execute("SELECT department, COUNT(*) FROM employees WHERE name LIKE '%a%' GROUP BY department ORDER BY department;")
	if err != nil {
		t.Fatalf("Failed to execute LIKE with GROUP BY: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should have some departments with employees having 'a' in their names
	if len(result.Rows) == 0 {
		t.Error("Expected some departments with employees having 'a' in names")
	}

	// Verify all counts are positive
	for _, row := range result.Rows {
		count := row["count"].(float64)
		if count <= 0 {
			t.Errorf("Expected positive count for departments, got %.0f", count)
		}
	}
}

// Test column pruning functionality
func TestColumnPruning(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test selecting specific columns
	result, err := engine.Execute("SELECT name, salary FROM employees LIMIT 3;")
	if err != nil {
		t.Fatalf("Failed to execute column pruning query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 3 rows with only name and salary columns
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// Verify only the requested columns are present
	expectedColumns := map[string]bool{"name": true, "salary": true}
	for _, row := range result.Rows {
		for col := range row {
			if !expectedColumns[col] {
				t.Errorf("Unexpected column in result: %s", col)
			}
		}
		
		// Verify expected columns are present
		if _, exists := row["name"]; !exists {
			t.Error("Missing 'name' column in result")
		}
		if _, exists := row["salary"]; !exists {
			t.Error("Missing 'salary' column in result")
		}
	}

	// Test single column selection
	result, err = engine.Execute("SELECT name FROM employees WHERE department = 'Engineering';")
	if err != nil {
		t.Fatalf("Failed to execute single column query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 4 engineering employees with only name column
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 engineering employees, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		if len(row) != 1 {
			t.Errorf("Expected 1 column in result, got %d", len(row))
		}
		if _, exists := row["name"]; !exists {
			t.Error("Missing 'name' column in single column result")
		}
	}

	// Test that SELECT * still returns all columns
	result, err = engine.Execute("SELECT * FROM employees LIMIT 1;")
	if err != nil {
		t.Fatalf("Failed to execute SELECT * query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}

	// Should have all employee columns
	expectedAllColumns := []string{"id", "name", "department", "salary", "age", "hire_date"}
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != len(expectedAllColumns) {
			t.Errorf("Expected %d columns for SELECT *, got %d", len(expectedAllColumns), len(row))
		}
		
		for _, col := range expectedAllColumns {
			if _, exists := row[col]; !exists {
				t.Errorf("Missing column '%s' in SELECT * result", col)
			}
		}
	}

	// Test column pruning with WHERE clause
	result, err = engine.Execute("SELECT salary FROM employees WHERE name LIKE 'John%';")
	if err != nil {
		t.Fatalf("Failed to execute column pruning with WHERE: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return John Doe with only salary column
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 John, got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != 1 {
			t.Errorf("Expected 1 column (salary), got %d", len(row))
		}
		if _, exists := row["salary"]; !exists {
			t.Error("Missing 'salary' column")
		}
	}
}

// Test column pruning with aggregates and GROUP BY
func TestColumnPruningWithAggregates(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Test aggregate with specific column
	result, err := engine.Execute("SELECT COUNT(*), AVG(salary) FROM employees WHERE department = 'Engineering';")
	if err != nil {
		t.Fatalf("Failed to execute aggregate query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 1 row with aggregates
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 aggregate row, got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if _, exists := row["count"]; !exists {
			t.Error("Missing 'count' column in aggregate result")
		}
		if _, exists := row["avg_salary"]; !exists {
			t.Error("Missing 'avg_salary' column in aggregate result")
		}
	}

	// Test GROUP BY with column pruning
	result, err = engine.Execute("SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY department;")
	if err != nil {
		t.Fatalf("Failed to execute GROUP BY query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 5 departments
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 departments, got %d", len(result.Rows))
	}

	// Verify each row has department and count columns
	for _, row := range result.Rows {
		if len(row) != 2 {
			t.Errorf("Expected 2 columns (department, count), got %d", len(row))
		}
		if _, exists := row["department"]; !exists {
			t.Error("Missing 'department' column in GROUP BY result")
		}
		if _, exists := row["count"]; !exists {
			t.Error("Missing 'count' column in GROUP BY result")
		}
	}

	// Test ORDER BY with column pruning
	result, err = engine.Execute("SELECT name FROM employees ORDER BY salary DESC LIMIT 3;")
	if err != nil {
		t.Fatalf("Failed to execute ORDER BY query: %v", err)
	}
	
	if result.Error != "" {
		t.Fatalf("Query returned error: %s", result.Error)
	}

	// Should return 3 employees with only name column, ordered by salary (which was needed for sorting)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 employees, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		if len(row) != 1 {
			t.Errorf("Expected 1 column (name), got %d", len(row))
		}
		if _, exists := row["name"]; !exists {
			t.Error("Missing 'name' column in ORDER BY result")
		}
	}
}

// Test query result caching functionality
func TestQueryCaching(t *testing.T) {
	generateSampleData()
	
	// Create engine with custom cache configuration
	cacheConfig := CacheConfig{
		MaxMemoryMB: 10,                  // Small cache for testing
		DefaultTTL:  1 * time.Minute,    // 1 minute TTL for testing
		Enabled:     true,
	}
	engine := NewQueryEngineWithCache("./data", cacheConfig)
	defer engine.Close()

	// Test basic caching
	query := "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;"
	
	// First execution should miss cache and execute query
	result1, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute first query: %v", err)
	}
	
	if result1.Error != "" {
		t.Fatalf("First query returned error: %s", result1.Error)
	}

	// Check cache stats after first query
	stats := engine.GetCacheStats()
	if stats.TotalQueries != 1 {
		t.Errorf("Expected 1 total query, got %d", stats.TotalQueries)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 cache miss, got %d", stats.Misses)
	}
	if stats.Hits != 0 {
		t.Errorf("Expected 0 cache hits, got %d", stats.Hits)
	}

	// Second execution should hit cache
	result2, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute second query: %v", err)
	}
	
	if result2.Error != "" {
		t.Fatalf("Second query returned error: %s", result2.Error)
	}

	// Results should be identical
	if len(result1.Rows) != len(result2.Rows) {
		t.Errorf("Cached result has different row count: %d vs %d", len(result1.Rows), len(result2.Rows))
	}

	// Check cache stats after second query
	stats = engine.GetCacheStats()
	if stats.TotalQueries != 2 {
		t.Errorf("Expected 2 total queries, got %d", stats.TotalQueries)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 cache miss, got %d", stats.Misses)
	}
	if stats.Hits != 1 {
		t.Errorf("Expected 1 cache hit, got %d", stats.Hits)
	}

	// Verify hit rate calculation
	expectedHitRate := 50.0 // 1 hit out of 2 queries
	if stats.GetHitRate() != expectedHitRate {
		t.Errorf("Expected hit rate %.1f%%, got %.1f%%", expectedHitRate, stats.GetHitRate())
	}
}

// Test cache with different queries
func TestCacheWithDifferentQueries(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	queries := []string{
		"SELECT * FROM employees LIMIT 3;",
		"SELECT COUNT(*) FROM employees;",
		"SELECT name FROM employees WHERE department = 'HR';",
		"SELECT AVG(salary) FROM employees GROUP BY department;",
	}

	// Execute each query twice
	for _, query := range queries {
		// First execution - cache miss
		result1, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute query '%s': %v", query, err)
		}
		if result1.Error != "" {
			t.Fatalf("Query '%s' returned error: %s", query, result1.Error)
		}

		// Second execution - cache hit
		result2, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute cached query '%s': %v", query, err)
		}
		if result2.Error != "" {
			t.Fatalf("Cached query '%s' returned error: %s", query, result2.Error)
		}

		// Results should be identical
		if len(result1.Rows) != len(result2.Rows) {
			t.Errorf("Query '%s': cached result has different row count: %d vs %d", 
				query, len(result1.Rows), len(result2.Rows))
		}
	}

	// Check final cache stats
	stats := engine.GetCacheStats()
	expectedTotal := int64(len(queries) * 2) // Each query executed twice
	expectedHits := int64(len(queries))      // Second execution of each query should hit cache
	expectedMisses := int64(len(queries))    // First execution of each query should miss cache

	if stats.TotalQueries != expectedTotal {
		t.Errorf("Expected %d total queries, got %d", expectedTotal, stats.TotalQueries)
	}
	if stats.Hits != expectedHits {
		t.Errorf("Expected %d cache hits, got %d", expectedHits, stats.Hits)
	}
	if stats.Misses != expectedMisses {
		t.Errorf("Expected %d cache misses, got %d", expectedMisses, stats.Misses)
	}
}

// Test cache TTL expiration
func TestCacheTTLExpiration(t *testing.T) {
	generateSampleData()
	
	// Create engine with very short TTL
	cacheConfig := CacheConfig{
		MaxMemoryMB: 10,
		DefaultTTL:  50 * time.Millisecond, // Very short TTL for testing
		Enabled:     true,
	}
	engine := NewQueryEngineWithCache("./data", cacheConfig)
	defer engine.Close()

	query := "SELECT COUNT(*) FROM employees;"
	
	// First execution
	result1, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute first query: %v", err)
	}
	if result1.Error != "" {
		t.Fatalf("First query returned error: %s", result1.Error)
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Second execution after TTL expiration should miss cache
	result2, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute second query: %v", err)
	}
	if result2.Error != "" {
		t.Fatalf("Second query returned error: %s", result2.Error)
	}

	// Check cache stats - should have 2 misses, 0 hits
	stats := engine.GetCacheStats()
	if stats.TotalQueries != 2 {
		t.Errorf("Expected 2 total queries, got %d", stats.TotalQueries)
	}
	if stats.Misses != 2 {
		t.Errorf("Expected 2 cache misses due to TTL expiration, got %d", stats.Misses)
	}
	if stats.Hits != 0 {
		t.Errorf("Expected 0 cache hits due to TTL expiration, got %d", stats.Hits)
	}
}

// Test cache enable/disable functionality
func TestCacheEnableDisable(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	query := "SELECT name FROM employees LIMIT 2;"
	
	// Execute with cache enabled (default)
	result1, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute query with cache enabled: %v", err)
	}
	if result1.Error != "" {
		t.Fatalf("Query with cache enabled returned error: %s", result1.Error)
	}

	// Execute again - should hit cache
	_, err = engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute cached query: %v", err)
	}

	// Check that we got a cache hit
	stats := engine.GetCacheStats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 cache hit with cache enabled, got %d", stats.Hits)
	}

	// Disable cache
	engine.SetCacheEnabled(false)

	// Execute query again - should not use cache
	result3, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute query with cache disabled: %v", err)
	}
	if result3.Error != "" {
		t.Fatalf("Query with cache disabled returned error: %s", result3.Error)
	}

	// Cache stats should not have changed (no new hits)
	newStats := engine.GetCacheStats()
	if newStats.Hits != stats.Hits {
		t.Errorf("Cache hits should not increase when cache is disabled")
	}
}

// Test cache memory management
func TestCacheMemoryManagement(t *testing.T) {
	generateSampleData()
	
	// Create engine with very small cache to test eviction
	cacheConfig := CacheConfig{
		MaxMemoryMB: 1, // Very small cache
		DefaultTTL:  5 * time.Minute,
		Enabled:     true,
	}
	engine := NewQueryEngineWithCache("./data", cacheConfig)
	defer engine.Close()

	// Execute several different queries to fill up cache
	queries := []string{
		"SELECT * FROM employees;",
		"SELECT * FROM products;",
		"SELECT name, salary FROM employees ORDER BY salary DESC;",
		"SELECT COUNT(*) FROM employees GROUP BY department;",
		"SELECT price FROM products WHERE category = 'Electronics';",
	}

	for _, query := range queries {
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute query '%s': %v", query, err)
		}
		if result.Error != "" {
			t.Fatalf("Query '%s' returned error: %s", query, result.Error)
		}
	}

	// Check that evictions occurred due to memory pressure
	stats := engine.GetCacheStats()
	if stats.Evictions == 0 {
		t.Log("Note: No evictions occurred, cache might be larger than expected or queries smaller")
	}

	// Memory usage should be within limits
	maxMemoryBytes := float64(cacheConfig.MaxMemoryMB)
	if stats.GetMemoryUsageMB() > maxMemoryBytes*1.1 { // Allow 10% tolerance
		t.Errorf("Cache memory usage (%.2f MB) exceeds limit (%.2f MB)", 
			stats.GetMemoryUsageMB(), maxMemoryBytes)
	}
}

// Test cache clearing
func TestCacheClear(t *testing.T) {
	generateSampleData()
	
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Execute some queries to populate cache
	queries := []string{
		"SELECT COUNT(*) FROM employees;",
		"SELECT COUNT(*) FROM products;",
	}

	for _, query := range queries {
		_, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
	}

	// Verify cache has entries
	stats := engine.GetCacheStats()
	if stats.TotalQueries == 0 {
		t.Fatal("No queries executed before cache clear test")
	}

	// Clear cache
	engine.ClearCache()

	// Verify cache is empty
	newStats := engine.GetCacheStats()
	if newStats.CurrentSize != 0 {
		t.Errorf("Cache size should be 0 after clear, got %d", newStats.CurrentSize)
	}

	// Execute query again - should miss cache
	result, err := engine.Execute(queries[0])
	if err != nil {
		t.Fatalf("Failed to execute query after cache clear: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Query after cache clear returned error: %s", result.Error)
	}

	// Should be a cache miss
	finalStats := engine.GetCacheStats()
	if finalStats.Hits > newStats.Hits {
		t.Error("Should not get cache hit after cache clear")
	}
}