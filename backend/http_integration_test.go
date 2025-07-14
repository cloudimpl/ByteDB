package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"bytedb/core"
)

func TestHTTPIntegration(t *testing.T) {
	// First, ensure we have test data
	engine := NewTestQueryEngine()
	engine.Close() // We just want to generate the data

	// Start a local HTTP server that serves our Parquet files
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Map URL paths to local files
		fileMap := map[string]string{
			"/employees.parquet":   "./data/employees.parquet",
			"/products.parquet":    "./data/products.parquet",
			"/departments.parquet": "./data/departments.parquet",
		}

		filePath, exists := fileMap[r.URL.Path]
		if !exists {
			http.NotFound(w, r)
			return
		}

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}

		// Open the file
		file, err := os.Open(filePath)
		if err != nil {
			http.Error(w, "Failed to open file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Get file info
		stat, err := file.Stat()
		if err != nil {
			http.Error(w, "Failed to stat file", http.StatusInternalServerError)
			return
		}

		// Set headers for range request support
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))

		// Log the request for debugging
		t.Logf("HTTP Request: %s %s, Range: %s", r.Method, r.URL.Path, r.Header.Get("Range"))

		// Serve the file with range support
		http.ServeContent(w, r, filePath, stat.ModTime(), file)
	}))
	defer server.Close()

	t.Logf("Test HTTP server started at: %s", server.URL)

	// Test 1: Basic HTTP table registration and querying
	t.Run("Basic HTTP Query", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register HTTP URL for employees table
		httpURL := server.URL + "/employees.parquet"
		engine.RegisterHTTPTable("employees", httpURL)

		// Execute a simple query
		query := "SELECT name, department, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC LIMIT 5"
		result, err := engine.Execute(query)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Query returned error: %s", result.Error)
		}

		t.Logf("Query executed successfully!")
		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row count: %d", result.Count)

		// Verify results
		if result.Count == 0 {
			t.Error("Expected at least some results")
		}

		// Print first few results
		for i, row := range result.Rows {
			if i >= 3 { // Only print first 3 rows
				break
			}
			t.Logf("Row %d: name=%s, department=%s, salary=%v",
				i+1, row["name"], row["department"], row["salary"])
		}

		// Verify all results are from Engineering
		for _, row := range result.Rows {
			if row["department"] != "Engineering" {
				t.Errorf("Expected Engineering department, got %v", row["department"])
			}
		}
	})

	// Test 2: Multiple HTTP tables
	t.Run("Multiple HTTP Tables", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register multiple HTTP tables
		engine.RegisterHTTPTable("employees", server.URL+"/employees.parquet")
		engine.RegisterHTTPTable("departments", server.URL+"/departments.parquet")

		// Query with JOIN across HTTP tables
		query := `
			SELECT e.name, e.department, d.manager, d.budget 
			FROM employees e 
			JOIN departments d ON e.department = d.name 
			WHERE e.salary > 70000 
			ORDER BY e.salary DESC 
			LIMIT 5
		`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("JOIN query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("JOIN query returned error: %s", result.Error)
		}

		t.Logf("JOIN query executed successfully!")
		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row count: %d", result.Count)

		// Verify we got results
		if result.Count == 0 {
			t.Error("Expected at least some results from JOIN")
		}

		// Print results
		for i, row := range result.Rows {
			if i >= 2 { // Only print first 2 rows
				break
			}
			t.Logf("Row %d: name=%s, department=%s, manager=%s, budget=%v",
				i+1, row["name"], row["department"], row["manager"], row["budget"])
		}
	})

	// Test 3: Aggregation on HTTP tables
	t.Run("Aggregation on HTTP Tables", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register HTTP table
		engine.RegisterHTTPTable("employees", server.URL+"/employees.parquet")

		// Aggregation query
		query := `
			SELECT department, 
				   COUNT(*) as employee_count, 
				   AVG(salary) as avg_salary,
				   MAX(salary) as max_salary
			FROM employees 
			GROUP BY department 
			ORDER BY avg_salary DESC
		`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Aggregation query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Aggregation query returned error: %s", result.Error)
		}

		t.Logf("Aggregation query executed successfully!")
		t.Logf("Columns: %v", result.Columns)
		t.Logf("Row count: %d", result.Count)

		// Print results
		for _, row := range result.Rows {
			t.Logf("Department: %s, Count: %v, Avg Salary: %v, Max Salary: %v",
				row["department"], row["employee_count"], row["avg_salary"], row["max_salary"])
		}

		// Verify we have multiple departments
		if result.Count < 2 {
			t.Error("Expected multiple departments in aggregation results")
		}
	})

	// Test 4: Functions with HTTP tables
	t.Run("Functions with HTTP Tables", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register HTTP table
		engine.RegisterHTTPTable("employees", server.URL+"/employees.parquet")

		// Query with string functions
		query := `
			SELECT UPPER(name) as upper_name, 
				   LENGTH(name) as name_length, 
				   CONCAT(name, ' (', department, ')') as full_info
			FROM employees 
			WHERE LENGTH(name) > 8 
			ORDER BY name_length DESC 
			LIMIT 3
		`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Functions query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Functions query returned error: %s", result.Error)
		}

		t.Logf("Functions query executed successfully!")
		t.Logf("Row count: %d", result.Count)

		// Print results
		for i, row := range result.Rows {
			t.Logf("Row %d: upper_name=%s, name_length=%v, full_info=%s",
				i+1, row["upper_name"], row["name_length"], row["full_info"])
		}

		// Verify all names are longer than 8 characters
		for _, row := range result.Rows {
			nameLength := row["name_length"].(int)
			if nameLength <= 8 {
				t.Errorf("Expected name length > 8, got %d", nameLength)
			}
		}
	})

	// Test 5: Performance test with larger query
	t.Run("Performance Test", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register HTTP table
		engine.RegisterHTTPTable("employees", server.URL+"/employees.parquet")

		start := time.Now()

		// Complex query that should benefit from range requests
		query := `
			SELECT department, name, salary,
				   RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank
			FROM employees 
			WHERE salary > 60000
			ORDER BY department, salary_rank
		`

		result, err := engine.Execute(query)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Performance query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Performance query returned error: %s", result.Error)
		}

		t.Logf("Performance query completed in %v", duration)
		t.Logf("Row count: %d", result.Count)

		// Verify we got results in reasonable time
		if duration > 5*time.Second {
			t.Errorf("Query took too long: %v", duration)
		}

		if result.Count == 0 {
			t.Error("Expected some results from performance query")
		}
	})

	// Test 6: Mixed local and HTTP tables
	t.Run("Mixed Local and HTTP Tables", func(t *testing.T) {
		engine := core.NewQueryEngine("./data")
		defer engine.Close()

		// Register only employees as HTTP, keep products as local
		engine.RegisterHTTPTable("employees", server.URL+"/employees.parquet")
		// products table will use local file automatically

		// Query that uses both local and HTTP tables
		query := `
			SELECT 'employees' as source, COUNT(*) as count FROM employees
			UNION ALL
			SELECT 'products' as source, COUNT(*) as count FROM products
		`

		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Mixed tables query failed: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Mixed tables query returned error: %s", result.Error)
		}

		t.Logf("Mixed tables query executed successfully!")
		t.Logf("Row count: %d", result.Count)

		// Print results
		for _, row := range result.Rows {
			t.Logf("Source: %s, Count: %v", row["source"], row["count"])
		}

		// Should have exactly 2 rows
		if result.Count != 2 {
			t.Errorf("Expected 2 rows (employees + products), got %d", result.Count)
		}
	})
}

func TestHTTPTableManagement(t *testing.T) {
	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	t.Run("Register and Unregister HTTP Table", func(t *testing.T) {
		// Register a table
		engine.RegisterHTTPTable("test_table", "http://nonexistent.example.com/test.parquet")

		// Try to query it (should fail due to non-existent HTTP URL)
		result1, err1 := engine.Execute("SELECT * FROM test_table LIMIT 1")

		// Check if we got an error either in err or result.Error
		hasError1 := err1 != nil || (result1 != nil && result1.Error != "")
		if !hasError1 {
			t.Error("Expected error for non-existent HTTP URL")
		}

		if err1 != nil {
			t.Logf("HTTP error in err: %v", err1)
		}
		if result1 != nil && result1.Error != "" {
			t.Logf("HTTP error in result: %s", result1.Error)
		}

		// Unregister the table
		engine.UnregisterHTTPTable("test_table")

		// Now it should try local file and give different error
		result2, err2 := engine.Execute("SELECT * FROM test_table LIMIT 1")

		// Check if we got an error either in err or result.Error
		hasError2 := err2 != nil || (result2 != nil && result2.Error != "")
		if !hasError2 {
			t.Error("Expected error for non-existent local file")
		}

		if err2 != nil {
			t.Logf("Local file error in err: %v", err2)
		}
		if result2 != nil && result2.Error != "" {
			t.Logf("Local file error in result: %s", result2.Error)
		}

		// Both should have errors
		if !hasError1 || !hasError2 {
			t.Error("Both HTTP and local access should produce errors")
		}
	})
}
