package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestHTTPParquetReader(t *testing.T) {
	// Create a test server that serves a local parquet file
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if it's a range request
		rangeHeader := r.Header.Get("Range")
		
		// Read the local test file
		localFile := "./data/employees.parquet"
		file, err := os.Open(localFile)
		if err != nil {
			t.Errorf("Failed to open test file: %v", err)
			http.Error(w, "Test file not found", http.StatusNotFound)
			return
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			http.Error(w, "Failed to stat file", http.StatusInternalServerError)
			return
		}

		// Set content length header
		w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")

		if rangeHeader != "" {
			// Handle range request
			http.ServeContent(w, r, "employees.parquet", stat.ModTime(), file)
		} else {
			// Serve full file
			http.ServeContent(w, r, "employees.parquet", stat.ModTime(), file)
		}
	}))
	defer testServer.Close()

	t.Run("HTTP URL detection", func(t *testing.T) {
		if !isHTTPURL("http://example.com/file.parquet") {
			t.Error("Expected http:// URL to be detected as HTTP")
		}
		if !isHTTPURL("https://example.com/file.parquet") {
			t.Error("Expected https:// URL to be detected as HTTP")
		}
		if isHTTPURL("/local/path/file.parquet") {
			t.Error("Expected local path to not be detected as HTTP")
		}
		if isHTTPURL("file.parquet") {
			t.Error("Expected relative path to not be detected as HTTP")
		}
	})

	t.Run("HTTP Parquet file reading", func(t *testing.T) {
		// Make sure we have test data
		engine := NewTestQueryEngine()
		defer engine.Close()

		// Test HTTP URL
		httpURL := testServer.URL + "/employees.parquet"
		
		// Create HTTP parquet reader
		reader, err := NewParquetReader(httpURL)
		if err != nil {
			t.Fatalf("Failed to create HTTP parquet reader: %v", err)
		}
		defer reader.Close()

		// Verify schema
		schema := reader.GetSchema()
		if schema == nil {
			t.Error("Expected schema to be available")
		}

		// Verify column names
		columns := reader.GetColumnNames()
		expectedColumns := []string{"id", "name", "department", "salary", "age", "hire_date"}
		if len(columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}

		// Verify we can get row count
		rowCount := reader.GetRowCount()
		if rowCount <= 0 {
			t.Errorf("Expected positive row count, got %d", rowCount)
		}
	})

	t.Run("HTTP Parquet query execution", func(t *testing.T) {
		// Create a fresh query engine
		engine := NewQueryEngine("./data")
		defer engine.Close()

		// Register the HTTP URL for the employees table
		httpURL := testServer.URL + "/employees.parquet"
		engine.RegisterHTTPTable("employees", httpURL)

		// Now run a SQL query that will use the HTTP Parquet file
		query := "SELECT name, department FROM employees WHERE department = 'Engineering' LIMIT 3"
		result, err := engine.Execute(query)
		if err != nil {
			t.Fatalf("Failed to execute query on HTTP Parquet file: %v", err)
		}

		if result.Error != "" {
			t.Fatalf("Query returned error: %s", result.Error)
		}

		// Verify we got results
		if result.Count == 0 {
			t.Error("Expected results from HTTP Parquet query")
		}

		t.Logf("Successfully executed query on HTTP Parquet file, got %d rows", result.Count)
		
		// Verify column names
		expectedColumns := []string{"name", "department"}
		if len(result.Columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
		}

		// Verify all results are from Engineering department
		for _, row := range result.Rows {
			if row["department"] != "Engineering" {
				t.Errorf("Expected all rows to be from Engineering, got %v", row["department"])
			}
		}
	})
}

func TestHTTPTableRegistration(t *testing.T) {
	engine := NewQueryEngine("./data")
	defer engine.Close()

	t.Run("Register and query HTTP table", func(t *testing.T) {
		// Register a mock HTTP table
		engine.RegisterHTTPTable("test_table", "http://example.com/test.parquet")
		
		// The actual query will fail since the URL doesn't exist, but we can test registration
		// by checking that the getReader method attempts to use the HTTP URL
	})

	t.Run("Unregister HTTP table", func(t *testing.T) {
		// Register then unregister
		engine.RegisterHTTPTable("temp_table", "http://example.com/temp.parquet")
		engine.UnregisterHTTPTable("temp_table")
		
		// After unregistration, it should fall back to local file path
	})
}

func TestHTTPParquetErrors(t *testing.T) {
	t.Run("Invalid HTTP URL", func(t *testing.T) {
		_, err := NewParquetReader("http://nonexistent.example.com/file.parquet")
		if err == nil {
			t.Error("Expected error for non-existent HTTP URL")
		}
	})

	t.Run("Non-parquet HTTP content", func(t *testing.T) {
		// Create a test server that serves non-parquet content
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("This is not a parquet file"))
		}))
		defer testServer.Close()

		_, err := NewParquetReader(testServer.URL + "/notparquet.txt")
		if err == nil {
			t.Error("Expected error when trying to read non-parquet content")
		}
	})
}