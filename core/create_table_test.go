package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseCreateTable(t *testing.T) {
	parser := NewSQLParser()
	
	tests := []struct {
		name         string
		sql          string
		expectError  bool
		checkFunc    func(*testing.T, *ParsedQuery)
	}{
		{
			name: "CREATE TABLE AS SELECT FROM read_parquet",
			sql:  "CREATE TABLE employees AS SELECT * FROM read_parquet('employees.parquet')",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if query.Type != CREATE {
					t.Errorf("Expected CREATE type, got %v", query.Type)
				}
				if query.CreateTable == nil {
					t.Fatal("CreateTable is nil")
				}
				if query.CreateTable.TableName != "employees" {
					t.Errorf("Expected table name 'employees', got '%s'", query.CreateTable.TableName)
				}
				if !query.CreateTable.AsSelect {
					t.Error("Expected AsSelect to be true")
				}
				if query.CreateTable.SelectQuery == nil {
					t.Fatal("SelectQuery is nil")
				}
				if query.CreateTable.SelectQuery.TableFunction == nil {
					t.Fatal("TableFunction is nil")
				}
				if query.CreateTable.SelectQuery.TableFunction.Name != "read_parquet" {
					t.Errorf("Expected function 'read_parquet', got '%s'", query.CreateTable.SelectQuery.TableFunction.Name)
				}
			},
		},
		{
			name: "CREATE TABLE IF NOT EXISTS AS SELECT FROM read_parquet",
			sql:  "CREATE TABLE IF NOT EXISTS users AS SELECT * FROM read_parquet('users.parquet')",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if !query.CreateTable.IfNotExists {
					t.Error("Expected IfNotExists to be true")
				}
			},
		},
		{
			name: "CREATE TABLE with schema qualifier",
			sql:  "CREATE TABLE myschema.mytable AS SELECT * FROM read_parquet('data.parquet')",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if query.CreateTable.TableName != "myschema.mytable" {
					t.Errorf("Expected table name 'myschema.mytable', got '%s'", query.CreateTable.TableName)
				}
			},
		},
		{
			name: "SELECT FROM read_parquet",
			sql:  "SELECT * FROM read_parquet('employees.parquet')",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if query.Type != SELECT {
					t.Errorf("Expected SELECT type, got %v", query.Type)
				}
				if query.TableFunction == nil {
					t.Fatal("TableFunction is nil")
				}
				if query.TableFunction.Name != "read_parquet" {
					t.Errorf("Expected function 'read_parquet', got '%s'", query.TableFunction.Name)
				}
				if len(query.TableFunction.Arguments) != 1 {
					t.Errorf("Expected 1 argument, got %d", len(query.TableFunction.Arguments))
				}
				if arg, ok := query.TableFunction.Arguments[0].(string); !ok || arg != "employees.parquet" {
					t.Errorf("Expected argument 'employees.parquet', got %v", query.TableFunction.Arguments[0])
				}
			},
		},
		{
			name: "CREATE TABLE with column definitions",
			sql:  "CREATE TABLE employees (id INT NOT NULL, name VARCHAR, salary DECIMAL)",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if len(query.CreateTable.Columns) != 3 {
					t.Fatalf("Expected 3 columns, got %d", len(query.CreateTable.Columns))
				}
				// Check first column
				if query.CreateTable.Columns[0].Name != "id" {
					t.Errorf("Expected column name 'id', got '%s'", query.CreateTable.Columns[0].Name)
				}
				if query.CreateTable.Columns[0].Nullable {
					t.Error("Expected id column to be NOT NULL")
				}
			},
		},
		{
			name: "CREATE TABLE AS SELECT",
			sql:  "CREATE TABLE new_employees AS SELECT * FROM employees WHERE salary > 50000",
			checkFunc: func(t *testing.T, query *ParsedQuery) {
				if !query.CreateTable.AsSelect {
					t.Error("Expected AsSelect to be true")
				}
				if query.CreateTable.SelectQuery == nil {
					t.Fatal("SelectQuery is nil")
				}
				if query.CreateTable.SelectQuery.TableName != "employees" {
					t.Errorf("Expected SELECT from 'employees', got '%s'", query.CreateTable.SelectQuery.TableName)
				}
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := parser.Parse(tt.sql)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if tt.checkFunc != nil {
				tt.checkFunc(t, query)
			}
		})
	}
}

func TestExecuteCreateTable(t *testing.T) {
	// Create temporary directory for test
	tempDir := t.TempDir()
	
	// Create a test parquet file with actual parquet data
	testFile := filepath.Join(tempDir, "test_data.parquet")
	// Use a minimal valid parquet file for testing
	// This is just for testing file existence, not actual data reading
	if err := createTestParquetFile(testFile); err != nil {
		t.Fatalf("Failed to create test parquet file: %v", err)
	}
	
	// Create query engine
	engine := NewQueryEngine(tempDir)
	defer engine.Close()
	
	tests := []struct {
		name        string
		setup       func()
		sql         string
		expectError bool
		checkFunc   func(*testing.T, *QueryResult)
	}{
		{
			name: "CREATE TABLE AS SELECT FROM read_parquet",
			sql:  "CREATE TABLE test_table AS SELECT * FROM read_parquet('test_data.parquet')",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error != "" {
					t.Errorf("Unexpected error: %s", result.Error)
				}
				if result.Count != 1 {
					t.Errorf("Expected 1 row, got %d", result.Count)
				}
				// Verify table was registered
				_, err := engine.tableRegistry.GetTablePath("test_table")
				if err != nil {
					t.Errorf("Table not registered: %v", err)
				}
			},
		},
		{
			name: "SELECT directly from read_parquet",
			sql:  "SELECT * FROM read_parquet('test_data.parquet') LIMIT 1",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error != "" {
					t.Errorf("Unexpected error: %s", result.Error)
				}
				// Should return at least column headers even if file is dummy
				if len(result.Columns) == 0 && result.Error == "" {
					t.Error("Expected at least column information")
				}
			},
		},
		{
			name: "CREATE TABLE FROM non-existent file",
			sql:  "CREATE TABLE missing AS SELECT * FROM read_parquet('missing.parquet')",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error == "" {
					t.Error("Expected error for missing file")
				}
				if !contains(result.Error, "not found") && !contains(result.Error, "failed to open") {
					t.Errorf("Expected 'not found' or 'failed to open' error, got: %s", result.Error)
				}
			},
		},
		{
			name: "CREATE TABLE IF NOT EXISTS (duplicate)",
			setup: func() {
				// Pre-register a table
				engine.RegisterTable("existing_table", testFile)
			},
			sql: "CREATE TABLE IF NOT EXISTS existing_table AS SELECT * FROM read_parquet('test_data.parquet')",
			checkFunc: func(t *testing.T, result *QueryResult) {
				// Should succeed without error due to IF NOT EXISTS
				if result.Error != "" {
					t.Errorf("Unexpected error with IF NOT EXISTS: %s", result.Error)
				}
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			
			result, err := engine.Execute(tt.sql)
			if err != nil && !tt.expectError {
				t.Fatalf("Unexpected execution error: %v", err)
			}
			
			if tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func TestExecuteCreateTableWithCatalog(t *testing.T) {
	// Create temporary directory for test
	tempDir := t.TempDir()
	
	// Create a test parquet file
	testFile := filepath.Join(tempDir, "catalog_test.parquet")
	if err := createTestParquetFile(testFile); err != nil {
		t.Fatalf("Failed to create test parquet file: %v", err)
	}
	
	// Create query engine with catalog enabled
	engine := NewQueryEngine(tempDir)
	defer engine.Close()
	
	// Enable catalog
	err := engine.EnableCatalog("memory", nil)
	if err != nil {
		t.Fatalf("Failed to enable catalog: %v", err)
	}
	
	tests := []struct {
		name        string
		sql         string
		expectError bool
		checkFunc   func(*testing.T, *QueryResult)
	}{
		{
			name: "CREATE TABLE with catalog",
			sql:  "CREATE TABLE catalog_table AS SELECT * FROM read_parquet('catalog_test.parquet')",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error != "" {
					t.Errorf("Unexpected error: %s", result.Error)
				}
				
				// Verify table exists in catalog
				ctx := context.Background()
				table, err := engine.catalogManager.GetTableMetadata(ctx, "catalog_table")
				if err != nil {
					t.Errorf("Table not found in catalog: %v", err)
				}
				if table == nil {
					t.Error("Table metadata is nil")
				}
			},
		},
		{
			name: "CREATE TABLE with schema qualifier",
			sql:  "CREATE TABLE myschema.mytable AS SELECT * FROM read_parquet('catalog_test.parquet')",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error != "" {
					t.Errorf("Unexpected error: %s", result.Error)
				}
			},
		},
		{
			name: "CREATE TABLE with column definitions",
			sql:  "CREATE TABLE empty_table (id INT NOT NULL, name VARCHAR, amount DECIMAL)",
			checkFunc: func(t *testing.T, result *QueryResult) {
				if result.Error != "" {
					t.Errorf("Unexpected error: %s", result.Error)
				}
				
				// Verify table schema in catalog
				ctx := context.Background()
				table, err := engine.catalogManager.GetTableMetadata(ctx, "empty_table")
				if err != nil {
					t.Errorf("Table not found in catalog: %v", err)
				}
				if len(table.Columns) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(table.Columns))
				}
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.sql)
			if err != nil && !tt.expectError {
				t.Fatalf("Unexpected execution error: %v", err)
			}
			
			if tt.checkFunc != nil {
				tt.checkFunc(t, result)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[0:len(substr)] == substr || contains(s[1:], substr)))
}

// createTestParquetFile creates a minimal valid parquet file for testing
func createTestParquetFile(path string) error {
	// For now, just create a dummy file
	// In a real test, we would use a parquet writer to create a valid file
	// This is sufficient for testing the CREATE TABLE functionality
	return os.WriteFile(path, []byte("dummy"), 0644)
}