package main

import (
	"encoding/json"
	"testing"
)

func TestSQLParser(t *testing.T) {
	parser := NewSQLParser()

	tests := []struct {
		name     string
		sql      string
		expected ParsedQuery
		hasError bool
	}{
		{
			name: "SELECT * with LIMIT",
			sql:  "SELECT * FROM employees LIMIT 3;",
			expected: ParsedQuery{
				Type:      SELECT,
				TableName: "employees",
				Columns:   []Column{{Name: "*", Alias: ""}},
				Where:     []WhereCondition{},
				Limit:     3,
			},
		},
		{
			name: "SELECT specific columns",
			sql:  "SELECT name, salary FROM employees;",
			expected: ParsedQuery{
				Type:      SELECT,
				TableName: "employees",
				Columns:   []Column{{Name: "name", Alias: ""}, {Name: "salary", Alias: ""}},
				Where:     []WhereCondition{},
				Limit:     0,
			},
		},
		{
			name: "SELECT with WHERE clause - string",
			sql:  "SELECT * FROM employees WHERE department = 'Engineering';",
			expected: ParsedQuery{
				Type:      SELECT,
				TableName: "employees",
				Columns:   []Column{{Name: "*", Alias: ""}},
				Where: []WhereCondition{
					{Column: "department", Operator: "=", Value: "Engineering"},
				},
				Limit: 0,
			},
		},
		{
			name: "SELECT with WHERE clause - numeric",
			sql:  "SELECT name, price FROM products WHERE price > 100;",
			expected: ParsedQuery{
				Type:      SELECT,
				TableName: "products",
				Columns:   []Column{{Name: "name", Alias: ""}, {Name: "price", Alias: ""}},
				Where: []WhereCondition{
					{Column: "price", Operator: ">", Value: int64(100)},
				},
				Limit: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.Parse(tt.sql)
			
			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.Type != tt.expected.Type {
				t.Errorf("Type mismatch: got %v, want %v", result.Type, tt.expected.Type)
			}
			
			if result.TableName != tt.expected.TableName {
				t.Errorf("TableName mismatch: got %s, want %s", result.TableName, tt.expected.TableName)
			}
			
			if len(result.Columns) != len(tt.expected.Columns) {
				t.Errorf("Columns length mismatch: got %d, want %d", len(result.Columns), len(tt.expected.Columns))
			}
			
			if result.Limit != tt.expected.Limit {
				t.Errorf("Limit mismatch: got %d, want %d", result.Limit, tt.expected.Limit)
			}
		})
	}
}

func TestParquetReader(t *testing.T) {
	// Setup test data
	generateSampleData()
	
	tests := []struct {
		name      string
		file      string
		limit     int
		expectMin int
		expectMax int
	}{
		{
			name:      "Read employees with limit",
			file:      "./data/employees.parquet",
			limit:     3,
			expectMin: 3,
			expectMax: 3,
		},
		{
			name:      "Read all employees",
			file:      "./data/employees.parquet",
			limit:     0,
			expectMin: 10,
			expectMax: 10,
		},
		{
			name:      "Read products with limit",
			file:      "./data/products.parquet",
			limit:     5,
			expectMin: 5,
			expectMax: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewParquetReader(tt.file)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer reader.Close()

			var rows []Row
			if tt.limit > 0 {
				rows, err = reader.ReadWithLimit(tt.limit)
			} else {
				rows, err = reader.ReadAll()
			}

			if err != nil {
				t.Errorf("Failed to read rows: %v", err)
				return
			}

			if len(rows) < tt.expectMin || len(rows) > tt.expectMax {
				t.Errorf("Row count out of range: got %d, want %d-%d", len(rows), tt.expectMin, tt.expectMax)
			}

			// Check that we have data in rows
			if len(rows) > 0 && len(rows[0]) == 0 {
				t.Errorf("First row is empty")
			}
		})
	}
}

func TestWhereClauseFiltering(t *testing.T) {
	generateSampleData()
	
	tests := []struct {
		name         string
		file         string
		conditions   []WhereCondition
		expectedRows int
		checkFunc    func([]Row) bool
	}{
		{
			name: "Filter employees by department",
			file: "./data/employees.parquet",
			conditions: []WhereCondition{
				{Column: "department", Operator: "=", Value: "Engineering"},
			},
			expectedRows: 4,
			checkFunc: func(rows []Row) bool {
				for _, row := range rows {
					if row["department"] != "Engineering" {
						return false
					}
				}
				return true
			},
		},
		{
			name: "Filter employees by salary greater than",
			file: "./data/employees.parquet",
			conditions: []WhereCondition{
				{Column: "salary", Operator: ">", Value: 75000},
			},
			expectedRows: 3, // Mike Johnson (80000), Lisa Davis (85000), Chris Anderson (78000)
			checkFunc: func(rows []Row) bool {
				for _, row := range rows {
					salary, ok := row["salary"].(float64)
					if !ok || salary <= 75000 {
						return false
					}
				}
				return true
			},
		},
		{
			name: "Filter products by price greater than",
			file: "./data/products.parquet",
			conditions: []WhereCondition{
				{Column: "price", Operator: ">", Value: 100},
			},
			expectedRows: 3, // Laptop (999.99), Monitor (299.99), Desk Chair (199.99)
			checkFunc: func(rows []Row) bool {
				for _, row := range rows {
					price, ok := row["price"].(float64)
					if !ok || price <= 100 {
						return false
					}
				}
				return true
			},
		},
		{
			name: "Filter products by price greater than or equal",
			file: "./data/products.parquet",
			conditions: []WhereCondition{
				{Column: "price", Operator: ">=", Value: 199.99},
			},
			expectedRows: 3, // Laptop, Monitor, Desk Chair
			checkFunc: func(rows []Row) bool {
				for _, row := range rows {
					price, ok := row["price"].(float64)
					if !ok || price < 199.99 {
						return false
					}
				}
				return true
			},
		},
		{
			name: "Filter products by price less than",
			file: "./data/products.parquet",
			conditions: []WhereCondition{
				{Column: "price", Operator: "<", Value: 30},
			},
			expectedRows: 5, // Mouse (29.99), Notebook (12.99), Pen Set (24.99), Coffee Mug (15.99), Water Bottle (19.99)
			checkFunc: func(rows []Row) bool {
				for _, row := range rows {
					price, ok := row["price"].(float64)
					if !ok || price >= 30 {
						return false
					}
				}
				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewParquetReader(tt.file)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}
			defer reader.Close()

			allRows, err := reader.ReadAll()
			if err != nil {
				t.Fatalf("Failed to read all rows: %v", err)
			}

			filteredRows := reader.FilterRows(allRows, tt.conditions)

			if len(filteredRows) != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, len(filteredRows))
				
				// Debug info
				t.Logf("All rows count: %d", len(allRows))
				t.Logf("Conditions: %+v", tt.conditions)
				for i, row := range filteredRows {
					t.Logf("Filtered row %d: %+v", i, row)
				}
			}

			if tt.checkFunc != nil && !tt.checkFunc(filteredRows) {
				t.Errorf("Filtered rows don't meet the expected criteria")
			}
		})
	}
}

func TestQueryEngine(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	defer engine.Close()

	tests := []struct {
		name         string
		sql          string
		expectedRows int
		hasError     bool
		checkFunc    func(*QueryResult) bool
	}{
		{
			name:         "SELECT * with limit",
			sql:          "SELECT * FROM employees LIMIT 3;",
			expectedRows: 3,
			hasError:     false,
			checkFunc: func(result *QueryResult) bool {
				return len(result.Columns) > 0 && result.Count == 3
			},
		},
		{
			name:         "SELECT specific columns",
			sql:          "SELECT name, salary FROM employees;",
			expectedRows: 10,
			hasError:     false,
			checkFunc: func(result *QueryResult) bool {
				// Check that we only get the requested columns
				for _, row := range result.Rows {
					if len(row) > 2 {
						return false
					}
					if _, hasName := row["name"]; !hasName {
						return false
					}
					if _, hasSalary := row["salary"]; !hasSalary {
						return false
					}
				}
				return true
			},
		},
		{
			name:         "WHERE clause with string comparison",
			sql:          "SELECT name FROM employees WHERE department = 'Engineering';",
			expectedRows: 4,
			hasError:     false,
			checkFunc: func(result *QueryResult) bool {
				for _, row := range result.Rows {
					if len(row) != 1 {
						return false
					}
					if _, hasName := row["name"]; !hasName {
						return false
					}
				}
				return true
			},
		},
		{
			name:         "WHERE clause with numeric comparison",
			sql:          "SELECT name, price FROM products WHERE price > 100;",
			expectedRows: 3,
			hasError:     false,
			checkFunc: func(result *QueryResult) bool {
				expectedNames := map[string]bool{
					"Laptop":     true,
					"Monitor":    true,
					"Desk Chair": true,
				}
				
				if len(result.Rows) != 3 {
					return false
				}
				
				for _, row := range result.Rows {
					name, hasName := row["name"]
					price, hasPrice := row["price"]
					
					if !hasName || !hasPrice {
						return false
					}
					
					nameStr, ok := name.(string)
					if !ok || !expectedNames[nameStr] {
						return false
					}
					
					priceFloat, ok := price.(float64)
					if !ok || priceFloat <= 100 {
						return false
					}
				}
				return true
			},
		},
		{
			name:         "Invalid table name",
			sql:          "SELECT * FROM nonexistent;",
			expectedRows: 0,
			hasError:     false, // Returns result with error field
			checkFunc: func(result *QueryResult) bool {
				return result.Error != "" && result.Count == 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Execute(tt.sql)
			
			if err != nil {
				t.Errorf("Unexpected error from Execute: %v", err)
				return
			}

			if tt.hasError {
				if result.Error == "" {
					t.Errorf("Expected error in result but got none")
				}
				return
			}

			if result.Error != "" && !tt.hasError {
				// For invalid table test, this is expected
				if tt.checkFunc == nil || !tt.checkFunc(result) {
					t.Errorf("Unexpected error in result: %s", result.Error)
					return
				}
			}

			if !tt.hasError && result.Count != tt.expectedRows {
				t.Errorf("Expected %d rows, got %d", tt.expectedRows, result.Count)
			}

			if tt.checkFunc != nil && !tt.checkFunc(result) {
				t.Errorf("Result doesn't meet the expected criteria")
				t.Logf("Result: %+v", result)
			}
		})
	}
}

func TestJSONOutput(t *testing.T) {
	generateSampleData()
	engine := NewQueryEngine("./data")
	defer engine.Close()

	sql := "SELECT name, price FROM products WHERE price > 500;"
	jsonStr, err := engine.ExecuteToJSON(sql)
	if err != nil {
		t.Fatalf("Failed to execute to JSON: %v", err)
	}

	var result QueryResult
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if result.Count != 1 {
		t.Errorf("Expected 1 row, got %d", result.Count)
		t.Logf("Rows: %+v", result.Rows)
	}

	expectedColumns := []string{"name", "price"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	// Verify the data matches our expectations
	for _, row := range result.Rows {
		name, hasName := row["name"]
		price, hasPrice := row["price"]
		
		if !hasName || !hasPrice {
			t.Errorf("Missing required columns in row: %+v", row)
		}
		
		if priceFloat, ok := price.(float64); !ok || priceFloat <= 500 {
			t.Errorf("Price should be > 500, got %v", price)
		}
		
		if nameStr, ok := name.(string); !ok || nameStr == "" {
			t.Errorf("Name should be non-empty string, got %v", name)
		}
	}
}

func TestNumericTypeComparison(t *testing.T) {
	reader := &ParquetReader{}
	
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"int vs float64 - equal", 100, 100.0, 0},
		{"int vs float64 - less", 50, 100.0, -1},
		{"int vs float64 - greater", 150, 100.0, 1},
		{"float64 vs int - equal", 100.0, 100, 0},
		{"float64 vs int - less", 50.5, 100, -1},
		{"float64 vs int - greater", 150.5, 100, 1},
		{"int32 vs float64", int32(100), 100.0, 0},
		{"float32 vs int", float32(100.0), 100, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reader.CompareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("CompareValues(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}