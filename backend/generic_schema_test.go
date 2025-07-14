package main

import (
	"os"
	"testing"

	"bytedb/core"
	"github.com/parquet-go/parquet-go"
)

// Test with a completely different schema to prove generic functionality
type Customer struct {
	CustomerID   int64   `parquet:"customer_id"`
	FirstName    string  `parquet:"first_name"`
	LastName     string  `parquet:"last_name"`
	Email        string  `parquet:"email"`
	Age          int32   `parquet:"age"`
	Revenue      float64 `parquet:"revenue"`
	IsActive     bool    `parquet:"is_active"`
	SignupDate   string  `parquet:"signup_date"`
	Region       string  `parquet:"region"`
	Subscription string  `parquet:"subscription"`
}

// Test with yet another different schema
type Order struct {
	OrderID     string  `parquet:"order_id"`
	CustomerRef int64   `parquet:"customer_ref"`
	Amount      float64 `parquet:"amount"`
	Currency    string  `parquet:"currency"`
	Status      string  `parquet:"status"`
	CreatedAt   string  `parquet:"created_at"`
	ShippedAt   string  `parquet:"shipped_at,optional"`
	Items       int32   `parquet:"items"`
}

// generateCustomerData creates a test file with a customer schema
func generateCustomerData() {
	customers := []Customer{
		{1001, "Alice", "Johnson", "alice@example.com", 29, 12500.50, true, "2023-01-15", "North", "Premium"},
		{1002, "Bob", "Smith", "bob@example.com", 34, 8750.25, true, "2023-02-22", "South", "Basic"},
		{1003, "Carol", "Davis", "carol@example.com", 28, 15200.75, false, "2023-03-10", "East", "Premium"},
		{1004, "David", "Wilson", "david@example.com", 41, 6500.00, true, "2023-04-05", "West", "Basic"},
		{1005, "Emma", "Brown", "emma@example.com", 25, 22300.40, true, "2023-05-12", "North", "Enterprise"},
	}

	file, err := os.Create("./data/customers.parquet")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Customer{}))
	defer writer.Close()

	for _, customer := range customers {
		if err := writer.Write(customer); err != nil {
			panic(err)
		}
	}
}

// generateOrderData creates a test file with an order schema
func generateOrderData() {
	orders := []Order{
		{"ORD-001", 1001, 299.99, "USD", "shipped", "2023-06-01", "2023-06-03", 3},
		{"ORD-002", 1002, 149.50, "USD", "pending", "2023-06-02", "", 1},
		{"ORD-003", 1003, 599.95, "EUR", "delivered", "2023-06-03", "2023-06-05", 2},
		{"ORD-004", 1004, 89.99, "USD", "cancelled", "2023-06-04", "", 1},
		{"ORD-005", 1005, 1299.99, "USD", "shipped", "2023-06-05", "2023-06-07", 5},
	}

	file, err := os.Create("./data/orders.parquet")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Order{}))
	defer writer.Close()

	for _, order := range orders {
		if err := writer.Write(order); err != nil {
			panic(err)
		}
	}
}

// Test generic schema detection with customer data
func TestGenericSchemaCustomers(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data
	generateCustomerData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test basic SELECT on customer data
	result, err := engine.Execute("SELECT first_name, last_name, revenue FROM customers WHERE age > 30;")
	if err != nil {
		t.Fatalf("Failed to execute customer query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Customer query returned error: %s", result.Error)
	}

	// Should return Bob (34) and David (41)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 customers over 30, got %d", len(result.Rows))
	}

	// Verify column structure
	expectedColumns := []string{"first_name", "last_name", "revenue"}
	for _, expectedCol := range expectedColumns {
		found := false
		for _, actualCol := range result.Columns {
			if actualCol == expectedCol {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in result", expectedCol)
		}
	}

	// Test aggregates on customer data
	result, err = engine.Execute("SELECT region, COUNT(*), AVG(revenue) FROM customers GROUP BY region ORDER BY region;")
	if err != nil {
		t.Fatalf("Failed to execute customer aggregate query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Customer aggregate query returned error: %s", result.Error)
	}

	// Should return 4 regions (East, North, South, West)
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 regions, got %d", len(result.Rows))
	}

	// Test schema introspection
	reader, err := core.NewParquetReader("./data/customers.parquet")
	if err != nil {
		t.Fatalf("Failed to create customer reader: %v", err)
	}
	defer reader.Close()

	schemaInfo := reader.GetSchemaInfo()
	if schemaInfo["field_count"].(int) != 10 {
		t.Errorf("Expected 10 fields in customer schema, got %d", schemaInfo["field_count"])
	}

	// Verify some field names exist
	fields := schemaInfo["fields"].([]map[string]interface{})
	fieldNames := make([]string, len(fields))
	for i, field := range fields {
		fieldNames[i] = field["name"].(string)
	}

	expectedFields := []string{"customer_id", "first_name", "last_name", "email", "revenue", "is_active"}
	for _, expectedField := range expectedFields {
		found := false
		for _, actualField := range fieldNames {
			if actualField == expectedField {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected field %s not found in customer schema", expectedField)
		}
	}
}

// Test generic schema detection with order data
func TestGenericSchemaOrders(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate test data
	generateOrderData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test basic SELECT on order data
	result, err := engine.Execute("SELECT order_id, amount, currency FROM orders WHERE status = 'shipped';")
	if err != nil {
		t.Fatalf("Failed to execute order query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Order query returned error: %s", result.Error)
	}

	// Should return 2 shipped orders (ORD-001, ORD-005)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 shipped orders, got %d", len(result.Rows))
	}

	// Test IN operator with string IDs
	result, err = engine.Execute("SELECT order_id, status FROM orders WHERE order_id IN ('ORD-001', 'ORD-003', 'ORD-999');")
	if err != nil {
		t.Fatalf("Failed to execute IN query on orders: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Order IN query returned error: %s", result.Error)
	}

	// Should return 2 matching orders (ORD-001, ORD-003)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 matching orders, got %d", len(result.Rows))
	}

	// Test aggregates with different numeric types
	result, err = engine.Execute("SELECT currency, COUNT(*), SUM(amount), AVG(items) FROM orders GROUP BY currency;")
	if err != nil {
		t.Fatalf("Failed to execute order aggregate query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Order aggregate query returned error: %s", result.Error)
	}

	// Should return 2 currencies (USD, EUR)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 currencies, got %d", len(result.Rows))
	}

	// Test ORDER BY with string fields
	result, err = engine.Execute("SELECT order_id, amount FROM orders ORDER BY order_id DESC LIMIT 3;")
	if err != nil {
		t.Fatalf("Failed to execute ORDER BY on orders: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Order ORDER BY query returned error: %s", result.Error)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 orders in ORDER BY result, got %d", len(result.Rows))
	}

	// Verify first row has the highest order_id (ORD-005)
	if len(result.Rows) > 0 {
		firstRow := result.Rows[0]
		if firstRow["order_id"] != "ORD-005" {
			t.Errorf("Expected first order to be ORD-005, got %v", firstRow["order_id"])
		}
	}
}

// Test mixed data types and edge cases
func TestGenericSchemaDataTypes(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	// Generate both test data files
	generateCustomerData()
	generateOrderData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test boolean field operations
	result, err := engine.Execute("SELECT first_name, is_active FROM customers WHERE is_active = true;")
	if err != nil {
		t.Fatalf("Failed to execute boolean query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Boolean query returned error: %s", result.Error)
	}

	// Should return 4 active customers (all except Carol)
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 active customers, got %d", len(result.Rows))
	}

	// Test different numeric types in comparison (test each condition separately since AND is not implemented yet)
	result, err = engine.Execute("SELECT customer_id, age FROM customers WHERE customer_id > 1002;")
	if err != nil {
		t.Fatalf("Failed to execute customer_id query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Customer_id query returned error: %s", result.Error)
	}

	// Should return Carol (1003), David (1004), and Emma (1005)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 customers with ID > 1002, got %d", len(result.Rows))
	}

	// Test age comparison with different numeric type (int32 vs int literal)
	result, err = engine.Execute("SELECT customer_id, age FROM customers WHERE age < 30;")
	if err != nil {
		t.Fatalf("Failed to execute age query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Age query returned error: %s", result.Error)
	}

	// Should return Alice (29), Carol (28), and Emma (25)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 customers under 30, got %d", len(result.Rows))
	}

	// Test LIKE operations on string fields
	result, err = engine.Execute("SELECT email FROM customers WHERE email LIKE '%@example.com';")
	if err != nil {
		t.Fatalf("Failed to execute LIKE query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("LIKE query returned error: %s", result.Error)
	}

	// All 5 customers should match
	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 customers with @example.com emails, got %d", len(result.Rows))
	}

	// Test column pruning with different field types
	result, err = engine.Execute("SELECT customer_id, is_active FROM customers LIMIT 2;")
	if err != nil {
		t.Fatalf("Failed to execute column pruning query: %v", err)
	}

	if result.Error != "" {
		t.Fatalf("Column pruning query returned error: %s", result.Error)
	}

	// Verify only requested columns are present
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		if len(row) != 2 {
			t.Errorf("Expected 2 columns in pruned result, got %d", len(row))
		}

		if _, exists := row["customer_id"]; !exists {
			t.Error("Missing customer_id column in pruned result")
		}

		if _, exists := row["is_active"]; !exists {
			t.Error("Missing is_active column in pruned result")
		}
	}
}

// Test performance with generic schema
func TestGenericSchemaPerformance(t *testing.T) {
	// Ensure data directory exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		t.Fatal(err)
	}

	generateCustomerData()

	engine := core.NewQueryEngine("./data")
	defer engine.Close()

	// Test cache effectiveness with generic schema
	query := "SELECT region, COUNT(*) FROM customers GROUP BY region;"

	// First execution (cache miss)
	result1, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute first query: %v", err)
	}

	if result1.Error != "" {
		t.Fatalf("First query returned error: %s", result1.Error)
	}

	// Second execution (cache hit)
	result2, err := engine.Execute(query)
	if err != nil {
		t.Fatalf("Failed to execute second query: %v", err)
	}

	if result2.Error != "" {
		t.Fatalf("Second query returned error: %s", result2.Error)
	}

	// Results should be identical
	if len(result1.Rows) != len(result2.Rows) {
		t.Errorf("Cached result differs from original: %d vs %d rows", len(result1.Rows), len(result2.Rows))
	}

	// Verify cache is working
	stats := engine.GetCacheStats()
	if stats.Hits < 1 {
		t.Errorf("Expected at least 1 cache hit, got %d", stats.Hits)
	}
}
