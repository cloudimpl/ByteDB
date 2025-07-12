package main

import (
	"bytedb/core"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	fmt.Println("🗂️  ByteDB Table Registry Demo")
	fmt.Println("==============================")

	// Create a query engine
	dataPath := "./data"
	engine := core.NewQueryEngine(dataPath)
	defer engine.Close()

	// Demo 1: Register tables programmatically
	fmt.Println("\n1️⃣  Registering Tables Programmatically")
	fmt.Println("---------------------------------------")

	// Register a simple table mapping
	err := engine.RegisterTable("customers", "customers_v2.parquet")
	if err != nil {
		fmt.Printf("❌ Error registering customers table: %v\n", err)
	} else {
		fmt.Println("✅ Registered 'customers' -> 'data/customers_v2.parquet'")
	}

	// Register with schema information
	userMapping := &core.TableMapping{
		TableName: "users",
		FilePath:  "user_data_2024.parquet",
		Schema: &core.TableSchema{
			Columns: []core.ColumnSchema{
				{Name: "user_id", Type: "int64", Nullable: false},
				{Name: "username", Type: "string", Nullable: false},
				{Name: "email", Type: "string", Nullable: false},
			},
		},
		Properties: map[string]interface{}{
			"description": "User account data",
			"owner":       "user_team",
		},
	}

	err = engine.RegisterTableWithSchema(userMapping)
	if err != nil {
		fmt.Printf("❌ Error registering users table: %v\n", err)
	} else {
		fmt.Println("✅ Registered 'users' with schema information")
	}

	// Demo 2: Load from configuration file
	fmt.Println("\n2️⃣  Loading Table Mappings from Configuration")
	fmt.Println("--------------------------------------------")

	configPath := "table_mappings_example.json"
	if _, err := os.Stat(configPath); err == nil {
		err = engine.LoadTableMappings(configPath)
		if err != nil {
			fmt.Printf("❌ Error loading config: %v\n", err)
		} else {
			fmt.Println("✅ Loaded table mappings from config file")
			
			// List all registered tables
			registry := engine.GetTableRegistry()
			tables := registry.ListTables()
			fmt.Printf("📋 Registered tables: %v\n", tables)
		}
	}

	// Demo 3: Query with mapped tables
	fmt.Println("\n3️⃣  Querying with Mapped Tables")
	fmt.Println("--------------------------------")

	// First, let's create a sample file that we can map
	createSampleEmployeeFile("data/employees_archive_2023.parquet")

	// Register the archive file as the current employees table
	err = engine.RegisterTable("employees", "employees_archive_2023.parquet")
	if err != nil {
		fmt.Printf("❌ Error registering employees mapping: %v\n", err)
	} else {
		fmt.Println("✅ Registered 'employees' -> 'data/employees_archive_2023.parquet'")
	}

	// Now query will use the mapped file
	query := "SELECT COUNT(*) as total FROM employees"
	result, err := engine.Execute(query)
	if err != nil {
		fmt.Printf("❌ Query error: %v\n", err)
	} else if result.Error != "" {
		fmt.Printf("❌ Query returned error: %s\n", result.Error)
	} else {
		fmt.Printf("✅ Query result: %d employees found\n", result.Rows[0]["total"])
		fmt.Println("   (Using mapped archive file instead of default employees.parquet)")
	}

	// Demo 4: Partitioned tables
	fmt.Println("\n4️⃣  Registering Partitioned Tables")
	fmt.Println("----------------------------------")

	// Create sample partition files
	partitions := []string{
		"data/sales/2024/01/sales.parquet",
		"data/sales/2024/02/sales.parquet",
		"data/sales/2024/03/sales.parquet",
	}

	// Create directories
	for _, partition := range partitions {
		dir := filepath.Dir(partition)
		os.MkdirAll(dir, 0755)
		// In a real scenario, these would be actual parquet files
		fmt.Printf("📁 Would create partition: %s\n", partition)
	}

	err = engine.RegisterPartitionedTable("sales_monthly", partitions)
	if err != nil {
		fmt.Printf("❌ Error registering partitioned table: %v\n", err)
	} else {
		fmt.Println("✅ Registered partitioned table 'sales_monthly' with 3 partitions")
	}

	// Demo 5: Save current mappings
	fmt.Println("\n5️⃣  Saving Table Mappings")
	fmt.Println("------------------------")

	outputConfig := "my_table_mappings.json"
	err = engine.SaveTableMappings(outputConfig)
	if err != nil {
		fmt.Printf("❌ Error saving mappings: %v\n", err)
	} else {
		fmt.Printf("✅ Saved current table mappings to %s\n", outputConfig)
	}

	// Demo 6: Benefits summary
	fmt.Println("\n🎯 Benefits of Table Registry:")
	fmt.Println("------------------------------")
	fmt.Println("1. Map logical table names to any physical file")
	fmt.Println("2. Support for versioned data files (e.g., employees_v1, employees_v2)")
	fmt.Println("3. Easy A/B testing with different data versions")
	fmt.Println("4. Partition support for large tables")
	fmt.Println("5. Schema documentation and validation")
	fmt.Println("6. Centralized table metadata management")
	fmt.Println("7. Backward compatibility with existing queries")

	fmt.Println("\n✅ Demo completed!")
}

// Helper function to create a sample parquet file
func createSampleEmployeeFile(path string) {
	// In a real scenario, this would create an actual parquet file
	// For demo purposes, we'll just ensure the directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Error creating directory: %v", err)
	}
	
	// If employees.parquet exists, copy it to the archive path
	srcPath := "data/employees.parquet"
	if _, err := os.Stat(srcPath); err == nil {
		// Copy file (simplified for demo)
		data, err := os.ReadFile(srcPath)
		if err == nil {
			os.WriteFile(path, data, 0644)
		}
	}
}