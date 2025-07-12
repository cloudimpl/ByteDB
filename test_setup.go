package main

import (
	"fmt"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

// generateSampleData creates sample Parquet files for testing
func generateSampleData() {
	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatal(err)
	}

	generateEmployees()
	generateProducts()
	generateDepartments()
	fmt.Println("Sample data generated in ./data directory")
}

func generateEmployees() {
	// Use fixed test data to ensure tests are deterministic
	employees := TestEmployees

	file, err := os.Create("./data/employees.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Employee{}))
	defer writer.Close()

	for _, emp := range employees {
		if err := writer.Write(emp); err != nil {
			log.Fatal(err)
		}
	}
}

func generateProducts() {
	// Use fixed test data to ensure tests are deterministic
	products := TestProducts

	file, err := os.Create("./data/products.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Product{}))
	defer writer.Close()

	for _, prod := range products {
		if err := writer.Write(prod); err != nil {
			log.Fatal(err)
		}
	}
}

func generateDepartments() {
	// Use fixed test data to ensure tests are deterministic
	departments := TestDepartments

	file, err := os.Create("./data/departments.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Department{}))
	defer writer.Close()

	for _, dept := range departments {
		if err := writer.Write(dept); err != nil {
			log.Fatal(err)
		}
	}
}