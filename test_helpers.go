package main

import (
	"os"
	"path/filepath"
	
	"github.com/parquet-go/parquet-go"
)

// TestDataDir is the directory for test data files
const TestDataDir = "./testdata"

// SetupTestData creates test data in a separate directory for tests
func SetupTestData() error {
	// Create test data directory
	if err := os.MkdirAll(TestDataDir, 0755); err != nil {
		return err
	}

	// Generate test data files
	if err := generateTestEmployees(); err != nil {
		return err
	}
	if err := generateTestProducts(); err != nil {
		return err
	}
	if err := generateTestDepartments(); err != nil {
		return err
	}

	return nil
}

// CleanupTestData removes test data directory
func CleanupTestData() error {
	return os.RemoveAll(TestDataDir)
}

// NewTestQueryEngine creates a query engine pointed at test data
func NewTestQueryEngine() *QueryEngine {
	// Ensure test data exists
	if _, err := os.Stat(TestDataDir); os.IsNotExist(err) {
		if err := SetupTestData(); err != nil {
			panic("Failed to setup test data: " + err.Error())
		}
	}
	return NewQueryEngine(TestDataDir)
}

func generateTestEmployees() error {
	file, err := os.Create(filepath.Join(TestDataDir, "employees.parquet"))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Employee{}))
	defer writer.Close()

	for _, emp := range TestEmployees {
		if err := writer.Write(emp); err != nil {
			return err
		}
	}
	return nil
}

func generateTestProducts() error {
	file, err := os.Create(filepath.Join(TestDataDir, "products.parquet"))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Product{}))
	defer writer.Close()

	for _, prod := range TestProducts {
		if err := writer.Write(prod); err != nil {
			return err
		}
	}
	return nil
}

func generateTestDepartments() error {
	file, err := os.Create(filepath.Join(TestDataDir, "departments.parquet"))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewWriter(file, parquet.SchemaOf(Department{}))
	defer writer.Close()

	for _, dept := range TestDepartments {
		if err := writer.Write(dept); err != nil {
			return err
		}
	}
	return nil
}