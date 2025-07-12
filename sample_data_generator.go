package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)


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
	employees := []Employee{
		{1, "John Doe", "Engineering", 75000.0, 30, "2020-01-15"},
		{2, "Jane Smith", "Marketing", 65000.0, 28, "2019-03-22"},
		{3, "Mike Johnson", "Engineering", 80000.0, 35, "2018-07-10"},
		{4, "Sarah Wilson", "HR", 55000.0, 32, "2021-05-18"},
		{5, "David Brown", "Sales", 70000.0, 29, "2019-11-02"},
		{6, "Lisa Davis", "Engineering", 85000.0, 33, "2017-12-01"},
		{7, "Tom Miller", "Marketing", 62000.0, 27, "2020-08-14"},
		{8, "Anna Garcia", "Finance", 68000.0, 31, "2019-06-25"},
		{9, "Chris Anderson", "Engineering", 78000.0, 34, "2018-04-03"},
		{10, "Maria Rodriguez", "Sales", 72000.0, 26, "2021-02-09"},
	}

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
	products := []Product{
		{1, "Laptop", "Electronics", 999.99, true, "High-performance laptop"},
		{2, "Mouse", "Electronics", 29.99, true, "Wireless optical mouse"},
		{3, "Keyboard", "Electronics", 79.99, true, "Mechanical keyboard"},
		{4, "Monitor", "Electronics", 299.99, false, "24-inch LED monitor"},
		{5, "Desk Chair", "Furniture", 199.99, true, "Ergonomic office chair"},
		{6, "Desk Lamp", "Furniture", 49.99, true, "LED desk lamp"},
		{7, "Notebook", "Stationery", 12.99, true, "Spiral notebook"},
		{8, "Pen Set", "Stationery", 24.99, true, "Set of 12 pens"},
		{9, "Coffee Mug", "Kitchen", 15.99, true, "Ceramic coffee mug"},
		{10, "Water Bottle", "Kitchen", 19.99, false, "Stainless steel water bottle"},
	}

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
	departments := []Department{
		{"Engineering", "Lisa Davis", 500000.0, "Building A", 4},
		{"Marketing", "Jane Smith", 200000.0, "Building B", 2},
		{"HR", "Sarah Wilson", 150000.0, "Building C", 1},
		{"Sales", "David Brown", 300000.0, "Building B", 2},
		{"Finance", "Anna Garcia", 250000.0, "Building C", 1},
		{"Research", "Dr. Smith", 400000.0, "Building D", 0}, // Department with no employees
	}

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

func init() {
	rand.Seed(time.Now().UnixNano())
}

