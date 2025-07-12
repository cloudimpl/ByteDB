package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

// Demo function showing how to use HTTP Parquet files with ByteDB
func demoHTTPUsage() {
	fmt.Println("=== ByteDB HTTP Parquet Demo ===")
	fmt.Println()

	// Create a query engine
	engine := NewQueryEngine("./data")
	defer engine.Close()

	// Method 1: Direct HTTP URL (if you modify your code to support it)
	fmt.Println("Method 1: Register HTTP table and query")
	
	// For this demo, we'll use a placeholder URL
	// In practice, you'd use a real HTTP URL like:
	// engine.RegisterHTTPTable("remote_employees", "https://example.com/data/employees.parquet")
	
	// For demonstration, let's show the API:
	httpURL := "https://example.com/data/employees.parquet"
	engine.RegisterHTTPTable("remote_employees", httpURL)
	
	fmt.Printf("✓ Registered HTTP table 'remote_employees' -> %s\n", httpURL)

	// Example queries you could run:
	exampleQueries := []string{
		"SELECT name, department FROM remote_employees WHERE salary > 70000",
		"SELECT department, COUNT(*) as emp_count FROM remote_employees GROUP BY department",
		"SELECT * FROM remote_employees ORDER BY hire_date DESC LIMIT 10",
	}

	fmt.Println("\nExample queries you can run on HTTP Parquet files:")
	for i, query := range exampleQueries {
		fmt.Printf("%d. %s\n", i+1, query)
	}

	// Show table management
	fmt.Println("\nTable Management:")
	fmt.Println("✓ Register: engine.RegisterHTTPTable(\"table_name\", \"http://url/file.parquet\")")
	fmt.Println("✓ Unregister: engine.UnregisterHTTPTable(\"table_name\")")

	// Benefits
	fmt.Println("\nBenefits of HTTP Parquet with ByteDB:")
	fmt.Println("🚀 Only downloads data needed for your query (HTTP range requests)")
	fmt.Println("💾 No need to download entire files to local storage")  
	fmt.Println("🔄 Same SQL interface as local files")
	fmt.Println("📊 Efficient with Parquet's columnar format")
	fmt.Println("🌐 Query remote data as if it were local")

	// Technical details
	fmt.Println("\nTechnical Implementation:")
	fmt.Println("• Uses HTTP Range requests for efficient partial downloads")
	fmt.Println("• Leverages Parquet's metadata to read only needed columns/row groups")
	fmt.Println("• Transparent fallback to local files when no HTTP URL registered")
	fmt.Println("• Thread-safe table registration and query execution")

	engine.UnregisterHTTPTable("remote_employees")
	fmt.Println()
	fmt.Println("✓ Demo completed!")
}

// Example HTTP server to serve Parquet files (for testing)
func startExampleHTTPServer(port string) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// In a real scenario, you'd serve actual Parquet files here
		filePath := "./data" + r.URL.Path
		
		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}

		// Serve file with range support
		file, err := os.Open(filePath)
		if err != nil {
			http.Error(w, "Failed to open file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			http.Error(w, "Failed to stat file", http.StatusInternalServerError)
			return
		}

		// Set headers for range request support
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")

		// Serve with range support
		http.ServeContent(w, r, filePath, stat.ModTime(), file)
	})

	fmt.Printf("Starting HTTP server on port %s\n", port)
	fmt.Printf("Serving Parquet files from ./data directory\n")
	fmt.Printf("Example URL: http://localhost:%s/employees.parquet\n", port)
	fmt.Println()
	
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Uncomment the main function below to run the demo
/*
func main() {
	if len(os.Args) > 1 && os.Args[1] == "server" {
		// Start HTTP server to serve Parquet files
		port := "8080"
		if len(os.Args) > 2 {
			port = os.Args[2]
		}
		startExampleHTTPServer(port)
	} else {
		// Run the demo
		demoHTTPUsage()
	}
}
*/