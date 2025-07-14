# HTTP Parquet Support in ByteDB

ByteDB now supports querying Parquet files over HTTP using efficient range requests! This allows you to query remote Parquet files without downloading them entirely.

## 🚀 Features

- **HTTP Range Requests**: Only downloads the data needed for your query
- **Transparent Usage**: Same SQL interface as local files
- **Efficient**: Leverages Parquet's columnar format for minimal data transfer
- **No Local Storage**: Query remote files without downloading them
- **Thread-Safe**: Concurrent access to HTTP tables is fully supported

## 📋 Requirements

- Remote server must support HTTP Range requests (`Accept-Ranges: bytes`)
- Parquet files must be accessible via HTTP/HTTPS

## 🛠 Usage

### Basic Usage

```go
// Create a query engine
engine := NewQueryEngine("./data")
defer engine.Close()

// Register an HTTP Parquet file as a table
engine.RegisterHTTPTable("remote_employees", "https://example.com/data/employees.parquet")

// Query it like a normal table
result, err := engine.Execute("SELECT name, department FROM remote_employees WHERE salary > 70000")
```

### Advanced Examples

```go
// Multiple HTTP tables
engine.RegisterHTTPTable("employees", "https://data.company.com/employees.parquet")
engine.RegisterHTTPTable("products", "https://data.company.com/products.parquet")

// JOIN across HTTP tables
result, err := engine.Execute(`
    SELECT e.name, e.department, p.name as product 
    FROM employees e 
    JOIN products p ON e.department = p.department
`)

// Aggregations on HTTP data
result, err := engine.Execute(`
    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
    FROM employees 
    GROUP BY department
`)

// Functions work too
result, err := engine.Execute(`
    SELECT UPPER(name) as name, LENGTH(department) as dept_len
    FROM employees 
    WHERE SUBSTRING(name, 1, 1) = 'J'
`)
```

### Table Management

```go
// Register HTTP table
engine.RegisterHTTPTable("table_name", "https://example.com/file.parquet")

// Unregister (falls back to local file)
engine.UnregisterHTTPTable("table_name")
```

### Mixed Local and HTTP Tables

```go
// Some tables from HTTP, others local
engine.RegisterHTTPTable("remote_data", "https://example.com/data.parquet")

// Query mixing both:
// - remote_data: from HTTP
// - local_table: from ./data/local_table.parquet
result, err := engine.Execute(`
    SELECT 'remote' as source, COUNT(*) FROM remote_data
    UNION ALL
    SELECT 'local' as source, COUNT(*) FROM local_table
`)
```

## 🌐 Setting Up an HTTP Server

To serve your Parquet files over HTTP with range request support:

### Simple Go HTTP Server

```go
package main

import (
    "net/http"
    "os"
)

func main() {
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        filePath := "./data" + r.URL.Path
        
        file, err := os.Open(filePath)
        if err != nil {
            http.NotFound(w, r)
            return
        }
        defer file.Close()

        stat, _ := file.Stat()
        
        // Enable range requests
        w.Header().Set("Accept-Ranges", "bytes")
        w.Header().Set("Content-Type", "application/octet-stream")
        
        http.ServeContent(w, r, filePath, stat.ModTime(), file)
    })

    http.ListenAndServe(":8080", nil)
}
```

### Using with Popular Servers

**Nginx**:
```nginx
location /data/ {
    alias /path/to/parquet/files/;
    add_header Accept-Ranges bytes;
}
```

**Apache**:
```apache
<Directory "/path/to/parquet/files">
    Header set Accept-Ranges bytes
</Directory>
```

**AWS S3**: Range requests are supported by default

**Google Cloud Storage**: Range requests are supported by default

## 📊 Performance Benefits

HTTP Parquet queries are efficient because:

1. **Column Pruning**: Only requested columns are downloaded
2. **Row Group Filtering**: Only relevant row groups are fetched
3. **Metadata First**: Parquet metadata is read first to plan optimal requests
4. **Range Requests**: HTTP Range headers fetch only needed byte ranges
5. **Caching**: Metadata and chunks can be cached for repeated queries

### Example Performance

For a 100MB Parquet file with 50 columns:
- `SELECT col1, col2 FROM table LIMIT 1000`: Downloads ~2MB
- `SELECT * FROM table WHERE date > '2024-01-01'`: Downloads only matching row groups
- `SELECT COUNT(*) FROM table`: Downloads only metadata

## 🧪 Testing

Run the comprehensive HTTP tests:

```bash
# Test HTTP functionality
go test -v -run "TestHTTPIntegration"

# Test error handling
go test -v -run "TestHTTPParquetErrors"

# Test table management
go test -v -run "TestHTTPTableManagement"
```

## 🐛 Error Handling

Common errors and solutions:

**Connection Refused**:
```
failed to create HTTP reader: dial tcp: connection refused
```
- Solution: Check if the HTTP server is running and URL is correct

**Range Not Satisfiable**:
```
HTTP 416 Range Not Satisfiable
```
- Solution: Ensure server supports range requests

**Invalid Parquet File**:
```
failed to open remote parquet file: invalid parquet file
```
- Solution: Verify the file is a valid Parquet file

**DNS Resolution**:
```
dial tcp: lookup hostname: no such host
```
- Solution: Check hostname and network connectivity

## 🔧 Technical Implementation

ByteDB's HTTP Parquet support uses:

- **[howett.net/ranger](https://github.com/DHowett/ranger)**: HTTP range request client
- **[parquet-go](https://github.com/parquet-go/parquet-go)**: Parquet file processing
- **HTTP/1.1 Range Requests**: RFC 7233 compliant range requests
- **Concurrent Access**: Thread-safe table registration and queries

The implementation automatically:
1. Detects HTTP/HTTPS URLs
2. Creates range-capable HTTP readers
3. Integrates with Parquet's io.ReaderAt interface
4. Handles metadata and data requests efficiently

## 🔮 Future Enhancements

Potential improvements:
- Connection pooling and keepalive
- Chunk caching with configurable TTL
- Compression-aware range requests
- Authentication support (Bearer tokens, etc.)
- Retry logic with exponential backoff
- Progress reporting for large queries

---

**Ready to query the world's data!** 🌍✨