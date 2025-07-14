# ByteDB Columnar File Format

A high-performance columnar storage format optimized for analytical queries, featuring B+ tree indexes for each column and RoaringBitmap integration for efficient duplicate handling.

## Key Features

- **Column-Oriented Storage**: Each column stored independently with its own B+ tree index
- **O(log n) Point Lookups**: Fast exact match queries using B+ tree structure
- **Efficient Range Queries**: Optimized range scans with sorted data
- **String Deduplication**: Unique strings stored once with offset-based indexing
- **Bitmap Compression**: RoaringBitmap for efficient storage of duplicate values
- **Immutable Design**: Write-once architecture for simplified concurrency
- **4KB Page Architecture**: Optimized for modern storage systems

## Architecture Overview

### File Structure
```
ByteDB Columnar File:
┌─────────────────────────────────────┐
│ File Header (4KB page)              │
├─────────────────────────────────────┤
│ Column Metadata Section             │
├─────────────────────────────────────┤
│ B+ Tree Pages (per column)          │
├─────────────────────────────────────┤
│ String/Binary Segments              │
├─────────────────────────────────────┤
│ RoaringBitmap Pages                │
├─────────────────────────────────────┤
│ Page Directory                      │
└─────────────────────────────────────┘
```

### String Optimization

Strings are stored using a compact segment design:
- **Unique Storage**: Each unique string stored only once
- **Offset-Based Keys**: B+ tree uses 8-byte offsets instead of full strings
- **Sorted Organization**: Strings sorted for efficient range queries

Example efficiency:
- Dataset: 10M rows with 1K unique strings (50 bytes average)
- Traditional: ~500 MB storage
- Optimized: ~80 MB storage (84% reduction)

## Usage Example

```go
package main

import (
    "fmt"
    "bytedb/columnar"
)

func main() {
    // Create a new columnar file
    cf, err := columnar.CreateFile("data.bytedb")
    if err != nil {
        panic(err)
    }
    defer cf.Close()
    
    // Add columns
    cf.AddColumn("user_id", columnar.DataTypeInt64, false)
    cf.AddColumn("username", columnar.DataTypeString, false)
    cf.AddColumn("score", columnar.DataTypeInt64, false)
    
    // Load data
    userData := []struct{ Key int64; RowNum uint64 }{
        {1001, 0},
        {1002, 1},
        {1001, 2}, // Duplicate - will use bitmap
    }
    cf.LoadIntColumn("user_id", userData)
    
    // Query data
    rows, _ := cf.QueryInt("user_id", 1001)
    fmt.Printf("Found %d rows for user_id=1001\n", len(rows))
    
    // Range query
    rows, _ = cf.RangeQueryInt("score", 100, 200)
    fmt.Printf("Found %d rows with score between 100-200\n", len(rows))
}
```

## Running the Example

```bash
cd columnar/example
go run main.go
```

This will create an example file demonstrating:
- Column creation with different data types
- Data loading with duplicates
- Point queries (exact match)
- Range queries
- Statistics retrieval

## Performance Characteristics

### Time Complexity
- **Point Lookup**: O(log n)
- **Range Query**: O(log n + k) where k = result size
- **Multi-Column Filter**: O(m × log n) where m = number of predicates

### Space Efficiency
- **B+ Tree Overhead**: ~10-15%
- **String Deduplication**: Up to 99% reduction for high duplicate scenarios
- **Bitmap Compression**: 90%+ compression for sparse data

### Query Performance
- **Integer Comparisons**: ~10x faster than string comparisons
- **Cache Efficiency**: Small tree nodes improve CPU cache utilization
- **Sequential Access**: Optimized for range scans

## Testing

Run all tests:
```bash
go test -v ./columnar
```

Run benchmarks:
```bash
go test -bench=. ./columnar
```

## Design Principles

1. **Immutable Files**: Once written, files are never modified
2. **Column Independence**: Each column can be queried without reading others
3. **Type-Specific Optimization**: Different strategies for different data types
4. **Memory Efficiency**: Careful management of buffer pools and caches
5. **Query-First Design**: Optimized for analytical query patterns

## File Format Specification

For detailed technical specifications, see [BYTEDB_COLUMNAR_FORMAT.md](../../BYTEDB_COLUMNAR_FORMAT.md).

## Future Enhancements

- [ ] Compression support (Snappy, Zstd, LZ4)
- [ ] Page-level statistics for enhanced filtering
- [ ] Parallel query execution
- [ ] Distributed file support
- [ ] Update/Delete support via versioning
- [ ] Column encryption
- [ ] Adaptive indexing based on query patterns