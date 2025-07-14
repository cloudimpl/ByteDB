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

## Query Operators

ByteDB Columnar Format supports a comprehensive set of query operators for building complex analytical queries.

### Comparison Operators

#### Equality (=)
```go
// Query for exact matches
rows, err := cf.QueryInt("user_id", 1001)
rows, err := cf.QueryString("username", "alice")
```

#### Greater Than (>)
```go
// Find all rows where value > threshold
rows, err := cf.QueryGreaterThan("age", uint8(25))
rows, err := cf.QueryGreaterThan("score", int32(100))
rows, err := cf.QueryGreaterThan("name", "M") // Strings after "M"
```

#### Greater Than or Equal (>=)
```go
// Find all rows where value >= threshold
rows, err := cf.QueryGreaterThanOrEqual("age", uint8(25))
rows, err := cf.QueryGreaterThanOrEqual("active", true) // For booleans
```

#### Less Than (<)
```go
// Find all rows where value < threshold
rows, err := cf.QueryLessThan("score", int32(50))
rows, err := cf.QueryLessThan("name", "John") // Strings before "John"
```

#### Less Than or Equal (<=)
```go
// Find all rows where value <= threshold
rows, err := cf.QueryLessThanOrEqual("score", int32(50))
```

#### Range/Between
```go
// Find all rows where min <= value <= max
rows, err := cf.RangeQueryInt("age", 25, 35)
rows, err := cf.RangeQueryString("name", "Alice", "Bob")
```

### Logical Operators

#### AND Operation
Combine multiple conditions with logical AND:
```go
// Find users aged 25+ with score > 100
ageResults, _ := cf.QueryGreaterThanOrEqual("age", uint8(25))
scoreResults, _ := cf.QueryGreaterThan("score", int32(100))
finalResults := cf.QueryAnd(ageResults, scoreResults)

// Multiple AND conditions
results := cf.QueryAnd(condition1, condition2, condition3)
```

#### OR Operation
Combine multiple conditions with logical OR:
```go
// Find users who are either premium OR active
premiumResults, _ := cf.QueryInt("is_premium", 1)
activeResults, _ := cf.QueryInt("is_active", 1) 
finalResults := cf.QueryOr(premiumResults, activeResults)

// Multiple OR conditions
results := cf.QueryOr(condition1, condition2, condition3)
```

#### NOT Operation
Exclude certain results:
```go
// Find all non-active users
activeResults, _ := cf.QueryGreaterThanOrEqual("active", true)
nonActiveResults, _ := cf.QueryNot("active", activeResults)
```

### Complex Query Examples

#### Multi-Column Filtering
```go
// (age BETWEEN 25 AND 35) AND (score > 100 OR premium = true)
ageRange, _ := cf.RangeQueryInt("age", 25, 35)
highScore, _ := cf.QueryGreaterThan("score", int32(100))
isPremium, _ := cf.QueryInt("premium", 1)
scoreOrPremium := cf.QueryOr(highScore, isPremium)
finalResults := cf.QueryAnd(ageRange, scoreOrPremium)
```

#### String Pattern Matching
```go
// Find names starting with "John" (using range query)
// This works because strings are sorted
results, _ := cf.RangeQueryString("name", "John", "John~")
```

### Supported Data Types

All operators work with the following data types:
- `DataTypeBool` - Boolean values (true/false)
- `DataTypeInt8` - Signed 8-bit integers
- `DataTypeInt16` - Signed 16-bit integers  
- `DataTypeInt32` - Signed 32-bit integers
- `DataTypeInt64` - Signed 64-bit integers
- `DataTypeUint8` - Unsigned 8-bit integers
- `DataTypeUint16` - Unsigned 16-bit integers
- `DataTypeUint32` - Unsigned 32-bit integers
- `DataTypeUint64` - Unsigned 64-bit integers
- `DataTypeFloat32` - 32-bit floating point
- `DataTypeFloat64` - 64-bit floating point
- `DataTypeString` - Variable-length strings
- `DataTypeBinary` - Variable-length binary data

### Space Efficiency by Data Type

Using appropriate data types provides significant space savings:
- **Boolean/Byte**: 34.8% space savings vs int64
- **Short (16-bit)**: 30.4% space savings vs int64
- **Int (32-bit)**: 21.7% space savings vs int64

This is achieved through variable-size key encoding in the B+ tree structure.

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
- **Point Lookup (=)**: O(log n)
- **Range Query (BETWEEN)**: O(log n + k) where k = result size
- **Comparison Operators (>, >=, <, <=)**: O(log n + k)
- **AND Operation**: O(min(n₁, n₂, ...)) where nᵢ = size of result set i
- **OR Operation**: O(n₁ + n₂ + ...) where nᵢ = size of result set i
- **NOT Operation**: O(n) where n = total rows in column
- **Multi-Column Filter**: O(m × log n) where m = number of predicates

### Operator Performance (100K rows dataset)
- **Equality**: ~19µs
- **Greater Than (10% selectivity)**: ~375µs
- **Less Than (0.25% selectivity)**: ~10µs  
- **Range Query (20% selectivity)**: ~774µs
- **Complex AND**: ~2.7ms
- **Complex OR**: ~222µs

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