# ByteDB Columnar File Format

A high-performance columnar storage format optimized for analytical queries, featuring space-optimized B+ tree indexes for each column and RoaringBitmap integration for efficient duplicate handling.

## Key Features

- **Column-Oriented Storage**: Each column stored independently with its own B+ tree index
- **Space-Optimized B+ Trees**: 50-89% space savings through optimized internal node structure
- **O(log n) Point Lookups**: Fast exact match queries using B+ tree structure
- **Efficient Range Queries**: Optimized range scans with sorted data
- **String Deduplication**: Unique strings stored once with offset-based indexing
- **Bitmap Compression**: RoaringBitmap for efficient storage of duplicate values
- **Unified Bitmap API**: All query operations return bitmaps for optimal performance
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
    
    // Query data (returns bitmap)
    bitmap, _ := cf.QueryInt("user_id", 1001)
    fmt.Printf("Found %d rows for user_id=1001\n", bitmap.GetCardinality())
    
    // Range query (returns bitmap)
    bitmap, _ = cf.RangeQueryInt("score", 100, 200)
    fmt.Printf("Found %d rows with score between 100-200\n", bitmap.GetCardinality())
    
    // Convert to slice if needed
    rows := columnar.BitmapToSlice(bitmap)
    fmt.Printf("Row numbers: %v\n", rows)
}
```

## Query Operators

ByteDB Columnar Format supports a comprehensive set of query operators for building complex analytical queries.

### Comparison Operators

#### Equality (=)
```go
// Query for exact matches (returns bitmap)
bitmap, err := cf.QueryInt("user_id", 1001)
bitmap, err := cf.QueryString("username", "alice")
```

#### Greater Than (>)
```go
// Find all rows where value > threshold (returns bitmap)
bitmap, err := cf.QueryGreaterThan("age", uint8(25))
bitmap, err := cf.QueryGreaterThan("score", int32(100))
bitmap, err := cf.QueryGreaterThan("name", "M") // Strings after "M"
```

#### Greater Than or Equal (>=)
```go
// Find all rows where value >= threshold (returns bitmap)
bitmap, err := cf.QueryGreaterThanOrEqual("age", uint8(25))
bitmap, err := cf.QueryGreaterThanOrEqual("active", true) // For booleans
```

#### Less Than (<)
```go
// Find all rows where value < threshold (returns bitmap)
bitmap, err := cf.QueryLessThan("score", int32(50))
bitmap, err := cf.QueryLessThan("name", "John") // Strings before "John"
```

#### Less Than or Equal (<=)
```go
// Find all rows where value <= threshold (returns bitmap)
bitmap, err := cf.QueryLessThanOrEqual("score", int32(50))
```

#### Range/Between
```go
// Find all rows where min <= value <= max (returns bitmap)
bitmap, err := cf.RangeQueryInt("age", 25, 35)
bitmap, err := cf.RangeQueryString("name", "Alice", "Bob")
```

### Logical Operators

#### AND Operation
Combine multiple conditions with logical AND:
```go
// Find users aged 25+ with score > 100
ageBitmap, _ := cf.QueryGreaterThanOrEqual("age", uint8(25))
scoreBitmap, _ := cf.QueryGreaterThan("score", int32(100))
result := cf.QueryAnd(ageBitmap, scoreBitmap)

// Multiple AND conditions
result := cf.QueryAnd(condition1, condition2, condition3)
```

#### OR Operation
Combine multiple conditions with logical OR:
```go
// Find users who are either premium OR active
premiumBitmap, _ := cf.QueryInt("is_premium", 1)
activeBitmap, _ := cf.QueryInt("is_active", 1) 
result := cf.QueryOr(premiumBitmap, activeBitmap)

// Multiple OR conditions
result := cf.QueryOr(condition1, condition2, condition3)
```

#### NOT Operation
Exclude certain results:
```go
// Find all non-active users
activeBitmap, _ := cf.QueryGreaterThanOrEqual("active", true)
nonActiveBitmap := cf.QueryNot(activeBitmap)
```

### Complex Query Examples

#### Multi-Column Filtering
```go
// (age BETWEEN 25 AND 35) AND (score > 100 OR premium = true)
ageRange, _ := cf.RangeQueryInt("age", 25, 35)
highScore, _ := cf.QueryGreaterThan("score", int32(100))
isPremium, _ := cf.QueryInt("premium", 1)
scoreOrPremium := cf.QueryOr(highScore, isPremium)
result := cf.QueryAnd(ageRange, scoreOrPremium)

// Get final row count
fmt.Printf("Found %d matching users\n", result.GetCardinality())
```

#### String Pattern Matching
```go
// Find names starting with "John" (using range query)
// This works because strings are sorted
namesBitmap, _ := cf.RangeQueryString("name", "John", "John~")
matchingRows := columnar.BitmapToSlice(namesBitmap)
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

The columnar format provides multiple layers of space optimization:

#### Variable-Size Key Encoding
Using appropriate data types provides significant space savings:
- **Boolean/Byte**: 34.8% space savings vs int64
- **Short (16-bit)**: 30.4% space savings vs int64
- **Int (32-bit)**: 21.7% space savings vs int64

#### Optimized B+ Tree Structure
Our space-optimized B+ tree implementation removes child pointers from internal nodes:
- **Boolean/UInt8**: 88.9% space reduction in internal nodes
- **UInt16**: 80.0% space reduction in internal nodes  
- **UInt32**: 66.7% space reduction in internal nodes
- **UInt64**: 50.0% space reduction in internal nodes

#### How Space Optimization Works
1. **No Child Pointers**: Internal nodes store only keys, not child page pointers
2. **Runtime Mapping**: Child page relationships reconstructed from parent pointers on file open
3. **Bulk-Loaded Structure**: Sequential page allocation enables efficient navigation
4. **Automatic Reconstruction**: Child page mapping rebuilt transparently when files are reopened

This multi-layered approach delivers both storage efficiency and query performance.

## Bitmap API

ByteDB Columnar Format provides a unified bitmap-based API for improved performance and memory efficiency. **All query operations now return bitmaps by default**, eliminating backward compatibility overhead and optimizing for analytical workloads.

### Unified Bitmap Query Methods

```go
// All query types return *roaring.Bitmap directly
bitmap, err := cf.QueryInt("user_id", 1001)
bitmap, err := cf.QueryString("username", "alice")  
bitmap, err := cf.RangeQueryInt("score", 100, 200)
bitmap, err := cf.QueryGreaterThan("age", uint8(25))
bitmap, err := cf.QueryGreaterThanOrEqual("score", int32(100))
bitmap, err := cf.QueryLessThan("price", 50.0)
bitmap, err := cf.QueryLessThanOrEqual("count", int64(1000))
```

### Working with Bitmaps

```go
// Logical operations on bitmaps
result := cf.QueryAnd(bitmap1, bitmap2, bitmap3)
result := cf.QueryOr(bitmap1, bitmap2)

// Convert bitmap to slice when needed
rows := columnar.BitmapToSlice(bitmap)

// Get cardinality without converting
count := bitmap.GetCardinality()

// Check if specific row is present
hasRow := bitmap.Contains(uint32(rowNumber))

// Iterate over set bits efficiently
iterator := bitmap.Iterator()
for iterator.HasNext() {
    rowNum := iterator.Next()
    // Process row
}
```

### Performance Benefits

- **Memory Efficiency**: Bitmaps use compressed storage for sparse data
- **Fast Set Operations**: AND/OR operations are optimized at the bit level
- **Reduced Allocations**: Reuse bitmap objects instead of creating new slices
- **Cache Friendly**: Smaller memory footprint improves CPU cache utilization

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
- **Optimized B+ Tree Overhead**: ~5-10% (50-89% reduction vs traditional B+ trees)
- **Variable-Size Key Encoding**: 21-35% space savings by data type
- **String Deduplication**: Up to 99% reduction for high duplicate scenarios
- **Bitmap Compression**: 90%+ compression for sparse data

### Query Performance
- **Integer Comparisons**: ~10x faster than string comparisons
- **Cache Efficiency**: Small tree nodes improve CPU cache utilization
- **Sequential Access**: Optimized for range scans

## Testing

The columnar format includes comprehensive test coverage with enhanced data validation:

### Test Coverage
- **16 test suites** covering all major functionality
- **49 individual tests** with complete data validation
- **Comprehensive validation**: Tests verify actual returned data, not just counts
- **Cross-API validation**: Ensures bitmap and slice APIs return identical results
- **Edge case testing**: Boundary conditions, empty results, and error scenarios

### Running Tests

Run all tests with enhanced validation:
```bash
go test -v ./columnar
```

Run specific test suites:
```bash
go test -v ./columnar -run TestBitmapAPI
go test -v ./columnar -run TestSpaceEfficiency
```

Run benchmarks:
```bash
go test -bench=. ./columnar
```

### Test Features
- **Data Integrity Validation**: Verifies query results match expected row numbers
- **Logical Consistency**: Validates AND/OR operations follow Boolean logic  
- **Boundary Testing**: Ensures range queries respect min/max boundaries
- **Pattern Validation**: Confirms data follows expected distributions
- **Performance Regression**: Monitors query performance over time

## Design Principles

1. **Immutable Files**: Once written, files are never modified
2. **Column Independence**: Each column can be queried without reading others
3. **Type-Specific Optimization**: Different strategies for different data types
4. **Memory Efficiency**: Careful management of buffer pools and caches
5. **Query-First Design**: Optimized for analytical query patterns

## File Format Specification

For detailed technical specifications, see [BYTEDB_COLUMNAR_FORMAT.md](../../BYTEDB_COLUMNAR_FORMAT.md).

## Recent Improvements

### ✅ Completed Optimizations (2024)
- **Space-Optimized B+ Trees**: 50-89% space reduction in internal nodes
- **Unified Bitmap API**: All queries return bitmaps for optimal performance
- **Enhanced Test Coverage**: Comprehensive data validation across all test cases
- **Child Page Mapping**: Runtime reconstruction of B+ tree navigation
- **Zero Backward Compatibility Overhead**: Streamlined API for early-stage development

### 🎯 Performance Gains
- **Storage Efficiency**: 50-89% reduction in B+ tree space usage
- **Query Performance**: Direct bitmap returns eliminate conversion overhead
- **Memory Usage**: Compressed bitmap storage for sparse result sets
- **Cache Utilization**: Smaller tree nodes improve CPU cache performance

## Future Enhancements

- [ ] Compression support (Snappy, Zstd, LZ4)
- [ ] Page-level statistics for enhanced filtering
- [ ] Parallel query execution
- [ ] Distributed file support
- [ ] Update/Delete support via versioning
- [ ] Column encryption
- [ ] Adaptive indexing based on query patterns
- [ ] Advanced bitmap operations (XOR, ANDNOT)
- [ ] Vectorized query execution