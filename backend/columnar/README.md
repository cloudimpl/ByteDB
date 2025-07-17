# ByteDB Columnar File Format

A high-performance columnar storage format optimized for analytical queries, featuring space-optimized B+ tree indexes for each column and RoaringBitmap integration for efficient duplicate handling.

## File Extension Requirement

All ByteDB columnar files **must** use the `.bytedb` extension. Both `CreateFile()` and `OpenFile()` functions enforce this requirement and will return an error if a different extension is used.

```go
// ✅ Correct
cf, err := columnar.CreateFile("data.bytedb")

// ❌ Will return error
cf, err := columnar.CreateFile("data.db")      // Error: must have .bytedb extension
cf, err := columnar.CreateFile("data.txt")     // Error: must have .bytedb extension
```

## Key Features

- **Column-Oriented Storage**: Each column stored independently with its own B+ tree index
- **Space-Optimized B+ Trees**: 50-89% space savings through optimized internal node structure
- **O(log n) Point Lookups**: Fast exact match queries using B+ tree structure
- **Efficient Range Queries**: Optimized range scans with sorted data
- **Comprehensive NULL Support**: First-class null handling with nullable columns and null-aware queries
- **Clean Type-Safe API**: `IntData` and `StringData` structures with `IsNull` boolean field
- **String Deduplication**: Unique strings stored once with offset-based indexing
- **Bitmap Compression**: RoaringBitmap for efficient storage of duplicate values
- **Unified Bitmap API**: All query operations return bitmaps for optimal performance
- **Efficient Iterator Interface**: Memory-efficient forward/reverse iteration with range support
- **Logical Deletion Support**: Mark rows for deletion without modifying immutable data
- **File Merge Capability**: Consolidate multiple files with automatic deletion cleanup
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
    
    // Add columns (nullable and non-nullable)
    cf.AddColumn("user_id", columnar.DataTypeInt64, false)  // Non-nullable
    cf.AddColumn("username", columnar.DataTypeString, true) // Nullable
    cf.AddColumn("score", columnar.DataTypeInt64, true)     // Nullable
    
    // Load data using clean type-safe API
    userData := []columnar.IntData{
        columnar.NewIntData(1001, 0),
        columnar.NewIntData(1002, 1),
        columnar.NewIntData(1001, 2), // Duplicate - will use bitmap
    }
    cf.LoadIntColumn("user_id", userData)
    
    // Load data with nulls seamlessly
    usernameData := []columnar.StringData{
        columnar.NewStringData("alice", 0),   // Non-null
        columnar.NewNullStringData(1),        // NULL value
        columnar.NewStringData("charlie", 2), // Non-null
    }
    cf.LoadStringColumn("username", usernameData)
    
    // Query data (returns bitmap)
    bitmap, _ := cf.QueryInt("user_id", 1001)
    fmt.Printf("Found %d rows for user_id=1001\n", bitmap.GetCardinality())
    
    // Query for NULL values
    nullBitmap, _ := cf.QueryNull("username")
    fmt.Printf("Found %d rows with NULL username\n", nullBitmap.GetCardinality())
    
    // Range query (automatically excludes NULLs)
    bitmap, _ = cf.RangeQueryInt("score", 100, 200)
    fmt.Printf("Found %d rows with score between 100-200\n", bitmap.GetCardinality())
    
    // Convert to slice if needed
    rows := columnar.BitmapToSlice(bitmap)
    fmt.Printf("Row numbers: %v\n", rows)
}
```

### Working with Different Data Types

```go
// All integer and boolean types use LoadIntColumn
cf.AddColumn("age", columnar.DataTypeUint8, false)
cf.AddColumn("count", columnar.DataTypeInt32, false)
cf.AddColumn("is_active", columnar.DataTypeBool, false)

// Load data - all use IntData
ageData := []columnar.IntData{
    columnar.NewIntData(25, 0),   // uint8 value
    columnar.NewIntData(30, 1),
}
cf.LoadIntColumn("age", ageData)

countData := []columnar.IntData{
    columnar.NewIntData(1000, 0),  // int32 value
    columnar.NewIntData(-500, 1),
}
cf.LoadIntColumn("count", countData)

boolData := []columnar.IntData{
    columnar.NewIntData(1, 0),     // true (non-zero)
    columnar.NewIntData(0, 1),     // false (zero)
}
cf.LoadIntColumn("is_active", boolData)

// Querying non-Int64 types requires comparison operators
// For equality on uint8:
age25_gte, _ := cf.QueryGreaterThanOrEqual("age", uint8(25))
age25_lte, _ := cf.QueryLessThanOrEqual("age", uint8(25))
age25 := cf.QueryAnd(age25_gte, age25_lte)

// For boolean values:
activeBitmap, _ := cf.QueryGreaterThanOrEqual("is_active", true)
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

#### NULL Operations
Query for NULL and non-NULL values:
```go
// Find rows where column IS NULL
nullBitmap, err := cf.QueryNull("optional_field")

// Find rows where column IS NOT NULL  
notNullBitmap, err := cf.QueryNotNull("optional_field")

// Note: Regular comparison queries automatically exclude NULL values
ageBitmap, _ := cf.QueryGreaterThan("age", int64(25))  // Excludes NULLs
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

#### Fully Supported Types
The following data types have complete support for loading, querying, and iteration:

- `DataTypeBool` - Boolean values (true/false)
- `DataTypeInt8` - Signed 8-bit integers
- `DataTypeInt16` - Signed 16-bit integers  
- `DataTypeInt32` - Signed 32-bit integers
- `DataTypeInt64` - Signed 64-bit integers
- `DataTypeUint8` - Unsigned 8-bit integers
- `DataTypeUint16` - Unsigned 16-bit integers
- `DataTypeUint32` - Unsigned 32-bit integers
- `DataTypeUint64` - Unsigned 64-bit integers
- `DataTypeString` - Variable-length strings

**Note on Integer Types**: While all integer types are supported, direct equality queries using `QueryInt()` only work with `DataTypeInt64`. For other integer types, use comparison operators or range queries:
```go
// For non-Int64 types, use range query for equality
bitmap, _ := cf.QueryGreaterThanOrEqual("age", uint8(25))
bitmap2, _ := cf.QueryLessThanOrEqual("age", uint8(25))
result := cf.QueryAnd(bitmap, bitmap2)  // age = 25
```

#### Not Yet Supported
The following types are defined but not fully implemented:

- `DataTypeFloat32` - 32-bit floating point (no LoadFloatColumn method)
- `DataTypeFloat64` - 64-bit floating point (no LoadFloatColumn method)
- `DataTypeBinary` - Variable-length binary data (no LoadBinaryColumn method)

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

## NULL Handling

ByteDB Columnar Format provides comprehensive first-class NULL support with clean, type-safe API design.

### Nullable Column Declaration

Columns can be declared as nullable or non-nullable during creation:

```go
// Non-nullable columns (IsNullable = false)
cf.AddColumn("id", columnar.DataTypeInt64, false)      // Cannot contain NULLs
cf.AddColumn("email", columnar.DataTypeString, false)  // Required field

// Nullable columns (IsNullable = true)  
cf.AddColumn("age", columnar.DataTypeInt64, true)      // Can contain NULLs
cf.AddColumn("phone", columnar.DataTypeString, true)   // Optional field
```

### Type-Safe Data Structures

The API uses clean data structures with `IsNull` boolean fields:

```go
// Integer data with null support
type IntData struct {
    Value  int64  // The actual value (ignored if IsNull is true)
    RowNum uint64 // Row number for this value  
    IsNull bool   // Whether this value is NULL
}

// String data with null support
type StringData struct {
    Value  string // The actual value (ignored if IsNull is true)
    RowNum uint64 // Row number for this value
    IsNull bool   // Whether this value is NULL
}
```

### Loading Data with NULLs

The unified API seamlessly handles both null and non-null data with built-in validation:

```go
// Load integer data with NULLs (into nullable column)
intData := []columnar.IntData{
    columnar.NewIntData(100, 0),       // Non-null value
    columnar.NewNullIntData(1),        // NULL value
    columnar.NewIntData(200, 2),       // Non-null value
    columnar.NewNullIntData(3),        // NULL value
}
cf.LoadIntColumn("age", intData)  // One method handles all cases

// Load string data with NULLs (into nullable column)
stringData := []columnar.StringData{
    columnar.NewStringData("Alice", 0),  // Non-null value
    columnar.NewNullStringData(1),       // NULL value
    columnar.NewStringData("Bob", 2),    // Non-null value
}
cf.LoadStringColumn("name", stringData)
```

### Data Integrity Validation

The API enforces nullable constraints automatically:

```go
// ✅ Valid: Loading NULLs into nullable column
cf.AddColumn("optional_field", columnar.DataTypeInt64, true)  // Nullable
data := []columnar.IntData{
    columnar.NewIntData(100, 0),
    columnar.NewNullIntData(1),  // Allowed
}
cf.LoadIntColumn("optional_field", data)  // Success

// ❌ Invalid: Loading NULLs into non-nullable column  
cf.AddColumn("required_field", columnar.DataTypeInt64, false)  // Non-nullable
invalidData := []columnar.IntData{
    columnar.NewIntData(100, 0),
    columnar.NewNullIntData(1),  // Not allowed!
}
err := cf.LoadIntColumn("required_field", invalidData)
// Returns error: "column required_field is declared as non-nullable but received NULL value at row 1"
```

### NULL-Aware Queries

Dedicated query methods for NULL handling:

```go
// Find rows where column IS NULL
nullRows, err := cf.QueryNull("age")
fmt.Printf("Found %d rows with NULL age\n", nullRows.GetCardinality())

// Find rows where column IS NOT NULL  
notNullRows, err := cf.QueryNotNull("age")
fmt.Printf("Found %d rows with non-NULL age\n", notNullRows.GetCardinality())

// Regular queries automatically exclude NULL values
ageRange, err := cf.RangeQueryInt("age", 25, 35)  // NULLs excluded
equalityMatch, err := cf.QueryInt("age", 30)       // NULLs excluded
```

### NULL Behavior in Operations

#### Comparison Operations
- **Regular queries** (=, >, <, BETWEEN) automatically **exclude NULL values**
- **Range queries** only return rows with non-NULL values in the specified range
- **NULL values are never included** in comparison results

#### Logical Operations
```go
// Complex queries with NULL awareness
hasAge, _ := cf.QueryNotNull("age")              // Rows with non-NULL age
hasName, _ := cf.QueryNotNull("name")            // Rows with non-NULL name
completeData := cf.QueryAnd(hasAge, hasName)     // Rows with both fields

missingAge, _ := cf.QueryNull("age")             // Rows with NULL age  
missingName, _ := cf.QueryNull("name")           // Rows with NULL name
missingEither := cf.QueryOr(missingAge, missingName) // Rows missing at least one field
```

### NULL Statistics

Column statistics include null counts and properly handle NULL values:

```go
stats, _ := cf.GetStats("nullable_column")
fmt.Printf("Null count: %v\n", stats["null_count"])       // Number of NULL values
fmt.Printf("Total keys: %v\n", stats["total_keys"])       // Non-NULL values only
fmt.Printf("Distinct count: %v\n", stats["distinct_count"]) // Excludes NULL from count
```

### Performance Characteristics

- **NULL Storage**: NULLs stored efficiently using RoaringBitmaps (1 bit per row)
- **Query Performance**: NULL queries are O(1) bitmap operations
- **Space Efficiency**: NULL values don't consume space in B+ tree indexes
- **Memory Usage**: Minimal overhead for NULL tracking

### Migration and Compatibility

The new API is designed for clean usage while maintaining performance:

```go
// ✅ Clean, recommended approach
data := []columnar.IntData{
    columnar.NewIntData(value, rowNum),    // Non-null
    columnar.NewNullIntData(rowNum),       // NULL
}
cf.LoadIntColumn("column", data)

// ✅ Helper functions for bulk operations
data := make([]columnar.IntData, 1000)
for i := 0; i < 1000; i++ {
    if shouldBeNull(i) {
        data[i] = columnar.NewNullIntData(uint64(i))
    } else {
        data[i] = columnar.NewIntData(getValue(i), uint64(i))
    }
}
```

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

## Iterator API

ByteDB Columnar Format provides efficient iterators for ordered traversal of column data, offering both forward and reverse iteration capabilities with range support.

### Creating Iterators

```go
// Basic column iterator
iter, err := cf.NewIterator("user_id")
if err != nil {
    panic(err)
}
defer iter.Close()

// Range iterator with bounds
rangeIter, err := cf.NewRangeIterator("score", int32(100), int32(200))
if err != nil {
    panic(err)
}
defer rangeIter.Close()

// Convert bitmap results to iterator
bitmap, _ := cf.QueryGreaterThan("age", uint8(25))
bitmapIter, err := cf.BitmapToIterator("age", bitmap)
```

### Iterator Navigation

```go
// Forward iteration
for iter.Next() {
    key := iter.Key()           // Get current key value (typed)
    rows := iter.Rows()         // Get bitmap of rows for this key
    fmt.Printf("Key %v: %d rows\n", key, rows.GetCardinality())
}

// Reverse iteration
iter.SeekLast()                 // Position at last element
for iter.Prev() {
    key := iter.Key()
    rows := iter.Rows()
    // Process in reverse order
}

// Seek operations
iter.Seek(int64(150))          // Position at first key >= 150
iter.SeekFirst()               // Position at first key
iter.SeekLast()                // Position at last key

// Mixed navigation
iter.SeekFirst()
iter.Next()                    // Move forward
iter.Next()
iter.Prev()                    // Move backward
```

### Type-Safe Key Access

The iterator returns properly typed keys based on the column data type:

```go
// Integer columns
iter, _ := cf.NewIterator("user_id")  // int64 column
for iter.Next() {
    userId := iter.Key().(int64)      // Type assertion to int64
    // Process userId
}

// String columns  
iter, _ := cf.NewIterator("username")  // string column
for iter.Next() {
    username := iter.Key().(string)    // Type assertion to string
    // Process username
}

// Boolean columns
iter, _ := cf.NewIterator("is_active") // bool column
for iter.Next() {
    isActive := iter.Key().(bool)      // Type assertion to bool
    // Process boolean value
}
```

### Range Iteration

```go
// Create range iterator
iter, err := cf.NewRangeIterator("price", float64(10.0), float64(100.0))

// Iterate through range
for iter.Next() {
    price := iter.Key().(float64)
    if !iter.InBounds() {
        break  // Outside range
    }
    // Process price within [10.0, 100.0]
}

// Check range bounds
if iter.HasLowerBound() {
    lower := iter.LowerBound()  // Get lower bound value
}
if iter.HasUpperBound() {
    upper := iter.UpperBound()  // Get upper bound value
}
```

### Iterator with Duplicates

When a key has multiple associated rows, the iterator returns all rows as a bitmap:

```go
// Column with duplicate values
iter, _ := cf.NewIterator("category")
for iter.Next() {
    category := iter.Key().(string)
    rowBitmap := iter.Rows()
    
    // Process all rows for this category
    fmt.Printf("Category %s appears in %d rows\n", 
        category, rowBitmap.GetCardinality())
    
    // Iterate through individual rows if needed
    rowIter := rowBitmap.Iterator()
    for rowIter.HasNext() {
        rowNum := rowIter.Next()
        // Process individual row
    }
}
```

### Error Handling

```go
iter, err := cf.NewIterator("column_name")
if err != nil {
    // Handle creation error
}

// Check for errors during iteration
for iter.Next() {
    if iter.Error() != nil {
        // Handle iteration error
        break
    }
    // Process data
}

// Check validity
if !iter.Valid() {
    // Iterator is not positioned at valid entry
}
```

### Performance Characteristics

- **Memory Efficient**: Streaming design - doesn't load entire column into memory
- **Page Caching**: Caches recently accessed B-tree pages for efficiency
- **O(log n) Seeks**: Fast positioning using B-tree structure
- **Compression Aware**: Works transparently with compressed columns
- **Type Safe**: Returns properly typed values based on column type

### Iterator Data Type Support

Iterators work with all fully supported data types:
- ✅ Boolean, Int8, Int16, Int32, Int64
- ✅ Uint8, Uint16, Uint32, Uint64
- ✅ String (with lexicographic ordering)
- ❌ Float32, Float64 (not yet implemented)
- ❌ Binary (not yet implemented)

### Use Cases

1. **Ordered Data Processing**:
   ```go
   // Process users in ID order
   iter, _ := cf.NewIterator("user_id")
   for iter.Next() {
       processUser(iter.Key().(int64), iter.Rows())
   }
   ```

2. **Range Analysis**:
   ```go
   // Analyze scores in specific range
   iter, _ := cf.NewRangeIterator("score", 80, 100)
   for iter.Next() {
       analyzeHighScores(iter.Key().(int32), iter.Rows())
   }
   ```

3. **Reverse Processing**:
   ```go
   // Process latest entries first
   iter, _ := cf.NewIterator("timestamp")
   iter.SeekLast()
   for count := 0; count < 100 && iter.Valid(); count++ {
       processRecentEntry(iter.Key().(int64), iter.Rows())
       iter.Prev()
   }
   ```

4. **Memory-Efficient Exports**:
   ```go
   // Export large dataset without loading into memory
   iter, _ := cf.NewIterator("product_id")
   writer := csv.NewWriter(outputFile)
   for iter.Next() {
       exportProduct(writer, iter.Key(), iter.Rows())
   }
   ```

## Deletion and Merge API

ByteDB Columnar Format supports logical deletion and efficient file merging, enabling cleanup of deleted data during consolidation operations.

### Deletion API

Since columnar files are immutable, deletions are tracked using a bitmap that marks rows as deleted without modifying the actual data:

```go
// Mark a single row as deleted
err := cf.DeleteRow(rowNum)

// Mark multiple rows as deleted
deletedRows := roaring.New()
deletedRows.Add(10, 20, 30)
err := cf.DeleteRows(deletedRows)

// Check if a row is deleted
if cf.IsRowDeleted(rowNum) {
    // Row is marked for deletion
}

// Get count of deleted rows
count := cf.GetDeletedCount()

// Get bitmap of all deleted rows
deletedBitmap := cf.GetDeletedRows()

// Restore deleted rows
err := cf.UndeleteRow(rowNum)
err := cf.UndeleteRows(restoredRows)
```

### Important Notes on Deletion

- **Immutable Files**: Deletions don't modify existing data - they're tracked in a separate bitmap
- **Query Performance**: Deleted rows are NOT filtered during queries for performance reasons
- **Persistence**: The deleted bitmap is saved in the file header and restored on file open
- **Primary Use Case**: Deletions are primarily used during file merging to exclude rows from new files

### Global Row Numbers

ByteDB Columnar Format uses globally unique row numbers across all files in a dataset. This is essential for the deletion system to work correctly:

- **External Management**: Row number generation is managed externally by the application
- **Cross-File References**: Deleted row bitmaps in one file can reference rows in other files
- **Merge Behavior**: During merge, all deleted bitmaps are collected to create a global view

Example with global row numbers:
```go
// File 1: Uses global row numbers 1000-1999
data1 := []columnar.IntData{
    columnar.NewIntData(100, 1000),
    columnar.NewIntData(101, 1001),
    columnar.NewIntData(102, 1002),
}
cf1.LoadIntColumn("id", data1)

// File 2: Uses global row numbers 2000-2999
// Can mark rows from File 1 as deleted
data2 := []columnar.IntData{
    columnar.NewIntData(200, 2000),
    columnar.NewIntData(201, 2001),
}
cf2.LoadIntColumn("id", data2)
cf2.DeleteRow(1001)  // Marks row from File 1 as deleted

// During merge, row 1001 from File 1 will be excluded
```

### File Merging

The merge functionality combines multiple columnar files into a single consolidated file, with support for handling deleted rows:

```go
// Basic merge
err := MergeFiles("output.bytedb", []string{"file1.bytedb", "file2.bytedb"}, nil)

// Merge with options
options := &MergeOptions{
    // How to handle deleted rows
    DeletedRowHandling: ExcludeDeleted,  // Default: exclude deleted rows
    
    // Whether to deduplicate keys across files
    DeduplicateKeys: true,
    
    // Compression for output file
    CompressionOptions: NewCompressionOptions().
        WithPageCompression(CompressionZstd, CompressionLevelBest),
    
    // Use memory-efficient streaming merge (recommended for large files)
    UseStreamingMerge: true,
}

err := MergeFiles("output.bytedb", inputFiles, options)
```

#### Streaming Merge

For large datasets that exceed available memory, enable streaming merge:

```go
options := &MergeOptions{
    UseStreamingMerge: true,  // Uses iterators instead of loading all data
}
```

Streaming merge:
- Uses sorted iterators to merge data without loading everything into memory
- Writes data directly to output file pages in append-only fashion
- Maintains sort order using a min-heap of iterators
- Zero memory accumulation - processes one entry at a time
- Ideal for merging files larger than available RAM

### Deletion Handling During Merge

Three strategies are available for handling deleted rows:

1. **ExcludeDeleted** (default): Deleted rows are not written to the merged file
   ```go
   options := &MergeOptions{
       DeletedRowHandling: ExcludeDeleted,
   }
   ```

2. **PreserveDeleted**: Deleted rows are copied with their deleted status maintained
   ```go
   options := &MergeOptions{
       DeletedRowHandling: PreserveDeleted,
   }
   ```

3. **ConsolidateDeleted**: Merge all deleted bitmaps into the output file
   ```go
   options := &MergeOptions{
       DeletedRowHandling: ConsolidateDeleted,
   }
   ```

### Merge Examples

#### Merging with Cleanup
```go
// Mark obsolete data for deletion
cf1.DeleteRows(obsoleteRows)
cf1.Close()

// During merge, obsolete rows are excluded
err := MergeFiles("cleaned.bytedb", []string{"cf1.bytedb", "cf2.bytedb"}, 
    &MergeOptions{
        DeletedRowHandling: ExcludeDeleted,
    })
```

#### Handling Duplicates
```go
options := &MergeOptions{
    // Keep only the latest version of duplicate keys
    DeduplicateKeys: true,
}

// When files have overlapping data, later files take precedence
err := MergeFiles("merged.bytedb", []string{"old.bytedb", "new.bytedb"}, options)
```

#### Column-Specific Merge Strategies
```go
options := &MergeOptions{
    ColumnStrategies: map[string]ColumnMergeStrategy{
        "count": {
            // Custom merge function for aggregation
            MergeFunc: func(values []interface{}) interface{} {
                sum := int64(0)
                for _, v := range values {
                    sum += v.(int64)
                }
                return sum
            },
        },
    },
}
```

### Performance Considerations

- **Memory Efficient**: 
  - Regular merge: Loads all data into memory (limited by RAM)
  - Streaming merge: Uses iterators to process data in batches (limited only by disk space)
- **Deleted Row Overhead**: No query-time overhead since deletions aren't checked during normal queries
- **Merge Performance**: 
  - Regular merge: O(n log n) when deduplication enabled due to in-memory sorting
  - Streaming merge: O(n log k) where k is number of input files (heap-based merge)
- **Deduplication Cost**: 
  - Regular merge: Requires sorting all data in memory
  - Streaming merge: Maintains sort order naturally through heap-based merge

## Running the Examples

### Basic Usage Example
```bash
cd columnar/example
go run main.go
```

This will create an example file demonstrating:
- Column creation with different data types
- Data loading with duplicates using the new type-safe API
- Point queries (exact match)
- Range queries
- Bitmap operations
- Statistics retrieval

### NULL Handling Example
```bash
cd columnar/example  
go run null_example.go
```

This demonstrates comprehensive NULL support:
- Nullable and non-nullable column creation
- Loading data with NULL values using `IsNull` boolean field
- NULL-specific queries (`QueryNull`, `QueryNotNull`)
- Automatic NULL exclusion in regular queries
- Complex NULL-aware logical operations
- NULL statistics and tracking

### Iterator Example
```bash
cd columnar/example
go run iterator_example.go
```

This demonstrates the iterator functionality:
- Forward and reverse iteration
- Range-bounded iteration
- Seek operations
- Handling duplicate values with bitmaps
- Type-safe key access
- Complex filtering with iterators

### Deletion and Merge Example
```bash
cd columnar/example
go run deletion_merge_example.go
```

This demonstrates deletion and merge functionality:
- Marking rows for deletion in immutable files
- Creating multiple files with related data
- Merging files while excluding deleted rows
- Handling duplicate keys during merge
- Verifying merge results with queries and iterators

### Global Deletion Example
```bash
cd columnar/example
go run global_deletion_example.go
```

This demonstrates the global row number system:
- Using globally unique row numbers across multiple files
- Cross-file deletion where later files mark rows from earlier files as deleted
- Accumulation of deletion information across time
- Global deletion handling during merge operations
- Efficient deletion without modifying immutable files

## Performance Characteristics

### Time Complexity
- **Point Lookup (=)**: O(log n)
- **Range Query (BETWEEN)**: O(log n + k) where k = result size
- **Comparison Operators (>, >=, <, <=)**: O(log n + k)
- **AND Operation**: O(min(n₁, n₂, ...)) where nᵢ = size of result set i
- **OR Operation**: O(n₁ + n₂ + ...) where nᵢ = size of result set i
- **NOT Operation**: O(n) where n = total rows in column
- **Multi-Column Filter**: O(m × log n) where m = number of predicates
- **Iterator Seek**: O(log n) for positioning
- **Iterator Next/Prev**: O(1) amortized for sequential access
- **Iterator Range Scan**: O(log n + k) where k = keys in range

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
- **Comprehensive NULL Support**: First-class null handling with nullable columns and type-safe API
- **Clean Type-Safe API**: `IntData` and `StringData` structures with `IsNull` boolean field
- **Unified Bitmap API**: All queries return bitmaps for optimal performance
- **Enhanced Test Coverage**: Comprehensive data validation across all test cases
- **Child Page Mapping**: Runtime reconstruction of B+ tree navigation
- **Zero Backward Compatibility Overhead**: Streamlined API for early-stage development
- **Efficient Iterator Interface**: Memory-efficient streaming iteration with forward/reverse support
- **Range Iterator Support**: Bounded iteration within specified key ranges
- **Type-Safe Iteration**: Properly typed key values based on column data type
- **Logical Deletion Bitmap**: Track deleted rows for cleanup during file consolidation
- **File Merge Functionality**: Combine multiple files with configurable deletion handling
- **Global Row Number Support**: Cross-file deletion references for distributed datasets
- **Multiple Integer Types**: Support for Int8/16/32/64 and Uint8/16/32/64 with compression
- **Streaming Merge**: Memory-efficient merge using iterators for datasets larger than RAM

### 🎯 Performance Gains
- **Storage Efficiency**: 50-89% reduction in B+ tree space usage
- **NULL Efficiency**: RoaringBitmap-based NULL storage with minimal overhead
- **Query Performance**: Direct bitmap returns eliminate conversion overhead
- **Memory Usage**: Compressed bitmap storage for sparse result sets
- **Type Safety**: Compile-time safety with clean data structures
- **Cache Utilization**: Smaller tree nodes improve CPU cache performance

## Future Enhancements

- [x] Compression support (Gzip, Snappy, Zstd)
- [ ] Page-level statistics for enhanced filtering
- [ ] Parallel query execution
- [ ] Distributed file support
- [x] Delete support via logical deletion bitmap
- [ ] Update support via versioning
- [ ] Column encryption
- [ ] Adaptive indexing based on query patterns
- [ ] Advanced bitmap operations (XOR, ANDNOT)
- [ ] Vectorized query execution
- [ ] Float32/Float64 data type support
- [ ] Binary data type support
- [ ] Generic equality query method for all types
- [ ] Direct LoadFloatColumn and LoadBinaryColumn methods