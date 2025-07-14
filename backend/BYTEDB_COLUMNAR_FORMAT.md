# ByteDB Columnar File Format Architecture

**Version**: 1.0  
**Date**: 2025-07-14  
**Status**: Design Specification  

## 📋 Table of Contents

1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [File Structure](#file-structure)
4. [Page Management](#page-management)
5. [B+ Tree Implementation](#b-tree-implementation)
6. [Data Type Strategies](#data-type-strategies)
7. [RoaringBitmap Integration](#roaringbitmap-integration)
8. [Query Processing](#query-processing)
9. [Implementation Plan](#implementation-plan)
10. [Performance Characteristics](#performance-characteristics)
11. [Trade-offs & Design Decisions](#trade-offs--design-decisions)

## 🎯 Overview

ByteDB Columnar Format is a new immutable, column-oriented storage format designed specifically for analytical workloads. Unlike traditional formats like Parquet, this format stores each column as an independent B+ tree structure, enabling O(log n) search operations and optimized range queries.

### Key Innovation
- **Independent Column B+ Trees**: Each column is stored as a separate B+ tree for optimal search performance
- **RoaringBitmap Integration**: Efficient handling of duplicate values using compressed bitmaps
- **Type-Aware Storage**: Different optimization strategies for fixed-length vs variable-length data
- **Immutable Design**: Simplified concurrency model with single writer, multiple readers

### Target Use Cases
- **Analytical Queries**: Fast aggregations, filtering, and range operations
- **Point Lookups**: O(log n) search for specific values
- **Range Queries**: Efficient BETWEEN operations using B+ tree ordering
- **Multi-Column Filtering**: Bitmap intersection for complex predicates
- **High Cardinality Data**: Efficient storage of columns with many unique values

## 🏗️ Design Principles

### 1. **Immutable File Format**
- **Write-Once Philosophy**: Files are never modified after creation
- **Atomic Updates**: New files replace old ones for data updates
- **Reader Consistency**: Readers always see stable, consistent data
- **Simplified Concurrency**: No complex locking mechanisms needed

### 2. **Column Independence**
- **Separate B+ Trees**: Each column stored independently
- **Independent Access**: Can read individual columns without others
- **Optimized Storage**: Type-specific optimizations per column
- **Parallel Processing**: Columns can be processed in parallel

### 3. **Page-Based Architecture**
- **Fixed 4KB Pages**: Standard page size for optimal I/O
- **Memory Alignment**: Pages align with OS memory pages
- **Buffer Pool Friendly**: Efficient caching and memory management
- **Atomic I/O Operations**: Complete pages read/written atomically

### 4. **Query-Optimized Design**
- **Search Performance**: O(log n) point lookups via B+ trees
- **Range Efficiency**: Sequential leaf page access for ranges
- **Bitmap Operations**: Fast set operations for filtering
- **Statistics Integration**: Built-in statistics for query optimization

## 📁 File Structure

### Overall File Layout
```
ByteDB Columnar File:
┌─────────────────────────────────────┐
│ File Header (4KB page)              │
├─────────────────────────────────────┤
│ Column Metadata Section             │
│ ├─ Column 1 metadata                │
│ ├─ Column 2 metadata                │
│ └─ ...                              │
├─────────────────────────────────────┤
│ B+ Tree Pages (4KB each)            │
│ ├─ Column 1 tree (offsets for vars) │
│ ├─ Column 2 tree (direct for fixed) │
│ └─ ...                              │
├─────────────────────────────────────┤
│ String/Binary Segments (4KB each)   │
│ ├─ Unique strings (sorted, compact) │
│ ├─ Unique binary data               │
│ └─ ...                              │
├─────────────────────────────────────┤
│ RoaringBitmap Pages (4KB each)      │
│ ├─ Bitmap for duplicate values      │
│ └─ ...                              │
├─────────────────────────────────────┤
│ Page Directory                      │
│ ├─ Page offset table                │
│ └─ Page type information            │
└─────────────────────────────────────┘
```

**Key Layout Changes**:
- **String/Binary Segments**: New section for compact storage of unique variable-length values
- **Offset-Based Trees**: B+ trees for variable-length columns store offsets instead of full values
- **Eliminated Redundancy**: No duplicate strings/binary data stored anywhere in the file

### File Header Structure (4KB)
```c
struct FileHeader {
    // Magic number and version
    uint32_t magic_number;           // "BYDB" or similar
    uint16_t major_version;          // Format major version
    uint16_t minor_version;          // Format minor version
    
    // File metadata
    uint64_t file_size;              // Total file size in bytes
    uint32_t page_size;              // Always 4096 for this format
    uint64_t row_count;              // Total number of rows
    uint32_t column_count;           // Number of columns
    
    // Section offsets
    uint64_t metadata_offset;        // Column metadata section offset
    uint64_t page_directory_offset;  // Page directory offset
    uint64_t value_storage_offset;   // Variable-length value storage offset
    
    // File statistics
    uint64_t creation_timestamp;     // File creation time
    uint64_t total_pages;            // Total number of 4KB pages
    uint32_t compression_type;       // Compression algorithm used
    
    // Checksum and padding
    uint64_t header_checksum;        // Header integrity check
    uint8_t  reserved[4000];         // Reserved for future use
};
```

### Column Metadata Structure
```c
struct ColumnMetadata {
    // Column identification
    char     column_name[64];        // Column name (null-terminated)
    uint8_t  data_type;              // Data type enum
    uint8_t  is_nullable;            // Nullable flag
    uint16_t flags;                  // Additional flags
    
    // B+ Tree information
    uint64_t root_page_id;           // Root page of B+ tree
    uint32_t tree_height;            // Height of B+ tree
    uint64_t total_keys;             // Total number of unique keys
    
    // Statistics
    uint64_t null_count;             // Number of null values
    uint64_t distinct_count;         // Number of distinct values
    uint64_t min_value_offset;       // Offset to minimum value
    uint64_t max_value_offset;       // Offset to maximum value
    
    // Storage information
    uint64_t total_size_bytes;       // Total storage used for column
    uint64_t bitmap_pages_count;     // Number of bitmap pages
    uint8_t  average_key_size;       // Average key size in bytes
    
    uint8_t  reserved[32];           // Reserved for future use
};
```

## 🗂️ Page Management

### Page Types
```c
enum PageType {
    PAGE_TYPE_FILE_HEADER    = 0x01,
    PAGE_TYPE_BTREE_INTERNAL = 0x02,
    PAGE_TYPE_BTREE_LEAF     = 0x03,
    PAGE_TYPE_ROARING_BITMAP = 0x04,
    PAGE_TYPE_STRING_SEGMENT = 0x05,
    PAGE_TYPE_BINARY_SEGMENT = 0x06,
    PAGE_TYPE_PAGE_DIRECTORY = 0x07
};
```

### Standard Page Header (64 bytes)
```c
struct PageHeader {
    uint8_t  page_type;              // Page type from enum above
    uint8_t  compression_type;       // Compression used for this page
    uint16_t flags;                  // Page-specific flags
    uint32_t data_size;              // Size of actual data in page
    
    uint64_t page_id;                // Unique page identifier
    uint64_t parent_page_id;         // Parent page (for B+ tree)
    uint64_t next_page_id;           // Next page in sequence (for leaf pages)
    uint64_t prev_page_id;           // Previous page in sequence
    
    uint32_t entry_count;            // Number of entries in page
    uint32_t free_space;             // Available free space in bytes
    
    uint64_t checksum;               // Page integrity checksum
    uint8_t  reserved[16];           // Reserved for future use
};
```

### Page Size Calculations
```
Fixed Page Size: 4096 bytes (4KB)
Page Header:     64 bytes
Available Data:  4032 bytes

Fanout Calculations:
- Internal Node: (4032 bytes) / (32-byte key + 8-byte page_id) = ~100 children
- Leaf Node:     (4032 bytes) / (32-byte key + 8-byte value) = ~100 entries
- Bitmap Page:   4032 bytes available for RoaringBitmap data
```

## 🌳 B+ Tree Implementation

### Tree Structure Design

#### Internal Node Structure
```c
struct BTreeInternalNode {
    PageHeader header;               // Standard page header (64 bytes)
    
    // Node-specific data
    uint32_t key_count;              // Number of keys in this node
    uint8_t  level;                  // Level in tree (0 = leaf)
    uint8_t  reserved[3];
    
    // Key/Pointer pairs (variable length)
    struct {
        uint8_t  key_data[32];       // Key data (fixed or variable)
        uint64_t child_page_id;      // Child page identifier
    } entries[MAX_INTERNAL_ENTRIES];
    
    uint8_t free_space[REMAINING_SPACE];
};
```

#### Leaf Node Structure
```c
struct BTreeLeafNode {
    PageHeader header;               // Standard page header (64 bytes)
    
    // Leaf-specific data
    uint32_t key_count;              // Number of keys in this leaf
    uint64_t bitmap_flags;           // Bit flags indicating which keys use bitmaps
    
    // Key/Value pairs
    struct {
        uint8_t  key_data[32];       // Key data
        uint64_t value_or_offset;    // Row number OR bitmap page offset
    } entries[MAX_LEAF_ENTRIES];
    
    uint8_t free_space[REMAINING_SPACE];
};
```

### Key Design Patterns

#### Fixed-Length Key Storage
```c
// For primitive types (int32, int64, float64, bool)
struct FixedLengthKey {
    union {
        int32_t  int32_value;
        int64_t  int64_value;
        double   double_value;
        bool     bool_value;
    } data;
    uint8_t type_tag;                // Type identifier
    uint8_t padding[7];              // Align to 8 bytes
};
```

#### Variable-Length Key Storage
```c
// For strings and binary data
struct VariableLengthKey {
    uint32_t hash_prefix;            // Hash of full key for quick comparison
    uint32_t full_key_offset;        // Offset to full key in value storage
    uint32_t key_length;             // Length of full key
    uint8_t  key_prefix[20];         // First 20 bytes of key for comparison
};
```

### Value Storage Strategy

#### Bit Flag Logic for Values
```
Value Field (64 bits):
┌─┬─────────────────────────────────────────────────────────────┐
│F│                    Value (63 bits)                         │
└─┴─────────────────────────────────────────────────────────────┘

F = Flag bit:
  0 = Single value (value is row number: int64)
  1 = Multiple values (value is bitmap page offset)

Examples:
Single value:  0|0000000000000000000000000000000000000000000000000042
               ↑ Flag=0, Row number = 42

Multiple values: 1|0000000000000000000000000000000000000000000000001024  
                 ↑ Flag=1, Bitmap at page 1024
```

## 📊 Data Type Strategies

### Fixed-Length Data Types

#### Integer Types (int32, int64)
```c
// Storage in B+ tree leaf
struct IntegerEntry {
    int64_t key_value;               // The actual integer value
    uint64_t value_or_bitmap;        // Row number or bitmap offset
};

// Optimization: Direct key comparison
int compare_integer_keys(int64_t a, int64_t b) {
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}
```

#### Floating Point Types (float32, float64)
```c
struct FloatEntry {
    double key_value;                // IEEE 754 double precision
    uint64_t value_or_bitmap;        // Row number or bitmap offset
};

// Special handling for NaN, infinity
int compare_float_keys(double a, double b) {
    if (isnan(a) && isnan(b)) return 0;
    if (isnan(a)) return 1;          // NaN sorts after everything
    if (isnan(b)) return -1;
    return (a < b) ? -1 : (a > b) ? 1 : 0;
}
```

#### Boolean Type
```c
struct BooleanEntry {
    bool key_value;                  // true or false
    uint64_t value_or_bitmap;        // Row number or bitmap offset
};

// Simple comparison (false < true)
int compare_boolean_keys(bool a, bool b) {
    return (int)a - (int)b;
}
```

### Variable-Length Data Types

#### String Type - Optimized Offset-Based Design

**Key Innovation**: Instead of storing string content in B+ tree nodes, we store unique strings in a compact segment and use their offsets as B+ tree keys. This provides:
- **Space Efficiency**: No duplicate strings stored
- **Cache Efficiency**: Smaller B+ tree nodes
- **Comparison Optimization**: Direct offset comparison with lazy string loading

```c
// B+ tree stores only offsets for string columns
struct StringBTreeEntry {
    uint64_t string_offset;          // Offset into string segment (this is the key)
    uint64_t value_or_bitmap;        // Row number or bitmap offset
};

// Compact string segment - no duplicates
struct StringSegment {
    PageHeader header;               // Standard page header (64 bytes)
    
    uint32_t string_count;           // Number of unique strings
    uint32_t total_size;             // Total size of string data
    uint64_t next_segment_page;      // For large string collections
    
    // String directory for fast access
    struct StringEntry {
        uint64_t offset;             // Offset within this segment
        uint32_t length;             // String length
        uint32_t hash;               // String hash for quick comparison
    } directory[MAX_STRINGS_PER_PAGE];
    
    // Packed string data
    uint8_t string_data[REMAINING_SPACE];
};

// Custom comparator for offset-based keys
struct StringOffsetComparator {
    StringSegment* segment;
    
    int compare(uint64_t offset_a, uint64_t offset_b) {
        if (offset_a == offset_b) return 0;
        
        // Load strings only when needed
        const char* str_a = load_string_at_offset(segment, offset_a);
        const char* str_b = load_string_at_offset(segment, offset_b);
        
        return strcmp(str_a, str_b);
    }
};
```

#### String Segment Benefits

**Storage Efficiency**:
```
Traditional Approach:
- 1M rows with 100 unique strings
- Each tree node stores full string data
- Total storage: 1M × average_string_size

Optimized Approach:
- Same 1M rows, 100 unique strings
- Tree nodes store only 8-byte offsets
- String segment: 100 × average_string_size
- Total storage: 1M × 8 bytes + 100 × average_string_size
- Reduction: ~90%+ for high duplicate scenarios
```

**Query Performance**:
```c
// Range query optimization
std::vector<uint64_t> range_query_strings(const std::string& min_str, const std::string& max_str) {
    // 1. Find offset range in string segment
    uint64_t min_offset = find_string_offset(min_str);
    uint64_t max_offset = find_string_offset(max_str);
    
    // 2. Use offset range for B+ tree scan
    return btree.range_scan(min_offset, max_offset);
    
    // Benefits:
    // - No string comparisons during tree traversal
    // - Smaller tree nodes = better cache utilization
    // - Fast integer comparisons until final result
}
```

#### Binary Data Type - Similar Offset-Based Design

```c
// B+ tree stores only offsets for binary columns
struct BinaryBTreeEntry {
    uint64_t binary_offset;          // Offset into binary segment (this is the key)
    uint64_t value_or_bitmap;        // Row number or bitmap offset
};

// Compact binary segment
struct BinarySegment {
    PageHeader header;               // Standard page header (64 bytes)
    
    uint32_t binary_count;           // Number of unique binary values
    uint32_t total_size;             // Total size of binary data
    uint64_t next_segment_page;      // For large binary collections
    
    // Binary directory
    struct BinaryEntry {
        uint64_t offset;             // Offset within this segment
        uint32_t length;             // Binary data length
        uint32_t hash;               // Hash for quick comparison
        uint8_t  prefix[8];          // First 8 bytes for comparison
    } directory[MAX_BINARIES_PER_PAGE];
    
    // Packed binary data
    uint8_t binary_data[REMAINING_SPACE];
};

// Custom comparator for binary offsets
struct BinaryOffsetComparator {
    BinarySegment* segment;
    
    int compare(uint64_t offset_a, uint64_t offset_b) {
        if (offset_a == offset_b) return 0;
        
        // Use hash and prefix for quick comparison
        BinaryEntry* entry_a = find_binary_entry(segment, offset_a);
        BinaryEntry* entry_b = find_binary_entry(segment, offset_b);
        
        if (entry_a->hash != entry_b->hash) {
            return (entry_a->hash < entry_b->hash) ? -1 : 1;
        }
        
        // Compare prefixes first
        int prefix_cmp = memcmp(entry_a->prefix, entry_b->prefix, 8);
        if (prefix_cmp != 0) return prefix_cmp;
        
        // Full comparison only if needed
        const uint8_t* data_a = load_binary_at_offset(segment, offset_a);
        const uint8_t* data_b = load_binary_at_offset(segment, offset_b);
        
        size_t min_len = std::min(entry_a->length, entry_b->length);
        int result = memcmp(data_a, data_b, min_len);
        
        if (result == 0) {
            return (entry_a->length < entry_b->length) ? -1 : 
                   (entry_a->length > entry_b->length) ? 1 : 0;
        }
        return result;
    }
};
```

### String Segment Management

#### Build Process for String Columns
```c
class StringSegmentBuilder {
private:
    std::unordered_map<std::string, uint64_t> string_to_offset;
    std::vector<std::string> unique_strings;
    uint64_t current_offset;
    
public:
    // Phase 1: Collect unique strings during data scan
    uint64_t add_string(const std::string& str) {
        auto it = string_to_offset.find(str);
        if (it != string_to_offset.end()) {
            return it->second;  // Return existing offset
        }
        
        // New unique string
        uint64_t offset = current_offset;
        string_to_offset[str] = offset;
        unique_strings.push_back(str);
        current_offset += str.length() + sizeof(uint32_t); // +4 for length prefix
        
        return offset;
    }
    
    // Phase 2: Build sorted string segment
    StringSegment* build_segment() {
        // Sort strings for optimal B+ tree construction
        std::sort(unique_strings.begin(), unique_strings.end());
        
        // Rebuild offsets after sorting
        rebuild_sorted_offsets();
        
        // Create segment pages
        return create_string_segment_pages();
    }
    
    // Phase 3: Build B+ tree with sorted offsets
    void build_btree(BPlusTree<uint64_t, uint64_t>* tree, 
                     const std::vector<std::pair<std::string, std::vector<uint64_t>>>& string_to_rows) {
        std::vector<std::pair<uint64_t, uint64_t>> sorted_entries;
        
        for (const auto& [str, row_numbers] : string_to_rows) {
            uint64_t string_offset = string_to_offset[str];
            
            // Handle duplicates with bitmap storage
            if (row_numbers.size() == 1) {
                sorted_entries.emplace_back(string_offset, row_numbers[0]);
            } else {
                uint64_t bitmap_offset = create_bitmap(row_numbers);
                sorted_entries.emplace_back(string_offset, bitmap_offset | BITMAP_FLAG);
            }
        }
        
        // Bulk load B+ tree
        tree->bulk_load(sorted_entries);
    }
};
```

#### Query Processing with String Segments
```c
class StringColumnQuery {
private:
    StringSegment* segments;
    BPlusTree<uint64_t, uint64_t>* btree;
    StringOffsetComparator comparator;
    
public:
    // Exact match query
    std::vector<uint64_t> find_exact(const std::string& target) {
        // 1. Find target string offset in segments
        uint64_t target_offset = find_string_offset_in_segments(target);
        if (target_offset == INVALID_OFFSET) {
            return {}; // String doesn't exist
        }
        
        // 2. Query B+ tree using offset
        return btree->find(target_offset);
    }
    
    // Range query optimization
    std::vector<uint64_t> find_range(const std::string& min_str, const std::string& max_str) {
        // 1. Find offset bounds in string segments
        uint64_t min_offset = find_string_offset_in_segments(min_str);
        uint64_t max_offset = find_string_offset_in_segments(max_str);
        
        if (min_offset == INVALID_OFFSET) {
            min_offset = find_next_greater_offset(min_str);
        }
        if (max_offset == INVALID_OFFSET) {
            max_offset = find_next_smaller_offset(max_str);
        }
        
        // 2. Efficient range scan using integer offsets
        return btree->range_search(min_offset, max_offset);
    }
    
    // Prefix query (LIKE 'prefix%')
    std::vector<uint64_t> find_prefix(const std::string& prefix) {
        // Calculate prefix bounds
        std::string max_prefix = prefix;
        max_prefix.back()++; // Increment last character
        
        return find_range(prefix, max_prefix);
    }
};

### Null Value Handling
```c
// Special null handling in B+ tree
#define NULL_KEY_MARKER 0xFFFFFFFFFFFFFFFF

// Nulls sort before all other values
// Separate bitmap tracks which rows are null for each column
struct NullBitmap {
    uint64_t null_bitmap_page;       // Page containing null row bitmap
    uint64_t null_count;             // Number of null values
};
```

## 🎯 RoaringBitmap Integration

### Bitmap Page Structure
```c
struct RoaringBitmapPage {
    PageHeader header;               // Standard page header (64 bytes)
    
    // Roaring bitmap metadata
    uint32_t cardinality;            // Number of set bits
    uint32_t container_count;        // Number of containers
    uint8_t  compression_level;      // Compression level used
    uint8_t  reserved[3];
    
    // Roaring bitmap data (4032 - 16 = 4016 bytes available)
    uint8_t  roaring_data[4016];
};
```

### Bitmap Threshold Strategy
```c
// Decision logic for when to use bitmaps
struct BitmapThreshold {
    static const uint32_t SINGLE_VALUE_THRESHOLD = 1;
    static const uint32_t SMALL_ARRAY_THRESHOLD = 4;
    static const uint32_t BITMAP_THRESHOLD = 5;
};

// Threshold decision algorithm
enum ValueStorageType {
    STORAGE_DIRECT_VALUE,    // Single row number
    STORAGE_SMALL_ARRAY,     // Small array in bitmap page
    STORAGE_ROARING_BITMAP   // Compressed roaring bitmap
};

ValueStorageType decide_storage_type(uint32_t value_count) {
    if (value_count == 1) {
        return STORAGE_DIRECT_VALUE;
    } else if (value_count <= SMALL_ARRAY_THRESHOLD) {
        return STORAGE_SMALL_ARRAY;
    } else {
        return STORAGE_ROARING_BITMAP;
    }
}
```

### Bitmap Operations for Queries
```c
// Fast set operations using RoaringBitmaps
class BitmapQueryProcessor {
public:
    // Single column predicates
    RoaringBitmap* evaluate_equals(Column* col, Value key);
    RoaringBitmap* evaluate_range(Column* col, Value min, Value max);
    RoaringBitmap* evaluate_in_list(Column* col, Value* keys, size_t count);
    
    // Multi-column operations
    RoaringBitmap* and_operation(RoaringBitmap* a, RoaringBitmap* b);
    RoaringBitmap* or_operation(RoaringBitmap* a, RoaringBitmap* b);
    RoaringBitmap* not_operation(RoaringBitmap* bitmap, uint64_t total_rows);
    
    // Result extraction
    std::vector<uint64_t> extract_row_numbers(RoaringBitmap* result);
};
```

## 🔍 Query Processing

### Query Types and Optimization

#### Point Lookups (WHERE column = value)
```c
Algorithm: point_lookup(column, target_value)
1. Load column metadata and tree root page
2. Traverse B+ tree to find leaf page containing key
   - O(log n) page reads
   - Binary search within each page
3. Check bit flag in leaf entry:
   - If flag = 0: Return single row number
   - If flag = 1: Load bitmap page and return all set bits
4. Return result set

Time Complexity: O(log n + k) where k = number of matching rows
Space Complexity: O(k) for result bitmap
```

#### Range Queries (WHERE column BETWEEN min AND max)
```c
Algorithm: range_scan(column, min_value, max_value)
1. Find leaf page containing min_value (O(log n))
2. Scan sequentially through leaf pages until max_value
   - Use next_page_id pointers for efficient traversal
   - For each matching key, collect row numbers or bitmaps
3. Aggregate all results into final bitmap
4. Return unified bitmap

Optimizations:
- Early termination when key > max_value
- Batch bitmap operations for efficiency
- Prefetch next leaf pages for sequential access
```

#### Multi-Column Filtering (WHERE col1 = x AND col2 > y)
```c
Algorithm: multi_column_filter(predicates[])
1. Process each column predicate independently:
   - Execute point lookups or range scans
   - Generate bitmap for each predicate
2. Combine bitmaps using boolean operations:
   - AND operation for conjunctive predicates
   - OR operation for disjunctive predicates
   - NOT operation for negated predicates
3. Return final bitmap representing matching rows

Parallelization:
- Process column predicates in parallel
- Use thread-safe bitmap operations
- Minimize memory allocation during operations
```

### Query Optimization Strategies

#### Predicate Ordering
```c
// Order predicates by selectivity (most selective first)
struct PredicateStats {
    double selectivity;              // Estimated selectivity (0.0 to 1.0)
    uint64_t estimated_cost;         // Cost estimate for execution
    bool has_index;                  // Whether column has B+ tree index
};

// Optimization: Execute most selective predicates first
std::vector<Predicate> optimize_predicate_order(std::vector<Predicate> predicates) {
    std::sort(predicates.begin(), predicates.end(),
        [](const Predicate& a, const Predicate& b) {
            return a.stats.selectivity < b.stats.selectivity;
        });
    return predicates;
}
```

#### Statistics-Based Optimization
```c
struct ColumnStatistics {
    uint64_t total_rows;             // Total number of rows
    uint64_t distinct_values;        // Number of distinct values
    uint64_t null_count;             // Number of null values
    double average_key_size;         // Average key size in bytes
    
    // Min/max values for range estimation
    Value min_value;
    Value max_value;
    
    // Histogram for selectivity estimation
    std::vector<HistogramBucket> histogram;
};

// Use statistics for cost estimation
double estimate_selectivity(ColumnStatistics* stats, Predicate predicate) {
    switch (predicate.type) {
        case EQUALS:
            return 1.0 / stats->distinct_values;
        case RANGE:
            return estimate_range_selectivity(stats, predicate.min, predicate.max);
        case IN_LIST:
            return predicate.value_count / (double)stats->distinct_values;
        default:
            return 0.5; // Default estimate
    }
}
```

### String Optimization Performance Analysis

#### Storage Efficiency Comparison
```
Dataset: 10M rows, 1K unique strings, average string length 50 bytes

Traditional Approach:
- B+ tree nodes store full strings
- Total B+ tree storage: 10M × 50 bytes = 500 MB
- Tree height: log(10M) ≈ 24 levels
- Page cache efficiency: Poor (large nodes)

Optimized Offset Approach:
- String segment: 1K × 50 bytes = 50 KB (99.99% reduction)
- B+ tree storage: 10M × 8 bytes = 80 MB (84% reduction)
- Tree height: log(10M) ≈ 24 levels (same)
- Page cache efficiency: Excellent (small nodes)

Total Reduction: ~500 MB → ~80 MB (84% overall reduction)
```

#### Query Performance Benefits
```
String Equality Query (WHERE name = 'John'):
Traditional: O(log n) with string comparisons at each level
Optimized:   O(log k + log n) where k = unique strings (k << n)
            - First find string offset: O(log k)
            - Then B+ tree lookup: O(log n) with integer comparisons
            - Benefit: Integer comparisons ~10x faster than string comparisons

String Range Query (WHERE name BETWEEN 'A' AND 'M'):
Traditional: O(log n + result_size) with string comparisons
Optimized:   O(log k + log n + result_size) with mostly integer comparisons
            - Benefit: Range bounds calculated once in string segment
            - Tree traversal uses fast integer offset comparisons

String Prefix Query (WHERE name LIKE 'John%'):
Traditional: Full tree scan with string prefix matching
Optimized:   Range query using offset bounds
            - Calculate offset range for prefix in string segment
            - Use efficient B+ tree range scan
            - Benefit: O(log n) vs O(n) complexity
```

#### Memory Access Patterns
```
Cache Locality Benefits:
- B+ tree nodes: 8-byte offsets vs 50-byte strings
- Nodes per page: ~500 offsets vs ~80 strings
- Tree height: Same, but faster traversal
- String segment: Sequential access, excellent cache locality

Memory Usage:
- Working set: Only active tree nodes + small string segment
- String segment: Loaded once, stays in cache
- Result: Better memory efficiency and lower latency
```

## 🛠️ Implementation Plan

### Phase 1: Core Infrastructure (Weeks 1-4)

#### Page Management System
```c
class PageManager {
private:
    static const size_t PAGE_SIZE = 4096;
    std::fstream file_handle;
    std::unordered_map<uint64_t, std::shared_ptr<Page>> page_cache;
    
public:
    // Core operations
    std::shared_ptr<Page> read_page(uint64_t page_id);
    uint64_t write_page(const Page& page);
    void flush_all_pages();
    
    // Cache management
    void set_cache_size(size_t max_pages);
    void evict_page(uint64_t page_id);
    
    // File operations
    bool open_file(const std::string& filename, bool create_new);
    void close_file();
    uint64_t get_file_size();
};
```

#### File Header and Metadata
```c
class FileMetadata {
private:
    FileHeader header;
    std::vector<ColumnMetadata> columns;
    
public:
    // File operations
    bool create_new_file(const std::string& filename);
    bool open_existing_file(const std::string& filename);
    
    // Column management
    void add_column(const std::string& name, DataType type);
    ColumnMetadata* get_column(const std::string& name);
    
    // Statistics
    void update_statistics(const std::string& column, const ColumnStatistics& stats);
    ColumnStatistics get_statistics(const std::string& column);
};
```

### Phase 2: B+ Tree Implementation (Weeks 5-8)

#### Generic B+ Tree
```c
template<typename KeyType, typename ValueType>
class BPlusTree {
private:
    uint64_t root_page_id;
    PageManager* page_manager;
    KeyComparator<KeyType> comparator;
    
public:
    // Search operations
    std::vector<ValueType> find(const KeyType& key);
    std::vector<ValueType> range_search(const KeyType& min, const KeyType& max);
    
    // Bulk loading (for immutable design)
    void bulk_load(std::vector<std::pair<KeyType, ValueType>>& sorted_data);
    
    // Iterator support
    class Iterator {
        // Implementation for range scans
    };
    Iterator begin();
    Iterator end();
    Iterator lower_bound(const KeyType& key);
    Iterator upper_bound(const KeyType& key);
};
```

#### Type-Specific Implementations
```c
// Specialized implementations for different data types
using IntegerBTree = BPlusTree<int64_t, uint64_t>;
using DoubleBTree = BPlusTree<double, uint64_t>;
using StringBTree = BPlusTree<std::string, uint64_t>;
using BinaryBTree = BPlusTree<std::vector<uint8_t>, uint64_t>;

// Key comparators
template<>
struct KeyComparator<double> {
    int operator()(const double& a, const double& b) const {
        // Handle NaN and infinity cases
        if (std::isnan(a) && std::isnan(b)) return 0;
        if (std::isnan(a)) return 1;
        if (std::isnan(b)) return -1;
        return (a < b) ? -1 : (a > b) ? 1 : 0;
    }
};
```

### Phase 3: RoaringBitmap Integration (Weeks 9-10)

#### Bitmap Management
```c
class BitmapManager {
private:
    PageManager* page_manager;
    std::unordered_map<uint64_t, roaring::Roaring> bitmap_cache;
    
public:
    // Bitmap operations
    uint64_t store_bitmap(const roaring::Roaring& bitmap);
    roaring::Roaring load_bitmap(uint64_t page_id);
    
    // Set operations
    roaring::Roaring intersect(const std::vector<uint64_t>& bitmap_pages);
    roaring::Roaring union_bitmaps(const std::vector<uint64_t>& bitmap_pages);
    roaring::Roaring negate_bitmap(uint64_t bitmap_page, uint64_t total_rows);
    
    // Utility functions
    std::vector<uint64_t> extract_row_numbers(const roaring::Roaring& bitmap);
    uint64_t count_set_bits(uint64_t bitmap_page);
};
```

#### Value Storage Decision Logic
```c
class ValueStorageManager {
public:
    struct StorageDecision {
        enum Type { DIRECT, SMALL_ARRAY, ROARING_BITMAP } type;
        uint64_t storage_location;
        uint32_t value_count;
    };
    
    StorageDecision decide_storage(const std::vector<uint64_t>& row_numbers) {
        StorageDecision decision;
        decision.value_count = row_numbers.size();
        
        if (row_numbers.size() == 1) {
            decision.type = StorageDecision::DIRECT;
            decision.storage_location = row_numbers[0];
        } else if (row_numbers.size() <= 4) {
            decision.type = StorageDecision::SMALL_ARRAY;
            decision.storage_location = store_small_array(row_numbers);
        } else {
            decision.type = StorageDecision::ROARING_BITMAP;
            roaring::Roaring bitmap(row_numbers.size(), row_numbers.data());
            decision.storage_location = bitmap_manager->store_bitmap(bitmap);
        }
        
        return decision;
    }
};
```

### Phase 4: Query Engine Integration (Weeks 11-12)

#### Query Processor
```c
class ColumnarQueryProcessor {
private:
    std::vector<std::unique_ptr<BPlusTree>> column_indexes;
    BitmapManager bitmap_manager;
    
public:
    // Query execution
    roaring::Roaring execute_predicate(const Predicate& pred);
    roaring::Roaring execute_query(const Query& query);
    
    // Optimization
    std::vector<Predicate> optimize_predicates(std::vector<Predicate> predicates);
    double estimate_query_cost(const Query& query);
    
    // Result materialization
    std::vector<Row> materialize_results(const roaring::Roaring& row_bitmap,
                                       const std::vector<std::string>& columns);
};
```

## 📈 Performance Characteristics

### Time Complexity Analysis

| Operation | Time Complexity | Description |
|-----------|----------------|-------------|
| Point Lookup | O(log n) | B+ tree traversal to leaf |
| Range Query | O(log n + k) | Find start + sequential scan |
| Multi-Column AND | O(m × log n + k) | m predicates + bitmap intersection |
| Multi-Column OR | O(m × log n + k) | m predicates + bitmap union |
| Insert (Bulk) | O(n log n) | Sort + bottom-up tree construction |

### Space Complexity Analysis

| Component | Space Usage | Notes |
|-----------|-------------|-------|
| B+ Tree Structure | ~10-15% overhead | Internal nodes + metadata |
| RoaringBitmaps | 90%+ compression | For duplicate values |
| Page Headers | ~1.6% overhead | 64 bytes per 4KB page |
| Value Storage | Variable | Depends on variable-length data |

### I/O Performance Characteristics

#### Read Performance
```
Sequential Range Scan:
- Leaf pages are linked for efficient traversal
- 4KB reads align with OS page size
- Prefetching opportunities for range queries

Random Point Lookups:
- Typically 3-4 page reads for tree traversal
- Page cache reduces repeated I/O
- Memory-mapped files for hot data
```

#### Cache Efficiency
```
Page-Level Caching:
- LRU eviction policy
- 4KB pages fit standard cache lines
- Separate cache pools for different page types

Buffer Pool Management:
- Dedicated pools for tree pages vs bitmap pages
- Adaptive sizing based on workload
- Write-ahead logging for consistency
```

### Compression Effectiveness

#### RoaringBitmap Compression
```
Typical Compression Ratios:
- Sparse bitmaps: 95%+ compression
- Dense bitmaps: 50-80% compression
- Sequential patterns: 99%+ compression

Memory Usage:
- In-memory: Compressed representation
- On-disk: Additional page-level compression
- Cache: Decompressed for fast access
```

## ⚖️ Trade-offs & Design Decisions

### Design Decision Matrix

| Aspect | Decision | Alternative | Rationale |
|--------|----------|-------------|-----------|
| **Page Size** | 4KB Fixed | Variable sizes | OS alignment, cache efficiency |
| **Tree Type** | B+ Tree | B-Tree, LSM | Range query optimization |
| **Mutability** | Immutable | Mutable | Simplified concurrency |
| **Bitmap Library** | RoaringBitmap | BitMagic, custom | Industry standard, compression |
| **Value Storage** | Row numbers | Inline values | Separation of concerns |

### Performance Trade-offs

#### Advantages Over Parquet
```
✅ Faster Point Lookups: O(log n) vs O(n) in row groups
✅ Better Range Queries: Natural B+ tree ordering
✅ Optimized Duplicates: RoaringBitmap compression
✅ Independent Columns: No row group dependencies
✅ Built-in Indexing: No external index files needed
```

#### Potential Disadvantages
```
❌ Write Performance: Tree construction vs append-only
❌ Storage Overhead: Tree structure vs packed data
❌ Memory Usage: Tree nodes vs streaming reads
❌ Complexity: More complex than row-based formats
```

### Concurrency Trade-offs

#### Single Writer Model
```
Advantages:
✅ No write conflicts or deadlocks
✅ Simplified consistency model
✅ Atomic file replacement for updates
✅ No complex locking mechanisms

Limitations:
❌ No concurrent writes
❌ Updates require full file rewrite
❌ Write latency for large datasets
```

#### Multi-Reader Model
```
Advantages:
✅ Unlimited concurrent readers
✅ No reader coordination needed
✅ Memory-mapped file sharing
✅ Consistent snapshots

Implementation:
- Memory-mapped files for efficiency
- Page-level caching across processes
- Copy-on-write for file updates
```

### Memory Management Strategy

#### Page Buffer Pool
```c
class BufferPool {
private:
    static const size_t DEFAULT_POOL_SIZE = 1024; // 4MB default
    std::unordered_map<uint64_t, Page*> cached_pages;
    std::list<uint64_t> lru_list;
    size_t max_pages;
    
public:
    Page* get_page(uint64_t page_id);
    void pin_page(uint64_t page_id);
    void unpin_page(uint64_t page_id);
    void evict_least_recently_used();
    
    // Statistics
    double get_hit_ratio();
    size_t get_memory_usage();
};
```

#### Memory-Mapped Files
```c
class MMapManager {
private:
    void* mapped_region;
    size_t file_size;
    int file_descriptor;
    
public:
    bool map_file(const std::string& filename);
    void unmap_file();
    Page* get_page_pointer(uint64_t page_id);
    
    // Advantages:
    // - OS handles caching
    // - Shared memory across processes
    // - Virtual memory benefits
};
```

## 🔮 Future Enhancements

### Planned Optimizations

#### Page-Level Filtering
```c
// Use ColumnIndex for page-level min/max filtering
struct PageStatistics {
    Value min_value;
    Value max_value;
    uint64_t null_count;
    uint64_t distinct_count;
};

// Skip pages that can't contain matching values
bool can_skip_page(const PageStatistics& stats, const Predicate& pred) {
    switch (pred.type) {
        case EQUALS:
            return pred.value < stats.min_value || pred.value > stats.max_value;
        case RANGE:
            return pred.max < stats.min_value || pred.min > stats.max_value;
        default:
            return false;
    }
}
```

#### Adaptive Compression
```c
// Choose compression based on data characteristics
enum CompressionType {
    NONE,           // No compression
    SNAPPY,         // Fast compression
    ZSTD,           // High compression ratio
    LZ4             // Fastest compression
};

CompressionType choose_compression(const PageStatistics& stats) {
    if (stats.distinct_count < 10) {
        return ZSTD;    // High compression for low cardinality
    } else if (stats.distinct_count > 1000) {
        return LZ4;     // Fast compression for high cardinality
    } else {
        return SNAPPY;  // Balanced choice
    }
}
```

#### Parallel Query Processing
```c
class ParallelQueryProcessor {
public:
    // Process multiple columns in parallel
    std::vector<roaring::Roaring> parallel_column_scan(
        const std::vector<Predicate>& predicates,
        size_t thread_count = std::thread::hardware_concurrency()
    );
    
    // Parallel bitmap operations
    roaring::Roaring parallel_bitmap_intersection(
        const std::vector<roaring::Roaring>& bitmaps,
        size_t thread_count
    );
};
```

### Research Directions

#### Machine Learning Integration
- **Learned Indexes**: Use ML models to predict key locations
- **Adaptive Indexing**: Learn query patterns and optimize accordingly
- **Compression Prediction**: ML-based compression algorithm selection

#### Advanced Storage Techniques
- **Delta Encoding**: Store differences for sorted data
- **Dictionary Encoding**: Shared dictionaries across pages
- **Bit Packing**: Pack multiple values into single integers

#### Distributed Extensions
- **Horizontal Partitioning**: Split large files across nodes
- **Query Federation**: Combine results from multiple files
- **Distributed Caching**: Coordinate page caches across cluster

---

**Document Maintenance**:
- Version: 1.0
- Last Updated: 2025-07-14
- Next Review: 2025-08-14
- Contact: ByteDB Development Team

This architecture document serves as the definitive specification for the ByteDB Columnar File Format and will be updated as the implementation evolves.