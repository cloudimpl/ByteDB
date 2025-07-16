# How Compression and Querying Work in ByteDB

## Overview
ByteDB uses page-level compression where data is compressed when written to disk pages and decompressed when read. This provides a balance between compression benefits and query performance.

## Compression Architecture

### 1. Page-Level Compression
```
┌─────────────────────────────────┐
│      Columnar File              │
├─────────────────────────────────┤
│  Header (uncompressed)          │
├─────────────────────────────────┤
│  Page 1: B-tree Index           │
│  ├─ Page Header (uncompressed)  │
│  └─ Page Data (compressed)      │
├─────────────────────────────────┤
│  Page 2: Data Page              │
│  ├─ Page Header (uncompressed)  │
│  └─ Page Data (compressed)      │
├─────────────────────────────────┤
│  Page 3: Bitmap Page            │
│  ├─ Page Header (uncompressed)  │
│  └─ Page Data (compressed)      │
└─────────────────────────────────┘
```

### 2. How Compression Works

#### Write Path:
1. **Data Collection**: Application writes data to columns
2. **Page Building**: Data is organized into 4KB pages
3. **Compression**: Before writing to disk, page data is compressed
4. **Storage**: Compressed data + uncompressed header written to file

```go
// Simplified compression flow
func (pm *PageManager) WritePage(page *Page) error {
    if pm.compressionEnabled {
        // Compress page data (but not header)
        compressed := pm.compressor.Compress(page.Data)
        
        // Write header (uncompressed) + compressed data
        writeHeader(page.Header)
        writeData(compressed)
    }
}
```

#### Read Path:
1. **Page Request**: Query requests specific page
2. **Load Page**: Read compressed data from disk
3. **Decompression**: Decompress page data in memory
4. **Query Processing**: Work with uncompressed data

```go
// Simplified decompression flow
func (pm *PageManager) ReadPage(pageID uint64) (*Page, error) {
    // Read header (always uncompressed)
    header := readHeader(pageID)
    
    if header.IsCompressed {
        // Read compressed data
        compressedData := readData(pageID)
        
        // Decompress to get original page data
        page.Data = pm.compressor.Decompress(compressedData)
    }
    return page
}
```

## Query Processing with Compression

### 1. B-Tree Index Queries

When you query for a specific value:

```
Query: WHERE id = 12345

Step 1: Navigate B-Tree Index
├─ Load root page (decompress if needed)
├─ Binary search to find child
├─ Load child page (decompress if needed)
└─ Continue until leaf page found

Step 2: Get Row Numbers
├─ Leaf page contains: value → bitmap of rows
└─ Return bitmap (e.g., rows {245, 1023, 4567})
```

### 2. How Queries Work with Compressed Pages

#### Example: Integer Column Query
```sql
SELECT * FROM table WHERE amount > 1000
```

**Process:**
1. **Index Traversal**:
   - Start at B-tree root page
   - Page is loaded and decompressed in memory
   - Binary search finds relevant child pages
   
2. **Range Scan**:
   - Load leaf pages covering range > 1000
   - Each page is decompressed once when loaded
   - Scan decompressed data to find matching values
   
3. **Bitmap Building**:
   - Collect row numbers into bitmap
   - Bitmap operations are on decompressed data

### 3. Why Compressed Queries Can Be Faster

Despite decompression overhead, compressed queries often perform better:

#### a) **Reduced I/O**
```
Uncompressed: Read 100MB from disk
Compressed:   Read 20MB from disk + decompress

If decompression is faster than reading 80MB from disk, 
compression wins!
```

#### b) **Better Cache Utilization**
- Compressed pages fit more data in OS page cache
- More data stays in memory = fewer disk reads

#### c) **CPU vs I/O Trade-off**
- Modern CPUs decompress very fast (especially Snappy/LZ4)
- Disk I/O is often the bottleneck
- Trading CPU cycles for less I/O improves performance

## Compression Algorithms Comparison

### 1. **Snappy** (Google's algorithm)
- **Speed**: Very fast compression/decompression
- **Ratio**: Moderate (40-60% reduction)
- **Best for**: High-throughput systems

```
Original data: [1,1,1,1,2,2,2,2,3,3,3,3]
Snappy:        [run:4x1][run:4x2][run:4x3]
```

### 2. **Gzip** (Deflate algorithm)
- **Speed**: Slow compression, moderate decompression
- **Ratio**: Good (70-80% reduction)
- **Best for**: Cold storage, archival data

```
Original data: "user_id_1, user_id_1, user_id_1"
Gzip:          [dictionary][references to patterns]
```

### 3. **Zstd** (Facebook's algorithm)
- **Speed**: Moderate compression, fast decompression
- **Ratio**: Excellent (80-90% reduction)
- **Best for**: Balanced performance and compression

## Real-World Example

Let's trace a query through the system:

```sql
-- Query: Find all transactions for user_id = 42
SELECT * FROM transactions WHERE user_id = 42
```

### Step-by-Step Execution:

1. **Query Planner**:
   - Identifies need to search user_id column
   - Locates B-tree index for user_id

2. **Index Root Page** (Page #5):
   ```
   [Header: Uncompressed]
   PageID: 5, Type: BTree, Compressed: true
   
   [Data: Compressed with Zstd]
   Compressed size: 1.2KB
   Decompressed size: 3.8KB
   Content: B-tree nodes with value ranges
   ```

3. **Decompression**:
   - Load 1.2KB from disk
   - Zstd decompress to 3.8KB in memory
   - Time: ~10 microseconds

4. **Binary Search**:
   - Search decompressed data for user_id = 42
   - Find pointer to leaf page #127

5. **Leaf Page** (Page #127):
   ```
   [Data: Compressed]
   Contains: user_id values 40-50
   Bitmap for user_id=42: {rows: 145, 892, 1456, 2301}
   ```

6. **Result**:
   - Return bitmap of matching rows
   - Total time: ~50 microseconds
   - Data read: 2.4KB (vs 7.6KB uncompressed)

## Performance Characteristics

### Write Performance Impact:
```
Operation          Uncompressed    Snappy    Gzip      Zstd
---------          ------------    ------    ----      ----
Write 1000 rows    17ms           52ms      1539ms    122ms
Overhead           -              3x        90x       7x
```

### Query Performance:
```
Operation          Uncompressed    Compressed (any)
---------          ------------    ----------------
Point Query        83μs            28-34μs
Range Query        1109μs          1060μs
Row Lookup         16μs            10μs
```

### Why Queries Are Faster with Compression:

1. **Less Data Transfer**:
   - Read 20% of data from disk
   - Decompression is faster than disk I/O

2. **Cache Efficiency**:
   - 5x more pages fit in memory
   - Reduced cache misses

3. **Sequential Access**:
   - Compressed pages = more sequential reads
   - Better disk throughput

## Best Practices

1. **Choose Compression Based on Workload**:
   - Write-heavy: Use Snappy or none
   - Balanced: Use Zstd
   - Read-heavy: Any compression works

2. **Consider Data Patterns**:
   - Sequential data compresses well
   - Random data compresses poorly
   - Low cardinality strings compress excellently

3. **Monitor Performance**:
   - Track compression ratios
   - Measure query latencies
   - Watch CPU usage during decompression

## Summary

The page-level compression in ByteDB provides:
- **Transparent operation**: Queries work identically on compressed/uncompressed data
- **Performance benefits**: Often faster queries due to reduced I/O
- **Flexibility**: Choose algorithm based on needs
- **Efficiency**: 80%+ data reduction with minimal query overhead

The key insight is that modern systems are often I/O bound, not CPU bound. By trading some CPU cycles for significantly less disk I/O, compression actually improves overall system performance while reducing storage costs.