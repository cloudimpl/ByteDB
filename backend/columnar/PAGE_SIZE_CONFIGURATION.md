# Page Size Configuration in ByteDB

## Overview

ByteDB now supports configurable page sizes in the compression options, allowing you to optimize for different workloads. However, **full dynamic page size support is not yet implemented**. Currently, only 4KB (4096 bytes) pages are supported.

## Current Implementation Status

### ✅ Completed
- Page size configuration in `CompressionOptions`
- `WithPageSize()` method for configuration
- Page size stored in file header for future compatibility
- Page size validation to prevent unsupported configurations
- Page size read from file header when opening files

### ❌ Not Yet Implemented
- Dynamic page allocation based on configured size
- Variable page I/O operations
- Dynamic calculations for entries per page
- Support for page sizes other than 4KB

## Usage

### Configure Page Size
```go
// Create compression options with page size
opts := NewCompressionOptions().
    WithPageSize(4096). // Only 4KB currently supported
    WithPageCompression(CompressionSnappy, CompressionLevelDefault)

// Create file with options
cf, err := CreateFileWithOptions("data.bytedb", opts)
if err != nil {
    // Will error if page size is not 4096
    log.Fatal(err)
}
```

### Available Page Sizes (Future)
When fully implemented, the following page sizes will be supported:
- 1KB (1,024 bytes) - Minimal memory usage
- 2KB (2,048 bytes) - Small datasets
- 4KB (4,096 bytes) - **Currently the only supported size**
- 8KB (8,192 bytes) - Balanced performance
- 16KB (16,384 bytes) - Recommended for most workloads
- 32KB (32,768 bytes) - High compression priority
- 64KB (65,536 bytes) - Maximum compression
- 128KB (131,072 bytes) - Extreme compression scenarios

### Validation
```go
opts := NewCompressionOptions().WithPageSize(16384)
if err := opts.ValidatePageSize(); err != nil {
    // Error: dynamic page size not yet supported
    fmt.Println(err)
}
```

## Benefits of Larger Page Sizes (When Implemented)

Based on our testing, larger page sizes provide significant compression benefits:

| Page Size | Compression Improvement | Use Case |
|-----------|------------------------|----------|
| 4KB | Baseline (0%) | Current default |
| 8KB | 5-15% better | Memory constrained |
| 16KB | 15-35% better | General purpose |
| 32KB | 25-50% better | Compression priority |
| 64KB | 30-65% better | Cold storage |

## Implementation Timeline

1. **Phase 1 (Complete)**: Configuration API and file header support
2. **Phase 2 (Planned)**: Infrastructure for dynamic pages
3. **Phase 3 (Future)**: Full dynamic page size implementation
4. **Phase 4 (Future)**: Migration tools for existing files

## Example: Future Usage

When fully implemented, you'll be able to optimize for different scenarios:

```go
// For high-compression cold storage
coldStorage := NewCompressionOptions().
    WithPageSize(64 * 1024).
    WithPageCompression(CompressionZstd, CompressionLevelBest)

// For balanced performance
balanced := NewCompressionOptions().
    WithPageSize(16 * 1024).
    WithPageCompression(CompressionSnappy, CompressionLevelDefault)

// For memory-constrained environments
lowMemory := NewCompressionOptions().
    WithPageSize(4 * 1024).
    WithPageCompression(CompressionGzip, CompressionLevelFastest)
```

## Current Limitations

1. Only 4KB pages are functional
2. Changing `PageSize` constant requires full recompilation
3. No migration path between different page sizes
4. All calculations assume 4KB pages

## Workaround

If you need better compression now:
1. Use more aggressive compression algorithms (Zstd, Gzip)
2. Enable compression on all page types
3. Consider pre-processing data before storage
4. Wait for full dynamic page size implementation

## Technical Details

The page size affects:
- Memory usage (buffer size per page)
- I/O operations (fewer with larger pages)
- Compression ratios (better with more data)
- Internal fragmentation (worse with larger pages)
- Cache efficiency (depends on access patterns)

## See Also

- [Page Size Implementation Plan](PAGE_SIZE_IMPLEMENTATION_PLAN.md)
- [Page Size Recommendations](PAGE_SIZE_RECOMMENDATIONS.md)
- [Compression and Query Explanation](COMPRESSION_QUERY_EXPLANATION.md)