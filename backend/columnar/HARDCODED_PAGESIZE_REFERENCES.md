# Hardcoded PageSize References in ByteDB

## Overview
This document lists all the places where PageSize (4096) is still hardcoded in the codebase. These references prevent true dynamic page size support.

## Critical Hardcoded References

### 1. Constants Definition (`types.go`)
```go
const (
    PageSize       = 4096 // 4KB pages
    // ...
    MaxInternalEntries = (PageSize - PageHeaderSize - 8) / 40  // ~100 entries
    MaxLeafEntries     = (PageSize - PageHeaderSize - 16) / 16  // ~250 entries
    MaxStringsPerPage  = (PageSize - PageHeaderSize - 16) / 16  // ~250 strings
)
```
**Impact**: Core constant that affects entire system

### 2. Page Structure (`page.go`)
```go
type Page struct {
    Header PageHeader
    Data   [PageSize - PageHeaderSize]byte  // Fixed-size array!
}
```
**Impact**: Cannot have variable-sized pages with fixed array

### 3. Page I/O Operations (`page.go`)
```go
// In NewPage()
FreeSpace: PageSize - PageHeaderSize

// In Marshal()
buf := make([]byte, PageSize)

// In Unmarshal()
if len(buf) != PageSize {
    return fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(buf))
}

// In ReadPage()
offset := int64(pageID * PageSize)
buf := make([]byte, PageSize)
if n != PageSize {
    return nil, fmt.Errorf("incomplete page read: expected %d, got %d", PageSize, n)
}

// In WritePage()
offset := int64(page.Header.PageID * PageSize)
if n != PageSize {
    return fmt.Errorf("incomplete page write: expected %d, got %d", PageSize, n)
}

// Page count calculation
pm.nextPageID = uint64(info.Size() / PageSize)
```
**Impact**: All I/O assumes fixed 4KB pages

### 4. Bitmap Operations (`bitmap.go`)
```go
availableSpace := PageSize - PageHeaderSize - 16 // 16 bytes for metadata
```
**Impact**: Bitmap storage calculations assume fixed page size

### 5. String Segment (`string_segment.go`)
```go
entriesPerPage := (PageSize - PageHeaderSize - 8) / 16 // 16 bytes per entry
```
**Impact**: String directory calculations assume fixed page size

### 6. Test Files
Multiple test files reference PageSize for calculations and comparisons.

## Why These Can't Be Easily Changed

1. **Page Structure**: The `Page` struct uses a compile-time constant for the data array size. Go doesn't support variable-sized arrays in structs.

2. **File Format**: Changing PageSize would break compatibility with existing files unless we implement migration tools.

3. **Performance**: Many calculations using PageSize are compile-time optimized. Dynamic values would add runtime overhead.

4. **Memory Allocation**: Fixed-size pages allow for efficient memory pooling and allocation strategies.

## Current Workaround

We've implemented:
1. PageSize field in CompressionOptions (for configuration)
2. PageSize storage in file header (for future compatibility)
3. Validation to ensure only 4KB is used
4. Documentation of the limitation

## What Would Be Required for True Dynamic Page Size

1. **Change Page struct to use slices**:
   ```go
   type Page struct {
       Header PageHeader
       Data   []byte  // Dynamic slice instead of array
   }
   ```

2. **Pass page size to all functions**:
   - Every function that uses PageSize would need the value passed in
   - Or store it in a context/config object accessible everywhere

3. **Update all calculations**:
   - Replace compile-time constants with runtime calculations
   - Update all offset calculations to use dynamic page size

4. **Implement page size discovery**:
   - When opening a file, read the page size from header first
   - Use that for all subsequent operations

5. **Migration tools**:
   - Convert files between different page sizes
   - Handle mixed page sizes in same file (complex!)

## Recommendation

The current implementation correctly stores the page size configuration for future use, but the codebase is not yet ready for dynamic page sizes. This would require a major refactoring effort as outlined in the implementation plan.

For now:
- Configuration API is ready
- File format supports storing page size
- Validation prevents incompatible configurations
- Future versions can implement full support without breaking the file format