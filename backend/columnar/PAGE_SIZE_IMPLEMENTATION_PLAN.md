# Page Size Configuration Implementation Plan

## Current State

The codebase currently uses a fixed PageSize constant of 4096 bytes throughout. While we've added PageSize to CompressionOptions and the file header, actually using dynamic page sizes requires significant refactoring.

## Implementation Challenges

### 1. Fixed Page Structure
```go
type Page struct {
    Header PageHeader
    Data   [PageSize - PageHeaderSize]byte  // Fixed-size array
}
```
This uses a compile-time constant, making dynamic sizing impossible without refactoring.

### 2. Widespread Constant Usage
The PageSize constant is used in:
- Page allocation and I/O operations
- Offset calculations (pageID * PageSize)
- Maximum entry calculations per page type
- String segment calculations
- Buffer allocations throughout the codebase

### 3. Breaking Changes
Changing to dynamic page sizes would:
- Require changing Page.Data to a slice
- Need page size passed to all functions
- Break binary compatibility with existing files
- Require updating all tests

## Recommended Implementation Approach

### Phase 1: Metadata Support (Current)
✅ Added PageSize to CompressionOptions
✅ Added WithPageSize() method  
✅ Store page size in file header
✅ Read page size from header on file open

### Phase 2: Infrastructure Preparation
1. Create a PageConfig struct to encapsulate page-related constants:
```go
type PageConfig struct {
    PageSize       int
    HeaderSize     int
    MaxInternal    int
    MaxLeaf        int
    MaxStrings     int
}
```

2. Add PageConfig to PageManager:
```go
type PageManager struct {
    // ... existing fields
    pageConfig PageConfig
}
```

3. Create page allocation functions that use dynamic sizes:
```go
func NewDynamicPage(pageType PageType, pageID uint64, pageSize int) *DynamicPage {
    return &DynamicPage{
        Header: PageHeader{...},
        Data:   make([]byte, pageSize-PageHeaderSize),
    }
}
```

### Phase 3: Gradual Migration
1. Create new interfaces that support dynamic page sizes
2. Implement parallel code paths for dynamic pages
3. Migrate one component at a time (e.g., start with new files)
4. Maintain backward compatibility for existing files

### Phase 4: Full Implementation
1. Replace all PageSize constant usages
2. Update all I/O operations to use dynamic sizes
3. Refactor entry calculations to be dynamic
4. Update all tests

## Interim Solution

For now, we can provide a "configuration-aware" implementation that:

1. Validates requested page size matches the constant:
```go
func (opts *CompressionOptions) ValidatePageSize() error {
    if opts.PageSize != PageSize {
        return fmt.Errorf("dynamic page size not yet supported: requested %d, system uses %d", 
            opts.PageSize, PageSize)
    }
    return nil
}
```

2. Documents the limitation clearly
3. Stores the page size in file header for future compatibility
4. Provides a migration path when dynamic support is added

## Code Changes for Interim Solution

### 1. Update CompressionOptions validation
### 2. Add validation in file creation
### 3. Document the limitation in comments
### 4. Add tests for page size validation

## Future Benefits

When fully implemented, dynamic page sizes will enable:
- 16KB pages: 15-35% better compression
- 32KB pages: 25-50% better compression
- Optimized configurations per workload
- Better I/O efficiency for large datasets

## Estimated Effort

- Phase 1: ✅ Complete
- Phase 2: 2-3 days
- Phase 3: 1-2 weeks
- Phase 4: 1 week + testing

Total: ~3-4 weeks for full implementation

## Recommendation

Given the significant refactoring required, I recommend:
1. Implementing the interim solution now
2. Planning the full implementation as a major version update
3. Providing clear documentation about current limitations
4. Ensuring forward compatibility with the file format