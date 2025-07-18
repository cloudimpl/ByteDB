package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// KeyComparator defines the interface for key comparison
type KeyComparator interface {
	Compare(a, b uint64) (int, error)
}

// IntComparator compares uint64 keys directly
type IntComparator struct{}

func (ic *IntComparator) Compare(a, b uint64) (int, error) {
	if a < b {
		return -1, nil
	} else if a > b {
		return 1, nil
	}
	return 0, nil
}

// GetComparatorForDataType returns the appropriate comparator for a data type
func GetComparatorForDataType(dataType DataType) KeyComparator {
	// For signed types with sort-preserving conversion, we can use the default comparator
	// because the conversion ensures correct ordering
	return &IntComparator{}
}

// BPlusTree represents a B+ tree optimized for columnar storage
type BPlusTree struct {
	pageManager   *PageManager
	bitmapManager *BitmapManager
	rootPageID    uint64
	dataType      DataType
	comparator    KeyComparator
	height        uint32
	
	// Child page mapping for space-efficient navigation
	// Maps internal page ID -> list of child page IDs
	childPageMap  map[uint64][]uint64
}

// NewBPlusTree creates a new B+ tree
func NewBPlusTree(pm *PageManager, dataType DataType, comparator KeyComparator) *BPlusTree {
	if comparator == nil {
		comparator = &IntComparator{}
	}
	
	return &BPlusTree{
		pageManager:   pm,
		bitmapManager: NewBitmapManager(pm),
		dataType:      dataType,
		comparator:    comparator,
		childPageMap:  make(map[uint64][]uint64),
	}
}

// BTreeInternalEntry represents an entry in an internal node (keys only, no child pointers)
type BTreeInternalEntry struct {
	Key uint64  // Only store keys, child pointers are calculated
}

// BTreeLeafEntry represents an entry in a leaf node
type BTreeLeafEntry struct {
	Key   uint64
	Value Value
}

// Find searches for a key and returns associated row numbers
func (bt *BPlusTree) Find(key uint64) ([]uint64, error) {
	if bt.rootPageID == 0 {
		return nil, nil // Empty tree
	}
	
	// Navigate to leaf
	leafPage, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	// Search within leaf
	entries, err := bt.readLeafEntries(leafPage)
	if err != nil {
		return nil, err
	}
	
	for _, entry := range entries {
		cmp, err := bt.comparator.Compare(entry.Key, key)
		if err != nil {
			return nil, err
		}
		
		if cmp == 0 {
			// Found the key
			if entry.Value.IsRowNumber() {
				return []uint64{entry.Value.GetRowNumber()}, nil
			} else {
				// Load bitmap
				return bt.loadBitmap(entry.Value.GetBitmapOffset())
			}
		} else if cmp > 0 {
			// Keys are sorted, so we've passed the target
			break
		}
	}
	
	return nil, nil // Not found
}

// RangeSearch finds all entries with keys in [minKey, maxKey]
func (bt *BPlusTree) RangeSearch(minKey, maxKey uint64) ([]uint64, error) {
	if bt.rootPageID == 0 {
		return nil, nil // Empty tree
	}
	
	results := make([]uint64, 0)
	
	// Navigate to first leaf containing minKey
	currentPage, err := bt.navigateToLeaf(minKey)
	if err != nil {
		return nil, err
	}
	
	// Scan leaves until we exceed maxKey
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			minCmp, err := bt.comparator.Compare(entry.Key, minKey)
			if err != nil {
				return nil, err
			}
			
			maxCmp, err := bt.comparator.Compare(entry.Key, maxKey)
			if err != nil {
				return nil, err
			}
			
			if minCmp >= 0 && maxCmp <= 0 {
				// Key is in range
				if entry.Value.IsRowNumber() {
					results = append(results, entry.Value.GetRowNumber())
				} else {
					// Load bitmap
					rows, err := bt.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					results = append(results, rows...)
				}
			} else if maxCmp > 0 {
				// We've passed the range
				return results, nil
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return results, nil
}

// RangeSearchGreaterThan finds all entries with key > value
func (bt *BPlusTree) RangeSearchGreaterThan(key uint64) ([]uint64, error) {
	// Find the leaf that would contain this key
	leaf, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	results := make([]uint64, 0)
	currentPage := leaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp > 0 {
				// Key is greater than value
				if entry.Value.IsRowNumber() {
					results = append(results, entry.Value.GetRowNumber())
				} else {
					// Load bitmap
					rows, err := bt.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					results = append(results, rows...)
				}
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return results, nil
}

// RangeSearchGreaterThanOrEqual finds all entries with key >= value
func (bt *BPlusTree) RangeSearchGreaterThanOrEqual(key uint64) ([]uint64, error) {
	// Find the leaf that would contain this key
	leaf, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	results := make([]uint64, 0)
	currentPage := leaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp >= 0 {
				// Key is greater than or equal to value
				if entry.Value.IsRowNumber() {
					results = append(results, entry.Value.GetRowNumber())
				} else {
					// Load bitmap
					rows, err := bt.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					results = append(results, rows...)
				}
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return results, nil
}

// RangeSearchLessThan finds all entries with key < value
func (bt *BPlusTree) RangeSearchLessThan(key uint64) ([]uint64, error) {
	// Start from the leftmost leaf
	currentPageID := bt.rootPageID
	
	// Navigate to leftmost leaf
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			break
		}
		
		// For internal nodes, get first child from childPageMap
		children, exists := bt.childPageMap[currentPageID]
		if !exists || len(children) == 0 {
			return nil, fmt.Errorf("no children found for internal page %d", currentPageID)
		}
		currentPageID = children[0]
	}
	
	results := make([]uint64, 0)
	
	// Scan leaves until we reach or exceed the key
	for currentPageID != 0 {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		entries, err := bt.readLeafEntries(page)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp < 0 {
				// Key is less than value
				if entry.Value.IsRowNumber() {
					results = append(results, entry.Value.GetRowNumber())
				} else {
					// Load bitmap
					rows, err := bt.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					results = append(results, rows...)
				}
			} else {
				// We've reached or exceeded the key
				return results, nil
			}
		}
		
		currentPageID = page.Header.NextPageID
	}
	
	return results, nil
}

// RangeSearchLessThanOrEqual finds all entries with key <= value
func (bt *BPlusTree) RangeSearchLessThanOrEqual(key uint64) ([]uint64, error) {
	// Start from the leftmost leaf
	currentPageID := bt.rootPageID
	
	// Navigate to leftmost leaf
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			break
		}
		
		// For internal nodes, get first child from childPageMap
		children, exists := bt.childPageMap[currentPageID]
		if !exists || len(children) == 0 {
			return nil, fmt.Errorf("no children found for internal page %d", currentPageID)
		}
		currentPageID = children[0]
	}
	
	results := make([]uint64, 0)
	
	// Scan leaves until we exceed the key
	for currentPageID != 0 {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		entries, err := bt.readLeafEntries(page)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp <= 0 {
				// Key is less than or equal to value
				if entry.Value.IsRowNumber() {
					results = append(results, entry.Value.GetRowNumber())
				} else {
					// Load bitmap
					rows, err := bt.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					results = append(results, rows...)
				}
			} else {
				// We've exceeded the key
				return results, nil
			}
		}
		
		currentPageID = page.Header.NextPageID
	}
	
	return results, nil
}

// BulkLoad creates a B+ tree from sorted key-value pairs
func (bt *BPlusTree) BulkLoad(entries []BTreeLeafEntry) error {
	if len(entries) == 0 {
		return nil
	}
	
	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		cmp, _ := bt.comparator.Compare(entries[i].Key, entries[j].Key)
		return cmp < 0
	})
	
	// Build leaves first
	leaves, err := bt.buildLeaves(entries)
	if err != nil {
		return err
	}
	
	// Build internal nodes bottom-up
	currentLevel := leaves
	height := uint32(1)
	
	for len(currentLevel) > 1 {
		nextLevel, err := bt.buildInternalLevel(currentLevel)
		if err != nil {
			return err
		}
		currentLevel = nextLevel
		height++
	}
	
	// Set root
	bt.rootPageID = currentLevel[0].Header.PageID
	bt.height = height
	
	return nil
}

// BulkLoadWithDuplicates creates a B+ tree handling duplicate keys with bitmaps
// Returns statistics calculated during tree construction for efficiency
func (bt *BPlusTree) BulkLoadWithDuplicates(keyRows []struct{Key uint64; RowNum uint64}) (*ColumnStats, error) {
	if len(keyRows) == 0 {
		return &ColumnStats{}, nil
	}
	
	// Initialize statistics
	stats := &ColumnStats{
		TotalKeys:     uint64(len(keyRows)),
		NullCount:     0, // No null support yet
		MinValue:      ^uint64(0), // Max uint64 value
		MaxValue:      0,
		AverageKeySize: uint8(GetDataTypeSize(bt.dataType)),
	}
	
	// Group by key and calculate min/max in single pass
	keyGroups := make(map[uint64][]uint64)
	for _, kr := range keyRows {
		keyGroups[kr.Key] = append(keyGroups[kr.Key], kr.RowNum)
		
		// Update min/max during grouping
		if kr.Key < stats.MinValue {
			stats.MinValue = kr.Key
		}
		if kr.Key > stats.MaxValue {
			stats.MaxValue = kr.Key
		}
	}
	
	// Set distinct count
	stats.DistinctCount = uint64(len(keyGroups))
	
	// Convert to leaf entries
	entries := make([]BTreeLeafEntry, 0, len(keyGroups))
	for key, rows := range keyGroups {
		var value Value
		
		if len(rows) == 1 {
			// Single row - store directly
			value = NewRowNumberValue(rows[0])
		} else {
			// Multiple rows - create bitmap
			_, bitmapPageID, err := bt.bitmapManager.CreateBitmapFromRows(rows)
			if err != nil {
				return nil, fmt.Errorf("failed to create bitmap for key %d: %w", key, err)
			}
			value = NewBitmapValue(bitmapPageID)
		}
		
		entries = append(entries, BTreeLeafEntry{Key: key, Value: value})
	}
	
	if err := bt.BulkLoad(entries); err != nil {
		return nil, err
	}
	
	return stats, nil
}

// buildLeaves creates leaf pages from sorted entries
func (bt *BPlusTree) buildLeaves(entries []BTreeLeafEntry) ([]*Page, error) {
	if bt.pageManager.compressionOptions != nil && 
	   bt.pageManager.compressionOptions.LeafPageCompression != CompressionNone {
		return bt.buildLeavesCompressed(entries)
	}
	return bt.buildLeavesUncompressed(entries)
}

// buildLeavesUncompressed is the original implementation
func (bt *BPlusTree) buildLeavesUncompressed(entries []BTreeLeafEntry) ([]*Page, error) {
	leaves := make([]*Page, 0)
	currentLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
	
	buf := bytes.NewBuffer(currentLeaf.Data[:0])
	entryCount := uint32(0)
	keySize := GetDataTypeSize(bt.dataType)
	
	for i, entry := range entries {
		// Calculate entry size
		entrySize := keySize + 8 // variable key size + 8 bytes value
		
		// Check if entry fits in current leaf
		if buf.Len()+entrySize > len(currentLeaf.Data) && entryCount > 0 {
			// Finalize current leaf
			currentLeaf.Header.EntryCount = entryCount
			currentLeaf.Header.DataSize = uint32(buf.Len())
			
			// Create new leaf before writing current one
			newLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
			currentLeaf.Header.NextPageID = newLeaf.Header.PageID
			newLeaf.Header.PrevPageID = currentLeaf.Header.PageID
			
			if err := bt.pageManager.WritePage(currentLeaf); err != nil {
				return nil, err
			}
			
			leaves = append(leaves, currentLeaf)
			
			currentLeaf = newLeaf
			buf = bytes.NewBuffer(currentLeaf.Data[:0])
			entryCount = 0
		}
		
		// Write entry to current leaf
		buf.Write(EncodeKey(entry.Key, bt.dataType))
		binary.Write(buf, ByteOrder, entry.Value.Data)
		entryCount++
		
		// Handle last entry
		if i == len(entries)-1 {
			currentLeaf.Header.EntryCount = entryCount
			currentLeaf.Header.DataSize = uint32(buf.Len())
			
			if err := bt.pageManager.WritePage(currentLeaf); err != nil {
				return nil, err
			}
			
			leaves = append(leaves, currentLeaf)
		}
	}
	
	return leaves, nil
}

// buildLeavesCompressed creates leaf pages with compression awareness
func (bt *BPlusTree) buildLeavesCompressed(entries []BTreeLeafEntry) ([]*Page, error) {
	leaves := make([]*Page, 0)
	currentLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
	
	buf := bytes.NewBuffer(currentLeaf.Data[:0])
	entryCount := uint32(0)
	
	// We'll allow buffer to grow beyond page size, then check compressed size
	tempBuf := new(bytes.Buffer)
	tempEntryCount := uint32(0)
	
	for i, entry := range entries {
		// Write entry to temp buffer
		tempBuf.Write(EncodeKey(entry.Key, bt.dataType))
		binary.Write(tempBuf, ByteOrder, entry.Value.Data)
		tempEntryCount++
		
		// Check both uncompressed and compressed size constraints
		// 1. Uncompressed data must fit in page (for decompression)
		// 2. Compressed data must fit in page (for storage)
		uncompressedSize := tempBuf.Len()
		compressedSize, _ := bt.pageManager.EstimateCompressedSize(PageTypeBTreeLeaf, tempBuf.Bytes())
		
		// If both sizes fit in page, update the actual buffer
		if uncompressedSize <= len(currentLeaf.Data) && compressedSize <= len(currentLeaf.Data) {
			// Copy temp buffer to page data
			copy(currentLeaf.Data[:], tempBuf.Bytes())
			buf = bytes.NewBuffer(currentLeaf.Data[:tempBuf.Len()])
			entryCount = tempEntryCount
		} else if entryCount > 0 {
			// Compressed data doesn't fit, finalize current page with previous data
			currentLeaf.Header.EntryCount = entryCount
			currentLeaf.Header.DataSize = uint32(buf.Len())
			
			// Create new leaf
			newLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
			currentLeaf.Header.NextPageID = newLeaf.Header.PageID
			newLeaf.Header.PrevPageID = currentLeaf.Header.PageID
			
			if err := bt.pageManager.WritePage(currentLeaf); err != nil {
				return nil, err
			}
			
			leaves = append(leaves, currentLeaf)
			
			// Start fresh with current entry
			currentLeaf = newLeaf
			tempBuf = new(bytes.Buffer)
			tempBuf.Write(EncodeKey(entry.Key, bt.dataType))
			binary.Write(tempBuf, ByteOrder, entry.Value.Data)
			tempEntryCount = 1
			
			// Copy temp buffer to page data
			copy(currentLeaf.Data[:], tempBuf.Bytes())
			buf = bytes.NewBuffer(currentLeaf.Data[:tempBuf.Len()])
			entryCount = 1
		}
		
		// Handle last entry
		if i == len(entries)-1 {
			// Ensure data is written to page if we haven't written yet
			if buf.Len() == 0 && tempBuf.Len() > 0 {
				copy(currentLeaf.Data[:], tempBuf.Bytes())
				currentLeaf.Header.EntryCount = tempEntryCount
				currentLeaf.Header.DataSize = uint32(tempBuf.Len())
			} else {
				currentLeaf.Header.EntryCount = entryCount
				currentLeaf.Header.DataSize = uint32(buf.Len())
			}
			
			if err := bt.pageManager.WritePage(currentLeaf); err != nil {
				return nil, err
			}
			
			leaves = append(leaves, currentLeaf)
		}
	}
	
	return leaves, nil
}

// buildInternalLevel builds one level of internal nodes (keys only, no child pointers)
func (bt *BPlusTree) buildInternalLevel(children []*Page) ([]*Page, error) {
	if bt.pageManager.compressionOptions != nil && 
	   bt.pageManager.compressionOptions.InternalPageCompression != CompressionNone {
		return bt.buildInternalLevelCompressed(children)
	}
	return bt.buildInternalLevelUncompressed(children)
}

// buildInternalLevelUncompressed is the original implementation
func (bt *BPlusTree) buildInternalLevelUncompressed(children []*Page) ([]*Page, error) {
	internals := make([]*Page, 0)
	currentInternal := bt.pageManager.AllocatePage(PageTypeBTreeInternal)
	
	buf := bytes.NewBuffer(currentInternal.Data[:0])
	entryCount := uint32(0)
	keySize := GetDataTypeSize(bt.dataType)
	
	// Track child pages for current internal node
	currentChildPages := make([]uint64, 0)
	currentChildPages = append(currentChildPages, children[0].Header.PageID) // First child
	
	// Set parent pointer for the first child
	children[0].Header.ParentPageID = currentInternal.Header.PageID
	if err := bt.pageManager.WritePage(children[0]); err != nil {
		return nil, err
	}
	
	// Skip the first child - we'll store keys starting from the second child
	for i := 1; i < len(children); i++ {
		child := children[i]
		
		// Get first key from child
		firstKey, err := bt.getFirstKeyFromPage(child)
		if err != nil {
			return nil, err
		}
		
		// Check if entry fits (only key size, no child pointer)
		entrySize := keySize // Only store keys, no child pointers
		if buf.Len()+entrySize > len(currentInternal.Data) && entryCount > 0 {
			// Finalize current internal node
			currentInternal.Header.EntryCount = entryCount
			currentInternal.Header.DataSize = uint32(buf.Len())
			
			// Store child page mapping for this internal node
			bt.childPageMap[currentInternal.Header.PageID] = make([]uint64, len(currentChildPages))
			copy(bt.childPageMap[currentInternal.Header.PageID], currentChildPages)
			
			if err := bt.pageManager.WritePage(currentInternal); err != nil {
				return nil, err
			}
			
			internals = append(internals, currentInternal)
			
			// Create new internal node
			currentInternal = bt.pageManager.AllocatePage(PageTypeBTreeInternal)
			buf = bytes.NewBuffer(currentInternal.Data[:0])
			entryCount = 0
			
			// Reset child pages tracking and start with current child
			currentChildPages = make([]uint64, 0)
			currentChildPages = append(currentChildPages, child.Header.PageID)
		} else {
			// Add this child to the current internal node
			currentChildPages = append(currentChildPages, child.Header.PageID)
		}
		
		// For all children except the first in this internal node, write the separator key
		if len(currentChildPages) > 1 {
			// Write key only (no child pointer)
			buf.Write(EncodeKey(firstKey, bt.dataType))
			entryCount++
		}
		
		// Update child's parent pointer
		child.Header.ParentPageID = currentInternal.Header.PageID
		if err := bt.pageManager.WritePage(child); err != nil {
			return nil, err
		}
	}
	
	// Finalize last internal node
	currentInternal.Header.EntryCount = entryCount
	currentInternal.Header.DataSize = uint32(buf.Len())
	
	// Store child page mapping for the final internal node
	bt.childPageMap[currentInternal.Header.PageID] = make([]uint64, len(currentChildPages))
	copy(bt.childPageMap[currentInternal.Header.PageID], currentChildPages)
	
	if err := bt.pageManager.WritePage(currentInternal); err != nil {
		return nil, err
	}
	
	internals = append(internals, currentInternal)
	
	return internals, nil
}

// buildInternalLevelCompressed builds internal nodes with compression awareness
func (bt *BPlusTree) buildInternalLevelCompressed(children []*Page) ([]*Page, error) {
	internals := make([]*Page, 0)
	currentInternal := bt.pageManager.AllocatePage(PageTypeBTreeInternal)
	
	buf := bytes.NewBuffer(currentInternal.Data[:0])
	entryCount := uint32(0)
	
	// Track child pages for current internal node
	currentChildPages := make([]uint64, 0)
	currentChildPages = append(currentChildPages, children[0].Header.PageID) // First child
	
	// Set parent pointer for the first child
	children[0].Header.ParentPageID = currentInternal.Header.PageID
	if err := bt.pageManager.WritePage(children[0]); err != nil {
		return nil, err
	}
	
	// Temp buffer for compression estimation
	tempBuf := new(bytes.Buffer)
	tempEntryCount := uint32(0)
	tempChildPages := make([]uint64, len(currentChildPages))
	copy(tempChildPages, currentChildPages)
	
	// Skip the first child - we'll store keys starting from the second child
	for i := 1; i < len(children); i++ {
		child := children[i]
		
		// Get first key from child
		firstKey, err := bt.getFirstKeyFromPage(child)
		if err != nil {
			return nil, err
		}
		
		// Add to temp buffer
		tempChildPages = append(tempChildPages, child.Header.PageID)
		if len(tempChildPages) > 1 {
			tempBuf.Write(EncodeKey(firstKey, bt.dataType))
			tempEntryCount++
		}
		
		// Check both uncompressed and compressed size constraints
		uncompressedSize := tempBuf.Len()
		compressedSize, _ := bt.pageManager.EstimateCompressedSize(PageTypeBTreeInternal, tempBuf.Bytes())
		
		// If both sizes fit, update actual buffer
		if uncompressedSize <= len(currentInternal.Data) && compressedSize <= len(currentInternal.Data) {
			currentChildPages = append(currentChildPages, child.Header.PageID)
			if len(currentChildPages) > 1 {
				buf.Write(EncodeKey(firstKey, bt.dataType))
				entryCount++
			}
			
			// Update child's parent pointer
			child.Header.ParentPageID = currentInternal.Header.PageID
			if err := bt.pageManager.WritePage(child); err != nil {
				return nil, err
			}
		} else if entryCount > 0 {
			// Finalize current internal node
			currentInternal.Header.EntryCount = entryCount
			currentInternal.Header.DataSize = uint32(buf.Len())
			
			// Store child page mapping for this internal node
			bt.childPageMap[currentInternal.Header.PageID] = make([]uint64, len(currentChildPages))
			copy(bt.childPageMap[currentInternal.Header.PageID], currentChildPages)
			
			if err := bt.pageManager.WritePage(currentInternal); err != nil {
				return nil, err
			}
			
			internals = append(internals, currentInternal)
			
			// Create new internal node
			currentInternal = bt.pageManager.AllocatePage(PageTypeBTreeInternal)
			buf = bytes.NewBuffer(currentInternal.Data[:0])
			entryCount = 0
			
			// Reset with current child
			currentChildPages = make([]uint64, 0)
			currentChildPages = append(currentChildPages, child.Header.PageID)
			
			// Update child's parent pointer
			child.Header.ParentPageID = currentInternal.Header.PageID
			if err := bt.pageManager.WritePage(child); err != nil {
				return nil, err
			}
			
			// Reset temp buffer
			tempBuf = new(bytes.Buffer)
			tempEntryCount = 0
			tempChildPages = make([]uint64, 1)
			tempChildPages[0] = child.Header.PageID
		}
	}
	
	// Finalize last internal node
	currentInternal.Header.EntryCount = entryCount
	currentInternal.Header.DataSize = uint32(buf.Len())
	
	// Store child page mapping for the final internal node
	bt.childPageMap[currentInternal.Header.PageID] = make([]uint64, len(currentChildPages))
	copy(bt.childPageMap[currentInternal.Header.PageID], currentChildPages)
	
	if err := bt.pageManager.WritePage(currentInternal); err != nil {
		return nil, err
	}
	
	internals = append(internals, currentInternal)
	
	return internals, nil
}

// navigateToLeaf finds the leaf page that should contain the given key using child page mapping
func (bt *BPlusTree) navigateToLeaf(key uint64) (*Page, error) {
	if bt.height == 1 {
		// Single leaf page tree
		return bt.pageManager.ReadPage(bt.rootPageID)
	}
	
	// Navigate using child page mapping
	currentPageID := bt.rootPageID
	
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			return page, nil
		}
		
		if page.Header.PageType != PageTypeBTreeInternal {
			return nil, fmt.Errorf("unexpected page type in navigation: %v", page.Header.PageType)
		}
		
		// Get child pages for this internal node
		childPages, exists := bt.childPageMap[page.Header.PageID]
		if !exists {
			return nil, fmt.Errorf("no child page mapping found for internal page %d", page.Header.PageID)
		}
		
		// Get internal entries to find the right child
		entries, err := bt.readInternalEntries(page)
		if err != nil {
			return nil, err
		}
		
		// Find which child to follow based on key comparison
		childIndex := 0
		for i, entry := range entries {
			cmp, err := bt.comparator.Compare(key, entry.Key)
			if err != nil {
				return nil, err
			}
			
			if cmp >= 0 {
				childIndex = i + 1
			} else {
				break
			}
		}
		
		// Ensure childIndex is within bounds
		if childIndex >= len(childPages) {
			childIndex = len(childPages) - 1
		}
		
		// Navigate to the selected child page
		currentPageID = childPages[childIndex]
	}
}

// readLeafEntries reads all entries from a leaf page
func (bt *BPlusTree) readLeafEntries(page *Page) ([]BTreeLeafEntry, error) {
	if page.Header.PageType != PageTypeBTreeLeaf {
		return nil, fmt.Errorf("expected leaf page, got %v", page.Header.PageType)
	}
	
	entries := make([]BTreeLeafEntry, 0, page.Header.EntryCount)
	buf := bytes.NewReader(page.Data[:page.Header.DataSize])
	keySize := GetDataTypeSize(bt.dataType)
	
	for i := uint32(0); i < page.Header.EntryCount; i++ {
		var entry BTreeLeafEntry
		
		// Read variable-size key
		keyBuf := make([]byte, keySize)
		if _, err := buf.Read(keyBuf); err != nil {
			return nil, err
		}
		entry.Key = DecodeKey(keyBuf, bt.dataType)
		
		if err := binary.Read(buf, ByteOrder, &entry.Value.Data); err != nil {
			return nil, err
		}
		
		entries = append(entries, entry)
	}
	
	return entries, nil
}

// readInternalEntries reads all entries from an internal page (keys only, no child pointers)
func (bt *BPlusTree) readInternalEntries(page *Page) ([]BTreeInternalEntry, error) {
	if page.Header.PageType != PageTypeBTreeInternal {
		return nil, fmt.Errorf("expected internal page, got %v", page.Header.PageType)
	}
	
	entries := make([]BTreeInternalEntry, 0, page.Header.EntryCount)
	buf := bytes.NewReader(page.Data[:page.Header.DataSize])
	keySize := GetDataTypeSize(bt.dataType)
	
	for i := uint32(0); i < page.Header.EntryCount; i++ {
		var entry BTreeInternalEntry
		
		// Read variable-size key only (no child pointer)
		keyBuf := make([]byte, keySize)
		if _, err := buf.Read(keyBuf); err != nil {
			return nil, err
		}
		entry.Key = DecodeKey(keyBuf, bt.dataType)
		
		entries = append(entries, entry)
	}
	
	return entries, nil
}

// getFirstKeyFromPage gets the first key from any page type
func (bt *BPlusTree) getFirstKeyFromPage(page *Page) (uint64, error) {
	switch page.Header.PageType {
	case PageTypeBTreeLeaf:
		entries, err := bt.readLeafEntries(page)
		if err != nil || len(entries) == 0 {
			return 0, fmt.Errorf("failed to read first key from leaf")
		}
		return entries[0].Key, nil
		
	case PageTypeBTreeInternal:
		// For optimized internal nodes, get first key from child page mapping
		childPages, exists := bt.childPageMap[page.Header.PageID]
		if !exists || len(childPages) == 0 {
			return 0, fmt.Errorf("no child page mapping found for internal page %d", page.Header.PageID)
		}
		
		// Get first key from the first child page
		firstChild, err := bt.pageManager.ReadPage(childPages[0])
		if err != nil {
			return 0, err
		}
		return bt.getFirstKeyFromPage(firstChild)
		
	default:
		return 0, fmt.Errorf("unexpected page type: %v", page.Header.PageType)
	}
}

// loadBitmap loads row numbers from a bitmap page
func (bt *BPlusTree) loadBitmap(bitmapPageID uint64) ([]uint64, error) {
	bitmap, err := bt.bitmapManager.LoadBitmap(bitmapPageID)
	if err != nil {
		return nil, err
	}
	
	return bt.bitmapManager.ExtractRowNumbers(bitmap), nil
}

// GetRootPageID returns the root page ID
func (bt *BPlusTree) GetRootPageID() uint64 {
	return bt.rootPageID
}

// GetHeight returns the height of the B+ tree
func (bt *BPlusTree) GetHeight() uint32 {
	return bt.height
}

// SetHeight sets the height of the B+ tree
func (bt *BPlusTree) SetHeight(height uint32) {
	bt.height = height
}

// FindStringOffset searches for a string in the B+ tree and returns its offset
// This is optimized for string B-trees where keys are string offsets
func (bt *BPlusTree) FindStringOffset(str string, stringSegment *StringSegment) (uint64, bool, error) {
	if bt.rootPageID == 0 {
		return 0, false, nil
	}
	
	// Navigate through the tree using string comparisons
	currentPageID := bt.rootPageID
	
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return 0, false, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			// We've reached a leaf, search for the string
			entries, err := bt.readLeafEntries(page)
			if err != nil {
				return 0, false, err
			}
			
			// Binary search through leaf entries
			left, right := 0, len(entries)-1
			
			for left <= right {
				mid := (left + right) / 2
				
				// Get string at this offset
				storedStr, err := stringSegment.GetString(entries[mid].Key)
				if err != nil {
					// If we can't read the string, skip to next
					if left == right {
						break
					}
					// Try the left half
					right = mid - 1
					continue
				}
				
				if storedStr == str {
					// Found the string
					return entries[mid].Key, true, nil
				} else if str < storedStr {
					right = mid - 1
				} else {
					left = mid + 1
				}
			}
			
			// Not found in this leaf
			return 0, false, nil
		}
		
		// Internal node - navigate using string comparisons
		if page.Header.PageType != PageTypeBTreeInternal {
			return 0, false, fmt.Errorf("unexpected page type in navigation: %v", page.Header.PageType)
		}
		
		// Get child pages for this internal node
		childPages, exists := bt.childPageMap[page.Header.PageID]
		if !exists {
			return 0, false, fmt.Errorf("no child page mapping found for internal page %d", page.Header.PageID)
		}
		
		// Get internal entries to find the right child
		entries, err := bt.readInternalEntries(page)
		if err != nil {
			return 0, false, err
		}
		
		// Binary search within the internal node to find the right child
		childIndex := 0
		left, right := 0, len(entries)-1
		
		for left <= right {
			mid := (left + right) / 2
			
			// Get the string for this key offset
			keyStr, err := stringSegment.GetString(entries[mid].Key)
			if err != nil {
				// If we can't read the string, treat it as greater than target
				right = mid - 1
				continue
			}
			
			// Compare strings lexicographically
			if str < keyStr {
				right = mid - 1
			} else {
				// str >= keyStr, this could be our child
				childIndex = mid + 1
				left = mid + 1
			}
		}
		
		// Ensure childIndex is within bounds
		if childIndex >= len(childPages) {
			childIndex = len(childPages) - 1
		}
		
		// Navigate to the selected child
		currentPageID = childPages[childIndex]
	}
}


// SetRootPageID sets the root page ID (used when loading existing tree)
func (bt *BPlusTree) SetRootPageID(pageID uint64) {
	bt.rootPageID = pageID
}

// ReconstructChildPageMapping rebuilds the child page mapping by traversing the tree
func (bt *BPlusTree) ReconstructChildPageMapping() error {
	if bt.rootPageID == 0 {
		return nil // Empty tree
	}
	
	// Clear existing mapping
	bt.childPageMap = make(map[uint64][]uint64)
	
	// Start from root and recursively build mapping
	return bt.reconstructMappingRecursive(bt.rootPageID)
}

// reconstructMappingRecursive recursively builds child page mapping for internal nodes
func (bt *BPlusTree) reconstructMappingRecursive(pageID uint64) error {
	page, err := bt.pageManager.ReadPage(pageID)
	if err != nil {
		return err
	}
	
	// If this is a leaf page, we're done
	if page.Header.PageType == PageTypeBTreeLeaf {
		return nil
	}
	
	// If this is not an internal node, skip
	if page.Header.PageType != PageTypeBTreeInternal {
		return nil
	}
	
	// For internal nodes, we need to find their children by scanning parent pointers
	
	// Calculate child page IDs by examining the structure
	// In the optimized format, we need to scan for child pages
	childPages := make([]uint64, 0)
	
	// Find all pages that have this page as their parent
	// Note: We need to be careful about the actual page count since some pages might not exist
	maxPageID := bt.pageManager.GetPageCount()
	
	for childPageID := uint64(1); childPageID < maxPageID; childPageID++ {
		childPage, err := bt.pageManager.ReadPage(childPageID)
		if err != nil {
			// Skip pages that can't be read - they might be beyond the actual file size
			// or might be invalid pages
			continue
		}
		
		// Only consider B-tree pages (leaf or internal)
		if childPage.Header.PageType != PageTypeBTreeLeaf && 
		   childPage.Header.PageType != PageTypeBTreeInternal {
			continue
		}
		
		// Check if this child points to our current page as parent
		if childPage.Header.ParentPageID == pageID {
			childPages = append(childPages, childPageID)
		}
	}
	
	// Store the mapping (need to sort child pages by their first key)
	if len(childPages) > 0 {
		// Sort child pages by their first key to maintain correct order
		type childPageWithKey struct {
			pageID uint64
			firstKey uint64
		}
		
		childPagesWithKeys := make([]childPageWithKey, 0, len(childPages))
		for _, childPageID := range childPages {
			childPage, err := bt.pageManager.ReadPage(childPageID)
			if err != nil {
				// If we can't read the page, use the page ID as fallback key
				childPagesWithKeys = append(childPagesWithKeys, childPageWithKey{
					pageID: childPageID,
					firstKey: childPageID,
				})
				continue
			}
			
			var firstKey uint64
			if childPage.Header.PageType == PageTypeBTreeLeaf {
				// For leaf pages, read first entry directly
				entries, err := bt.readLeafEntries(childPage)
				if err != nil || len(entries) == 0 {
					firstKey = childPageID // fallback
				} else {
					firstKey = entries[0].Key
				}
			} else {
				// For internal pages, use page ID as approximation for now
				firstKey = childPageID
			}
			
			childPagesWithKeys = append(childPagesWithKeys, childPageWithKey{
				pageID: childPageID,
				firstKey: firstKey,
			})
		}
		
		// Sort by first key
		for i := 0; i < len(childPagesWithKeys)-1; i++ {
			for j := i + 1; j < len(childPagesWithKeys); j++ {
				if childPagesWithKeys[i].firstKey > childPagesWithKeys[j].firstKey {
					childPagesWithKeys[i], childPagesWithKeys[j] = childPagesWithKeys[j], childPagesWithKeys[i]
				}
			}
		}
		
		// Extract sorted page IDs
		sortedChildPages := make([]uint64, len(childPagesWithKeys))
		for i, cwk := range childPagesWithKeys {
			sortedChildPages[i] = cwk.pageID
		}
		
		bt.childPageMap[pageID] = sortedChildPages
		
		// Recursively process child pages
		for _, childPageID := range sortedChildPages {
			if err := bt.reconstructMappingRecursive(childPageID); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// FindBitmap searches for a key and returns a bitmap of row numbers
func (bt *BPlusTree) FindBitmap(key uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	if bt.rootPageID == 0 {
		return result, nil // Empty tree
	}
	
	// Navigate to leaf
	leafPage, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	// Search within leaf
	entries, err := bt.readLeafEntries(leafPage)
	if err != nil {
		return nil, err
	}
	
	for _, entry := range entries {
		cmp, err := bt.comparator.Compare(entry.Key, key)
		if err != nil {
			return nil, err
		}
		
		if cmp == 0 {
			// Found the key
			if entry.Value.IsRowNumber() {
				result.Add(uint32(entry.Value.GetRowNumber()))
			} else {
				// Load bitmap
				bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
				if err != nil {
					return nil, err
				}
				result.Or(bitmap)
			}
			return result, nil
		} else if cmp > 0 {
			// Keys are sorted, so we've passed the target
			break
		}
	}
	
	return result, nil // Not found, return empty bitmap
}

// RangeSearchBitmap finds all entries with keys in [minKey, maxKey] and returns a bitmap
func (bt *BPlusTree) RangeSearchBitmap(minKey, maxKey uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	if bt.rootPageID == 0 {
		return result, nil // Empty tree
	}
	
	// Find starting leaf
	startLeaf, err := bt.navigateToLeaf(minKey)
	if err != nil {
		return nil, err
	}
	
	currentPage := startLeaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			minCmp, err := bt.comparator.Compare(entry.Key, minKey)
			if err != nil {
				return nil, err
			}
			
			maxCmp, err := bt.comparator.Compare(entry.Key, maxKey)
			if err != nil {
				return nil, err
			}
			
			if minCmp >= 0 && maxCmp <= 0 {
				// Key is in range
				if entry.Value.IsRowNumber() {
					result.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					result.Or(bitmap)
				}
			} else if maxCmp > 0 {
				// We've passed the range
				return result, nil
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return result, nil
}

// RangeSearchGreaterThanBitmap finds all entries with key > value and returns a bitmap
func (bt *BPlusTree) RangeSearchGreaterThanBitmap(key uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	// Find the leaf that would contain this key
	leaf, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	currentPage := leaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp > 0 {
				// Key is greater than value
				if entry.Value.IsRowNumber() {
					result.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					result.Or(bitmap)
				}
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return result, nil
}

// RangeSearchGreaterThanOrEqualBitmap finds all entries with key >= value and returns a bitmap
func (bt *BPlusTree) RangeSearchGreaterThanOrEqualBitmap(key uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	// Find the leaf that would contain this key
	leaf, err := bt.navigateToLeaf(key)
	if err != nil {
		return nil, err
	}
	
	currentPage := leaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp >= 0 {
				// Key is greater than or equal to value
				if entry.Value.IsRowNumber() {
					result.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					result.Or(bitmap)
				}
			}
		}
		
		// Move to next leaf
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	
	return result, nil
}

// RangeSearchLessThanBitmap finds all entries with key < value and returns a bitmap
func (bt *BPlusTree) RangeSearchLessThanBitmap(key uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	// Start from the leftmost leaf
	currentPageID := bt.rootPageID
	
	// Navigate to leftmost leaf
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			break
		}
		
		// For internal nodes, get first child from childPageMap
		children, exists := bt.childPageMap[currentPageID]
		if !exists || len(children) == 0 {
			return nil, fmt.Errorf("no children found for internal page %d", currentPageID)
		}
		currentPageID = children[0]
	}
	
	// Scan leaves until we reach or exceed the key
	for currentPageID != 0 {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		entries, err := bt.readLeafEntries(page)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp < 0 {
				// Key is less than value
				if entry.Value.IsRowNumber() {
					result.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					result.Or(bitmap)
				}
			} else {
				// We've reached or exceeded the key
				return result, nil
			}
		}
		
		currentPageID = page.Header.NextPageID
	}
	
	return result, nil
}

// RangeSearchLessThanOrEqualBitmap finds all entries with key <= value and returns a bitmap
func (bt *BPlusTree) RangeSearchLessThanOrEqualBitmap(key uint64) (*roaring.Bitmap, error) {
	result := roaring.New()
	
	// Start from the leftmost leaf
	currentPageID := bt.rootPageID
	
	// Navigate to leftmost leaf
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			break
		}
		
		// For internal nodes, get first child from childPageMap
		children, exists := bt.childPageMap[currentPageID]
		if !exists || len(children) == 0 {
			return nil, fmt.Errorf("no children found for internal page %d", currentPageID)
		}
		currentPageID = children[0]
	}
	
	// Scan leaves until we exceed the key
	for currentPageID != 0 {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		entries, err := bt.readLeafEntries(page)
		if err != nil {
			return nil, err
		}
		
		for _, entry := range entries {
			cmp, err := bt.comparator.Compare(entry.Key, key)
			if err != nil {
				return nil, err
			}
			
			if cmp <= 0 {
				// Key is less than or equal to value
				if entry.Value.IsRowNumber() {
					result.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						return nil, err
					}
					result.Or(bitmap)
				}
			} else {
				// We've exceeded the key
				return result, nil
			}
		}
		
		currentPageID = page.Header.NextPageID
	}
	
	return result, nil
}

// Iterator support methods

// findLeafPage finds the leaf page that would contain the given key
func (bt *BPlusTree) findLeafPage(key uint64) (*Page, error) {
	return bt.navigateToLeaf(key)
}

// findLeftmostLeaf finds the leftmost leaf page in the tree
func (bt *BPlusTree) findLeftmostLeaf() (*Page, error) {
	if bt.rootPageID == 0 {
		return nil, fmt.Errorf("empty tree")
	}
	
	currentPageID := bt.rootPageID
	
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			return page, nil
		}
		
		if page.Header.PageType != PageTypeBTreeInternal {
			return nil, fmt.Errorf("unexpected page type in navigation: %v", page.Header.PageType)
		}
		
		// For internal pages, get the first child using childPageMap
		childPages, exists := bt.childPageMap[page.Header.PageID]
		if !exists {
			return nil, fmt.Errorf("no child page mapping found for internal page %d", page.Header.PageID)
		}
		
		if len(childPages) == 0 {
			return nil, fmt.Errorf("empty child page list for internal page %d", page.Header.PageID)
		}
		
		// First child is the leftmost
		currentPageID = childPages[0]
	}
}

// findRightmostLeaf finds the rightmost leaf page in the tree
func (bt *BPlusTree) findRightmostLeaf() (*Page, error) {
	if bt.rootPageID == 0 {
		return nil, fmt.Errorf("empty tree")
	}
	
	currentPageID := bt.rootPageID
	
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			return page, nil
		}
		
		if page.Header.PageType != PageTypeBTreeInternal {
			return nil, fmt.Errorf("unexpected page type in navigation: %v", page.Header.PageType)
		}
		
		// For internal pages, get the last child using childPageMap
		childPages, exists := bt.childPageMap[page.Header.PageID]
		if !exists {
			return nil, fmt.Errorf("no child page mapping found for internal page %d", page.Header.PageID)
		}
		
		if len(childPages) == 0 {
			return nil, fmt.Errorf("empty child page list for internal page %d", page.Header.PageID)
		}
		
		// Last child is the rightmost
		currentPageID = childPages[len(childPages)-1]
	}
}

// LookupByRow performs a reverse lookup to find the key for a given row number
func (bt *BPlusTree) LookupByRow(rowNum uint64) (uint64, bool, error) {
	if bt.rootPageID == 0 {
		return 0, false, nil
	}
	
	// Start from the leftmost leaf and scan until we find the row
	leaf, err := bt.findLeftmostLeaf()
	if err != nil {
		return 0, false, err
	}
	
	currentPage := leaf
	
	for currentPage != nil {
		entries, err := bt.readLeafEntries(currentPage)
		if err != nil {
			return 0, false, err
		}
		
		for _, entry := range entries {
			if entry.Value.IsRowNumber() && entry.Value.GetRowNumber() == rowNum {
				return entry.Key, true, nil
			} else if !entry.Value.IsRowNumber() {
				// Check if the row is in the bitmap
				bitmap, err := bt.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
				if err != nil {
					return 0, false, err
				}
				
				if bitmap.Contains(uint32(rowNum)) {
					return entry.Key, true, nil
				}
			}
		}
		
		// Move to next page
		if currentPage.Header.NextPageID != 0 {
			currentPage, err = bt.pageManager.ReadPage(currentPage.Header.NextPageID)
			if err != nil {
				return 0, false, err
			}
		} else {
			break
		}
	}
	
	return 0, false, nil
}
