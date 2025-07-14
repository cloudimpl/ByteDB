package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
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

// BPlusTree represents a B+ tree optimized for columnar storage
type BPlusTree struct {
	pageManager   *PageManager
	bitmapManager *BitmapManager
	rootPageID    uint64
	dataType      DataType
	comparator    KeyComparator
	height        uint32
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
	}
}

// BTreeInternalEntry represents an entry in an internal node
type BTreeInternalEntry struct {
	Key        uint64
	ChildPageID uint64
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
		
		// For internal nodes, always go to first child
		currentPageID = ByteOrder.Uint64(page.Data[0:8])
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
		
		// For internal nodes, always go to first child
		currentPageID = ByteOrder.Uint64(page.Data[0:8])
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
func (bt *BPlusTree) BulkLoadWithDuplicates(keyRows []struct{Key uint64; RowNum uint64}) error {
	if len(keyRows) == 0 {
		return nil
	}
	
	// Group by key
	keyGroups := make(map[uint64][]uint64)
	for _, kr := range keyRows {
		keyGroups[kr.Key] = append(keyGroups[kr.Key], kr.RowNum)
	}
	
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
				return fmt.Errorf("failed to create bitmap for key %d: %w", key, err)
			}
			value = NewBitmapValue(bitmapPageID)
		}
		
		entries = append(entries, BTreeLeafEntry{Key: key, Value: value})
	}
	
	return bt.BulkLoad(entries)
}

// buildLeaves creates leaf pages from sorted entries
func (bt *BPlusTree) buildLeaves(entries []BTreeLeafEntry) ([]*Page, error) {
	leaves := make([]*Page, 0)
	currentLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
	
	buf := bytes.NewBuffer(currentLeaf.Data[:0])
	entryCount := uint32(0)
	keySize := GetDataTypeSize(bt.dataType)
	
	for i, entry := range entries {
		// Check if entry fits in current leaf
		entrySize := keySize + 8 // variable key size + 8 bytes value
		if buf.Len()+entrySize > len(currentLeaf.Data) {
			// Finalize current leaf
			currentLeaf.Header.EntryCount = entryCount
			currentLeaf.Header.DataSize = uint32(buf.Len())
			
			if err := bt.pageManager.WritePage(currentLeaf); err != nil {
				return nil, err
			}
			
			leaves = append(leaves, currentLeaf)
			
			// Create new leaf
			newLeaf := bt.pageManager.AllocatePage(PageTypeBTreeLeaf)
			currentLeaf.Header.NextPageID = newLeaf.Header.PageID
			newLeaf.Header.PrevPageID = currentLeaf.Header.PageID
			
			currentLeaf = newLeaf
			buf = bytes.NewBuffer(currentLeaf.Data[:0])
			entryCount = 0
		}
		
		// Write entry
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

// buildInternalLevel builds one level of internal nodes
func (bt *BPlusTree) buildInternalLevel(children []*Page) ([]*Page, error) {
	internals := make([]*Page, 0)
	currentInternal := bt.pageManager.AllocatePage(PageTypeBTreeInternal)
	
	buf := bytes.NewBuffer(currentInternal.Data[:0])
	entryCount := uint32(0)
	keySize := GetDataTypeSize(bt.dataType)
	
	// Write first child pointer
	binary.Write(buf, ByteOrder, children[0].Header.PageID)
	
	for i := 1; i < len(children); i++ {
		child := children[i]
		
		// Get first key from child
		firstKey, err := bt.getFirstKeyFromPage(child)
		if err != nil {
			return nil, err
		}
		
		// Check if entry fits
		entrySize := keySize + 8 // variable key size + 8 bytes child pointer
		if buf.Len()+entrySize > len(currentInternal.Data) {
			// Finalize current internal node
			currentInternal.Header.EntryCount = entryCount
			currentInternal.Header.DataSize = uint32(buf.Len())
			
			if err := bt.pageManager.WritePage(currentInternal); err != nil {
				return nil, err
			}
			
			internals = append(internals, currentInternal)
			
			// Create new internal node
			currentInternal = bt.pageManager.AllocatePage(PageTypeBTreeInternal)
			buf = bytes.NewBuffer(currentInternal.Data[:0])
			entryCount = 0
			
			// Write first child pointer
			binary.Write(buf, ByteOrder, child.Header.PageID)
		}
		
		// Write key and child pointer
		buf.Write(EncodeKey(firstKey, bt.dataType))
		binary.Write(buf, ByteOrder, child.Header.PageID)
		entryCount++
		
		// Update child's parent pointer
		child.Header.ParentPageID = currentInternal.Header.PageID
		if err := bt.pageManager.WritePage(child); err != nil {
			return nil, err
		}
	}
	
	// Finalize last internal node
	currentInternal.Header.EntryCount = entryCount
	currentInternal.Header.DataSize = uint32(buf.Len())
	
	if err := bt.pageManager.WritePage(currentInternal); err != nil {
		return nil, err
	}
	
	internals = append(internals, currentInternal)
	
	return internals, nil
}

// navigateToLeaf finds the leaf page that should contain the given key
func (bt *BPlusTree) navigateToLeaf(key uint64) (*Page, error) {
	currentPageID := bt.rootPageID
	
	for {
		page, err := bt.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}
		
		if page.Header.PageType == PageTypeBTreeLeaf {
			return page, nil
		}
		
		// Internal node - find child to follow
		entries, err := bt.readInternalEntries(page)
		if err != nil {
			return nil, err
		}
		
		// Binary search for appropriate child
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
		
		// Read child pointer
		if childIndex == 0 {
			// Use first child pointer
			currentPageID = ByteOrder.Uint64(page.Data[0:8])
		} else {
			// Use pointer after the key
			keySize := GetDataTypeSize(bt.dataType)
			offset := 8 + (childIndex-1)*(keySize+8) + keySize
			currentPageID = ByteOrder.Uint64(page.Data[offset : offset+8])
		}
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

// readInternalEntries reads all entries from an internal page
func (bt *BPlusTree) readInternalEntries(page *Page) ([]BTreeInternalEntry, error) {
	if page.Header.PageType != PageTypeBTreeInternal {
		return nil, fmt.Errorf("expected internal page, got %v", page.Header.PageType)
	}
	
	entries := make([]BTreeInternalEntry, 0, page.Header.EntryCount)
	buf := bytes.NewReader(page.Data[8:page.Header.DataSize]) // Skip first child pointer
	keySize := GetDataTypeSize(bt.dataType)
	
	for i := uint32(0); i < page.Header.EntryCount; i++ {
		var entry BTreeInternalEntry
		
		// Read variable-size key
		keyBuf := make([]byte, keySize)
		if _, err := buf.Read(keyBuf); err != nil {
			return nil, err
		}
		entry.Key = DecodeKey(keyBuf, bt.dataType)
		
		if err := binary.Read(buf, ByteOrder, &entry.ChildPageID); err != nil {
			return nil, err
		}
		
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
		// For internal nodes, recursively get from first child
		firstChildID := ByteOrder.Uint64(page.Data[0:8])
		firstChild, err := bt.pageManager.ReadPage(firstChildID)
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

// SetRootPageID sets the root page ID (used when loading existing tree)
func (bt *BPlusTree) SetRootPageID(pageID uint64) {
	bt.rootPageID = pageID
}