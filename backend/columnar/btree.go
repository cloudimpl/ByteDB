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

// buildInternalLevel builds one level of internal nodes (keys only, no child pointers)
func (bt *BPlusTree) buildInternalLevel(children []*Page) ([]*Page, error) {
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
		if buf.Len()+entrySize > len(currentInternal.Data) {
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
		
		// Write key only (no child pointer)
		buf.Write(EncodeKey(firstKey, bt.dataType))
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
	for childPageID := uint64(1); childPageID < bt.pageManager.GetPageCount(); childPageID++ {
		childPage, err := bt.pageManager.ReadPage(childPageID)
		if err != nil {
			continue // Skip inaccessible pages
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
		
		// For internal nodes, always go to first child
		currentPageID = ByteOrder.Uint64(page.Data[0:8])
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
		
		// For internal nodes, always go to first child
		currentPageID = ByteOrder.Uint64(page.Data[0:8])
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