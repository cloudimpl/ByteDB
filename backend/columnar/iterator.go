package columnar

import (
	"fmt"
	"sort"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// ColumnIterator provides ordered iteration over column data
type ColumnIterator interface {
	// Navigation
	Next() bool                    // Move to next item
	Prev() bool                    // Move to previous item (reverse iteration)
	Seek(value interface{}) bool   // Jump to specific value
	SeekFirst() bool              // Jump to first item
	SeekLast() bool               // Jump to last item
	
	// Current value access
	Key() interface{}             // Current key value
	Rows() *roaring.Bitmap       // Current rows bitmap
	
	// State
	Valid() bool                 // Is current position valid?
	Error() error               // Any error occurred?
	
	// Cleanup
	Close() error               // Release resources
}

// RangeIterator provides iteration over a range of values
type RangeIterator interface {
	ColumnIterator
	
	// Range bounds
	SetBounds(min, max interface{}) error
	InBounds() bool
	HasLowerBound() bool
	HasUpperBound() bool
	LowerBound() interface{}
	UpperBound() interface{}
}

// Direction specifies iteration direction
type Direction int

const (
	Forward Direction = iota
	Reverse
)

// BTreeIterator implements ColumnIterator for B-tree columns
type BTreeIterator struct {
	// B-tree reference
	btree *BPlusTree
	
	// Data type of the column
	dataType DataType
	
	// String segment for string columns
	stringSegment *StringSegment
	
	// Current position
	currentPage     *Page
	currentPageID   uint64
	currentEntryIdx int
	currentKey      uint64
	currentRows     *roaring.Bitmap
	
	// State
	valid     bool
	direction Direction
	err       error
	
	// Range bounds (for range iterator)
	hasLowerBound bool
	hasUpperBound bool
	lowerBound    uint64
	upperBound    uint64
	
	// Page cache for efficiency
	pageCache map[uint64]*Page
}

// NewBTreeIterator creates a new B-tree iterator
func NewBTreeIterator(btree *BPlusTree, dataType DataType, stringSegment *StringSegment) *BTreeIterator {
	return &BTreeIterator{
		btree:         btree,
		dataType:      dataType,
		stringSegment: stringSegment,
		direction:     Forward,
		pageCache:     make(map[uint64]*Page),
	}
}

// NewBTreeRangeIterator creates a new B-tree range iterator
func NewBTreeRangeIterator(btree *BPlusTree, dataType DataType, stringSegment *StringSegment, min, max uint64) *BTreeIterator {
	iter := &BTreeIterator{
		btree:         btree,
		dataType:      dataType,
		stringSegment: stringSegment,
		direction:     Forward,
		hasLowerBound: true,
		hasUpperBound: true,
		lowerBound:    min,
		upperBound:    max,
		pageCache:     make(map[uint64]*Page),
	}
	
	// Position at first valid entry but don't mark as valid yet
	// This way, the first Next() call will return the lower bound
	if iter.dataType == DataTypeString {
		// For strings, min is already an offset
		iter.Seek(min)
	} else {
		// For other types, convert to appropriate type for Seek
		iter.Seek(min)
	}
	if iter.valid {
		iter.valid = false  // Reset so Next() will return the current position
	}
	return iter
}

// Next moves to the next item
func (iter *BTreeIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	
	// If not currently valid, check if we have a positioned cursor
	if !iter.valid {
		if iter.currentPage != nil {
			// We have a positioned cursor, just validate it
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
		// No positioned cursor, seek to first
		return iter.SeekFirst()
	}
	
	// Move to next entry in current page
	if iter.currentPage != nil {
		iter.currentEntryIdx++
		
		// Check if we have more entries in current page
		if iter.currentEntryIdx < int(iter.currentPage.Header.EntryCount) {
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
	}
	
	// Move to next page
	if iter.currentPage != nil && iter.currentPage.Header.NextPageID != 0 {
		nextPageID := iter.currentPage.Header.NextPageID
		if iter.loadPage(nextPageID) {
			iter.currentEntryIdx = 0
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
	}
	
	// No more entries
	iter.valid = false
	return false
}

// Prev moves to the previous item
func (iter *BTreeIterator) Prev() bool {
	if iter.err != nil {
		return false
	}
	
	// If not currently valid, try to find last valid position
	if !iter.valid {
		return iter.SeekLast()
	}
	
	// Move to previous entry in current page
	if iter.currentPage != nil && iter.currentEntryIdx > 0 {
		iter.currentEntryIdx--
		if iter.loadCurrentEntry() {
			return iter.checkBounds()
		}
	}
	
	// Move to previous page
	if iter.currentPage != nil && iter.currentPage.Header.PrevPageID != 0 {
		prevPageID := iter.currentPage.Header.PrevPageID
		if iter.loadPage(prevPageID) {
			iter.currentEntryIdx = int(iter.currentPage.Header.EntryCount) - 1
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
	}
	
	// No more entries
	iter.valid = false
	return false
}

// Seek positions the iterator at the first entry >= value
func (iter *BTreeIterator) Seek(value interface{}) bool {
	if iter.err != nil {
		return false
	}
	
	var keyValue uint64
	
	// Handle different data types
	if iter.dataType == DataTypeString {
		// For strings, check if value is already an offset (uint64) or a string
		switch v := value.(type) {
		case uint64:
			// Already an offset
			keyValue = v
		case string:
			// Convert string to offset
			if iter.stringSegment != nil {
				// Find the offset for this string or the next closest one
				offset, found := iter.stringSegment.FindOffset(v)
				if !found {
					offset = iter.stringSegment.FindClosestOffset(v)
				}
				keyValue = offset
			} else {
				iter.err = fmt.Errorf("string segment not initialized")
				return false
			}
		default:
			iter.err = fmt.Errorf("seek value must be string or uint64 for string column, got %T", value)
			return false
		}
	} else {
		// For numeric types, convert to uint64
		var ok bool
		keyValue, ok = value.(uint64)
		if !ok {
			// Try to convert from various numeric types
			switch v := value.(type) {
			case int64:
				keyValue = uint64(v)
			case int32:
				keyValue = uint64(v)
			case int16:
				keyValue = uint64(v)
			case int8:
				keyValue = uint64(v)
			case uint32:
				keyValue = uint64(v)
			case uint16:
				keyValue = uint64(v)
			case uint8:
				keyValue = uint64(v)
			case bool:
				if v {
					keyValue = 1
				} else {
					keyValue = 0
				}
			default:
				iter.err = fmt.Errorf("invalid value type for seek: %T", value)
				return false
			}
		}
	}
	
	// Find the leaf page containing this key
	leafPage, err := iter.btree.findLeafPage(keyValue)
	if err != nil {
		iter.err = err
		return false
	}
	
	iter.currentPage = leafPage
	iter.currentPageID = leafPage.Header.PageID
	
	// Find the entry within the page
	entries, err := iter.btree.readLeafEntries(leafPage)
	if err != nil {
		iter.err = err
		return false
	}
	
	// Binary search for the first entry >= keyValue
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].Key >= keyValue
	})
	
	if idx < len(entries) {
		iter.currentEntryIdx = idx
		if iter.loadCurrentEntry() {
			return iter.checkBounds()
		}
	}
	
	// If not found in this page, check next page
	if leafPage.Header.NextPageID != 0 {
		nextPageID := leafPage.Header.NextPageID
		if iter.loadPage(nextPageID) {
			iter.currentEntryIdx = 0
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
	}
	
	iter.valid = false
	return false
}

// SeekFirst positions at the first entry
func (iter *BTreeIterator) SeekFirst() bool {
	if iter.err != nil {
		return false
	}
	
	// Find the leftmost leaf page
	leafPage, err := iter.btree.findLeftmostLeaf()
	if err != nil {
		iter.err = err
		return false
	}
	
	iter.currentPage = leafPage
	iter.currentPageID = leafPage.Header.PageID
	iter.currentEntryIdx = 0
	
	if iter.loadCurrentEntry() {
		return iter.checkBounds()
	}
	
	iter.valid = false
	return false
}

// SeekLast positions at the last entry
func (iter *BTreeIterator) SeekLast() bool {
	if iter.err != nil {
		return false
	}
	
	// If we have an upper bound, seek to it and then find the last valid entry
	if iter.hasUpperBound {
		// Find the page containing the upper bound
		leafPage, err := iter.btree.findLeafPage(iter.upperBound)
		if err != nil {
			iter.err = err
			return false
		}
		
		iter.currentPage = leafPage
		iter.currentPageID = leafPage.Header.PageID
		
		// Find entries in this page
		entries, err := iter.btree.readLeafEntries(leafPage)
		if err != nil {
			iter.err = err
			return false
		}
		
		// Find the last entry <= upperBound
		lastValidIdx := -1
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].Key <= iter.upperBound {
				lastValidIdx = i
				break
			}
		}
		
		if lastValidIdx >= 0 {
			iter.currentEntryIdx = lastValidIdx
			if iter.loadCurrentEntry() {
				return iter.checkBounds()
			}
		}
		
		// If not found in this page, check previous pages
		for leafPage.Header.PrevPageID != 0 {
			prevPageID := leafPage.Header.PrevPageID
			leafPage, err = iter.btree.pageManager.ReadPage(prevPageID)
			if err != nil {
				iter.err = err
				return false
			}
			
			iter.currentPage = leafPage
			iter.currentPageID = leafPage.Header.PageID
			
			entries, err = iter.btree.readLeafEntries(leafPage)
			if err != nil {
				iter.err = err
				return false
			}
			
			// Check if any entry is within bounds
			if len(entries) > 0 && entries[len(entries)-1].Key <= iter.upperBound {
				// Find the last valid entry
				for i := len(entries) - 1; i >= 0; i-- {
					if entries[i].Key <= iter.upperBound {
						iter.currentEntryIdx = i
						if iter.loadCurrentEntry() {
							return iter.checkBounds()
						}
						break
					}
				}
			}
		}
		
		iter.valid = false
		return false
	}
	
	// No upper bound - find the rightmost leaf page
	leafPage, err := iter.btree.findRightmostLeaf()
	if err != nil {
		iter.err = err
		return false
	}
	
	iter.currentPage = leafPage
	iter.currentPageID = leafPage.Header.PageID
	iter.currentEntryIdx = int(leafPage.Header.EntryCount) - 1
	
	if iter.loadCurrentEntry() {
		return iter.checkBounds()
	}
	
	iter.valid = false
	return false
}

// Key returns the current key value
func (iter *BTreeIterator) Key() interface{} {
	if !iter.valid {
		return nil
	}
	
	// Handle different data types
	switch iter.dataType {
	case DataTypeBool:
		return iter.currentKey != 0
	case DataTypeInt8:
		return int8(iter.currentKey)
	case DataTypeInt16:
		return int16(iter.currentKey)
	case DataTypeInt32:
		return int32(iter.currentKey)
	case DataTypeInt64:
		return int64(iter.currentKey)
	case DataTypeUint8:
		return uint8(iter.currentKey)
	case DataTypeUint16:
		return uint16(iter.currentKey)
	case DataTypeUint32:
		return uint32(iter.currentKey)
	case DataTypeUint64:
		return iter.currentKey
	case DataTypeString:
		// For strings, currentKey is the offset in the string segment
		if iter.stringSegment != nil {
			str, err := iter.stringSegment.GetString(iter.currentKey)
			if err != nil {
				iter.err = err
				return nil
			}
			return str
		}
		return nil
	default:
		// Float types not yet supported
		return nil
	}
}

// Rows returns the current rows bitmap
func (iter *BTreeIterator) Rows() *roaring.Bitmap {
	if !iter.valid {
		return nil
	}
	return iter.currentRows
}

// Valid returns true if the iterator is positioned at a valid entry
func (iter *BTreeIterator) Valid() bool {
	return iter.valid && iter.err == nil
}

// Error returns any error that occurred
func (iter *BTreeIterator) Error() error {
	return iter.err
}

// Close releases resources
func (iter *BTreeIterator) Close() error {
	iter.currentPage = nil
	iter.pageCache = nil
	iter.valid = false
	return nil
}

// SetBounds sets the range bounds for iteration
func (iter *BTreeIterator) SetBounds(min, max interface{}) error {
	minVal, ok := min.(uint64)
	if !ok {
		if intVal, ok := min.(int64); ok {
			minVal = uint64(intVal)
		} else {
			return fmt.Errorf("invalid min value type: %T", min)
		}
	}
	
	maxVal, ok := max.(uint64)
	if !ok {
		if intVal, ok := max.(int64); ok {
			maxVal = uint64(intVal)
		} else {
			return fmt.Errorf("invalid max value type: %T", max)
		}
	}
	
	iter.hasLowerBound = true
	iter.hasUpperBound = true
	iter.lowerBound = minVal
	iter.upperBound = maxVal
	
	return nil
}

// InBounds checks if current position is within bounds
func (iter *BTreeIterator) InBounds() bool {
	if !iter.valid {
		return false
	}
	
	if iter.hasLowerBound && iter.currentKey < iter.lowerBound {
		return false
	}
	
	if iter.hasUpperBound && iter.currentKey > iter.upperBound {
		return false
	}
	
	return true
}

// HasLowerBound returns true if iterator has a lower bound
func (iter *BTreeIterator) HasLowerBound() bool {
	return iter.hasLowerBound
}

// HasUpperBound returns true if iterator has an upper bound
func (iter *BTreeIterator) HasUpperBound() bool {
	return iter.hasUpperBound
}

// LowerBound returns the lower bound value
func (iter *BTreeIterator) LowerBound() interface{} {
	if !iter.hasLowerBound {
		return nil
	}
	return int64(iter.lowerBound)
}

// UpperBound returns the upper bound value
func (iter *BTreeIterator) UpperBound() interface{} {
	if !iter.hasUpperBound {
		return nil
	}
	return int64(iter.upperBound)
}

// Helper methods

// loadPage loads a page and caches it
func (iter *BTreeIterator) loadPage(pageID uint64) bool {
	if cached, ok := iter.pageCache[pageID]; ok {
		iter.currentPage = cached
		iter.currentPageID = pageID
		return true
	}
	
	page, err := iter.btree.pageManager.ReadPage(pageID)
	if err != nil {
		iter.err = err
		return false
	}
	
	iter.currentPage = page
	iter.currentPageID = pageID
	iter.pageCache[pageID] = page
	return true
}

// loadCurrentEntry loads the current entry from the current page
func (iter *BTreeIterator) loadCurrentEntry() bool {
	if iter.currentPage == nil {
		return false
	}
	
	entries, err := iter.btree.readLeafEntries(iter.currentPage)
	if err != nil {
		iter.err = err
		return false
	}
	
	if iter.currentEntryIdx >= len(entries) {
		return false
	}
	
	entry := entries[iter.currentEntryIdx]
	iter.currentKey = entry.Key
	
	// Extract bitmap from the Value
	if entry.Value.IsRowNumber() {
		// For single row entries, create a bitmap with just that row
		iter.currentRows = roaring.New()
		iter.currentRows.Add(uint32(entry.Value.GetRowNumber()))
	} else {
		// For bitmap entries, load the full bitmap
		bitmap, err := iter.btree.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
		if err != nil {
			iter.err = err
			return false
		}
		
		if bitmap.GetCardinality() > 0 {
			iter.currentRows = bitmap
		} else {
			iter.err = fmt.Errorf("empty bitmap for key %d", entry.Key)
			return false
		}
	}
	
	iter.valid = true
	return true
}

// checkBounds checks if current position is within bounds
func (iter *BTreeIterator) checkBounds() bool {
	if !iter.InBounds() {
		iter.valid = false
		return false
	}
	return true
}