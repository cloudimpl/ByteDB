package columnar

import (
	"bytes"
	"encoding/binary"
)

// BTreeStreamingBuilder builds a B+ tree in streaming fashion without buffering
type BTreeStreamingBuilder struct {
	btree           *BPlusTree
	currentLeaf     *Page
	leafBuffer      *bytes.Buffer
	leafCount       uint32
	entriesInLeaf   uint32
	
	// Track first keys for building internal nodes
	// Each level stores the first key of each node at that level
	internalLevels  [][]uint64  // First keys at each level
	levelPages      [][]uint64  // Page IDs at each level
}

// NewBTreeStreamingBuilder creates a new streaming builder
func NewBTreeStreamingBuilder(btree *BPlusTree) *BTreeStreamingBuilder {
	return &BTreeStreamingBuilder{
		btree:          btree,
		leafBuffer:     new(bytes.Buffer),
		internalLevels: make([][]uint64, 0),
		levelPages:     make([][]uint64, 0),
	}
}

// Add adds a key-value pair to the B-tree (must be called with sorted keys)
func (b *BTreeStreamingBuilder) Add(key uint64, value Value) error {
	keySize := GetDataTypeSize(b.btree.dataType)
	entrySize := keySize + 8 // key + value (8 bytes for Value)
	
	// Check if we need to start a new leaf
	maxEntriesPerLeaf := (b.btree.pageManager.pageSize - PageHeaderSize) / entrySize
	if b.entriesInLeaf >= uint32(maxEntriesPerLeaf) {
		if err := b.flushLeaf(); err != nil {
			return err
		}
	}
	
	// Write key
	keyBuf := EncodeKey(key, b.btree.dataType)
	if _, err := b.leafBuffer.Write(keyBuf); err != nil {
		return err
	}
	
	// Write value
	if err := binary.Write(b.leafBuffer, ByteOrder, value.Data); err != nil {
		return err
	}
	
	b.entriesInLeaf++
	return nil
}

// flushLeaf writes the current leaf page and updates parent tracking
func (b *BTreeStreamingBuilder) flushLeaf() error {
	if b.entriesInLeaf == 0 {
		return nil
	}
	
	// Allocate a new page for the leaf
	leafPage := b.btree.pageManager.AllocatePage(PageTypeBTreeLeaf)
	leafPageID := leafPage.Header.PageID
	
	// Update leaf page header
	leafPage.Header.EntryCount = b.entriesInLeaf
	leafPage.Header.DataSize = uint32(b.leafBuffer.Len())
	
	// Copy buffer contents to page data
	copy(leafPage.Data, b.leafBuffer.Bytes())
	
	// Link with previous leaf
	if b.currentLeaf != nil {
		b.currentLeaf.Header.NextPageID = leafPageID
		leafPage.Header.PrevPageID = b.currentLeaf.Header.PageID
		// Write the updated previous leaf
		if err := b.btree.pageManager.WritePage(b.currentLeaf); err != nil {
			return err
		}
	}
	
	// Get first key of this leaf for parent tracking
	firstKey := b.getFirstKeyFromBuffer()
	
	// Add to level 0 (leaf level)
	b.addToLevel(0, firstKey, leafPageID)
	
	// Save current leaf for linking
	b.currentLeaf = leafPage
	b.leafCount++
	
	// Reset for next leaf
	b.leafBuffer.Reset()
	b.entriesInLeaf = 0
	
	return nil
}

// getFirstKeyFromBuffer extracts the first key from the buffer
func (b *BTreeStreamingBuilder) getFirstKeyFromBuffer() uint64 {
	keySize := GetDataTypeSize(b.btree.dataType)
	keyBuf := b.leafBuffer.Bytes()[:keySize]
	return DecodeKey(keyBuf, b.btree.dataType)
}

// addToLevel adds a node to the specified level
func (b *BTreeStreamingBuilder) addToLevel(level int, firstKey uint64, pageID uint64) error {
	// Ensure we have enough levels
	for len(b.internalLevels) <= level {
		b.internalLevels = append(b.internalLevels, make([]uint64, 0))
		b.levelPages = append(b.levelPages, make([]uint64, 0))
	}
	
	// Add to this level
	b.internalLevels[level] = append(b.internalLevels[level], firstKey)
	b.levelPages[level] = append(b.levelPages[level], pageID)
	
	// Check if we need to flush this level
	maxChildrenPerInternal := (b.btree.pageManager.pageSize - PageHeaderSize) / 8
	if len(b.levelPages[level]) >= maxChildrenPerInternal {
		return b.flushInternalLevel(level)
	}
	
	return nil
}

// flushInternalLevel writes an internal node at the specified level
func (b *BTreeStreamingBuilder) flushInternalLevel(level int) error {
	if len(b.levelPages[level]) == 0 {
		return nil
	}
	
	// Allocate page for internal node
	internalPage := b.btree.pageManager.AllocatePage(PageTypeBTreeInternal)
	pageID := internalPage.Header.PageID
	
	// Write keys (not child page IDs) - internal nodes store keys only
	buf := new(bytes.Buffer)
	
	// Write the first key of each child (except the first child)
	// The number of keys is one less than the number of children
	numKeys := len(b.levelPages[level]) - 1
	for i := 0; i < numKeys; i++ {
		// Get the first key from child i+1
		firstKey := b.internalLevels[level][i+1]
		keyBuf := EncodeKey(firstKey, b.btree.dataType)
		if _, err := buf.Write(keyBuf); err != nil {
			return err
		}
	}
	
	// Update internal page header
	internalPage.Header.EntryCount = uint32(numKeys)
	internalPage.Header.DataSize = uint32(buf.Len())
	
	copy(internalPage.Data, buf.Bytes())
	
	// Write the page
	if err := b.btree.pageManager.WritePage(internalPage); err != nil {
		return err
	}
	
	// Update child page mapping - store it the same way regular merge does
	b.btree.childPageMap[pageID] = make([]uint64, len(b.levelPages[level]))
	copy(b.btree.childPageMap[pageID], b.levelPages[level])
	
	// Note: In streaming mode, we cannot update parent pointers in child pages
	// because child pages are written before parent pages exist.
	// The childPageMap stored above is sufficient for B-tree navigation.
	
	// Get first key of this internal node (first key of its first child)
	firstKey := b.internalLevels[level][0]
	
	// Clear this level
	b.internalLevels[level] = b.internalLevels[level][:0]
	b.levelPages[level] = b.levelPages[level][:0]
	
	// Add to parent level
	return b.addToLevel(level+1, firstKey, pageID)
}

// Finish completes the B-tree construction
func (b *BTreeStreamingBuilder) Finish() error {
	// Flush any remaining leaf
	if b.entriesInLeaf > 0 {
		if err := b.flushLeaf(); err != nil {
			return err
		}
	}
	
	// Write the last leaf page
	if b.currentLeaf != nil {
		if err := b.btree.pageManager.WritePage(b.currentLeaf); err != nil {
			return err
		}
	}
	
	// Flush all internal levels from bottom to top
	// Important: We need to flush in a way that doesn't keep adding new levels
	levelsToFlush := len(b.levelPages)
	for level := 0; level < levelsToFlush; level++ {
		if len(b.levelPages[level]) > 0 {
			if err := b.flushInternalLevel(level); err != nil {
				return err
			}
		}
	}
	
	// Find the root (highest level with exactly one page)
	for level := len(b.levelPages) - 1; level >= 0; level-- {
		if len(b.levelPages[level]) == 1 {
			b.btree.rootPageID = b.levelPages[level][0]
			b.btree.height = uint32(level + 1)
			break
		}
	}
	
	// If no internal nodes were created, the last leaf is the root
	if b.btree.rootPageID == 0 && b.currentLeaf != nil {
		b.btree.rootPageID = b.currentLeaf.Header.PageID
		b.btree.height = 1
	}
	
	// Now that the tree is fully built, update parent pointers
	// This is needed for ReconstructChildPageMapping to work when the file is reopened
	if err := b.updateParentPointers(); err != nil {
		return err
	}
	
	return nil
}

// updateParentPointers traverses the tree and sets parent pointers in all pages
func (b *BTreeStreamingBuilder) updateParentPointers() error {
	if b.btree.rootPageID == 0 {
		return nil
	}
	
	// Start from root with no parent
	return b.updateParentPointersRecursive(b.btree.rootPageID, 0)
}

// updateParentPointersRecursive recursively updates parent pointers
func (b *BTreeStreamingBuilder) updateParentPointersRecursive(pageID uint64, parentID uint64) error {
	// Read the page
	page, err := b.btree.pageManager.ReadPage(pageID)
	if err != nil {
		return err
	}
	
	// Update parent pointer
	page.Header.ParentPageID = parentID
	
	// Write the page back
	if err := b.btree.pageManager.WritePage(page); err != nil {
		return err
	}
	
	// If this is an internal node, recursively update its children
	if page.Header.PageType == PageTypeBTreeInternal {
		if children, exists := b.btree.childPageMap[pageID]; exists {
			for _, childPageID := range children {
				if err := b.updateParentPointersRecursive(childPageID, pageID); err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// EncodeKey encodes a key based on data type
func EncodeKey(key uint64, dataType DataType) []byte {
	size := GetDataTypeSize(dataType)
	buf := make([]byte, size)
	
	switch dataType {
	case DataTypeUint64, DataTypeInt64, DataTypeString:
		binary.LittleEndian.PutUint64(buf, key)
	case DataTypeUint32, DataTypeInt32:
		binary.LittleEndian.PutUint32(buf, uint32(key))
	case DataTypeUint16, DataTypeInt16:
		binary.LittleEndian.PutUint16(buf, uint16(key))
	case DataTypeUint8, DataTypeInt8, DataTypeBool:
		buf[0] = byte(key)
	}
	
	return buf
}

// DecodeKey decodes a key based on data type
func DecodeKey(buf []byte, dataType DataType) uint64 {
	switch dataType {
	case DataTypeUint64, DataTypeInt64, DataTypeString:
		return binary.LittleEndian.Uint64(buf)
	case DataTypeUint32, DataTypeInt32:
		return uint64(binary.LittleEndian.Uint32(buf))
	case DataTypeUint16, DataTypeInt16:
		return uint64(binary.LittleEndian.Uint16(buf))
	case DataTypeUint8, DataTypeInt8, DataTypeBool:
		return uint64(buf[0])
	default:
		return 0
	}
}