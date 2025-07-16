package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// StringSegment manages unique strings in a compact segment
type StringSegment struct {
	pageManager *PageManager
	rootPageID  uint64
	stringMap   map[string]uint64 // string -> offset mapping
	offsetMap   map[uint64]string // offset -> string mapping
	entries     []StringEntry
	totalSize   uint64
}

// NewStringSegment creates a new string segment
func NewStringSegment(pm *PageManager) *StringSegment {
	return &StringSegment{
		pageManager: pm,
		stringMap:   make(map[string]uint64),
		offsetMap:   make(map[uint64]string),
		entries:     make([]StringEntry, 0),
	}
}

// hashString computes a 32-bit hash of a string
func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// AddString adds a string to the segment and returns its offset
func (ss *StringSegment) AddString(str string) uint64 {
	// Check if string already exists
	if offset, exists := ss.stringMap[str]; exists {
		return offset
	}
	
	// Assign new offset
	offset := ss.totalSize
	length := uint32(len(str))
	hash := hashString(str)
	
	// Create entry
	entry := StringEntry{
		Offset: offset,
		Length: length,
		Hash:   hash,
	}
	
	// Update maps
	ss.stringMap[str] = offset
	ss.offsetMap[offset] = str
	ss.entries = append(ss.entries, entry)
	
	// Update total size (4 bytes for length + string data)
	ss.totalSize += uint64(4 + length)
	
	return offset
}

// GetString retrieves a string by its offset
func (ss *StringSegment) GetString(offset uint64) (string, error) {
	if str, exists := ss.offsetMap[offset]; exists {
		return str, nil
	}
	
	// Try to load from disk if not in memory
	return ss.loadStringFromDisk(offset)
}

// FindOffset finds the offset of a string, returns false if not found
func (ss *StringSegment) FindOffset(str string) (uint64, bool) {
	offset, exists := ss.stringMap[str]
	return offset, exists
}

// Build creates the segment pages from collected strings
func (ss *StringSegment) Build() error {
	// Sort strings for optimal B+ tree construction
	type stringWithOffset struct {
		str    string
		offset uint64
	}
	
	sorted := make([]stringWithOffset, 0, len(ss.stringMap))
	for str, offset := range ss.stringMap {
		sorted = append(sorted, stringWithOffset{str, offset})
	}
	
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].str < sorted[j].str
	})
	
	// Rebuild offsets after sorting
	ss.stringMap = make(map[string]uint64)
	ss.offsetMap = make(map[uint64]string)
	ss.entries = make([]StringEntry, 0, len(sorted))
	currentOffset := uint64(0)
	
	for _, item := range sorted {
		length := uint32(len(item.str))
		hash := hashString(item.str)
		
		entry := StringEntry{
			Offset: currentOffset,
			Length: length,
			Hash:   hash,
		}
		
		ss.stringMap[item.str] = currentOffset
		ss.offsetMap[currentOffset] = item.str
		ss.entries = append(ss.entries, entry)
		
		currentOffset += uint64(4 + length)
	}
	
	ss.totalSize = currentOffset
	
	// Write to pages
	return ss.writeToPages()
}

// writeToPages writes the string segment to disk pages
func (ss *StringSegment) writeToPages() error {
	// First page is the directory header
	dirPage := ss.pageManager.AllocatePage(PageTypeStringSegment)
	ss.rootPageID = dirPage.Header.PageID
	
	// Write directory header
	buf := bytes.NewBuffer(dirPage.Data[:0])
	binary.Write(buf, ByteOrder, uint32(len(ss.entries))) // string count
	binary.Write(buf, ByteOrder, uint32(ss.totalSize))    // total size
	binary.Write(buf, ByteOrder, uint64(0))               // first directory entry page (will be set later)
	
	// Calculate entries per page
	pageSize := ss.pageManager.GetPageSize()
	entriesPerPage := (pageSize - PageHeaderSize - 8) / 16 // 16 bytes per entry, 8 bytes for next page pointer
	
	// Allocate pages for directory entries
	var firstDirEntryPage *Page
	var currentDirPage *Page
	var previousDirPage *Page
	entryIndex := 0
	
	for entryIndex < len(ss.entries) {
		// Allocate a new directory entry page
		newDirPage := ss.pageManager.AllocatePage(PageTypeStringSegment)
		newDirPage.Header.ParentPageID = dirPage.Header.PageID
		
		if firstDirEntryPage == nil {
			firstDirEntryPage = newDirPage
			// Update header with first directory entry page
			buf := bytes.NewBuffer(dirPage.Data[:16])
			buf.Reset()
			binary.Write(buf, ByteOrder, uint32(len(ss.entries))) // string count
			binary.Write(buf, ByteOrder, uint32(ss.totalSize))    // total size
			binary.Write(buf, ByteOrder, newDirPage.Header.PageID) // first dir entry page
		}
		
		if previousDirPage != nil {
			// Link previous directory page to this one
			// The next page pointer is already reserved in the data, just update it
			nextPtrOffset := previousDirPage.Header.DataSize - 8
			ByteOrder.PutUint64(previousDirPage.Data[nextPtrOffset:], newDirPage.Header.PageID)
			if err := ss.pageManager.WritePage(previousDirPage); err != nil {
				return err
			}
		}
		
		// Write entries to this page
		pageBuf := bytes.NewBuffer(newDirPage.Data[:0])
		entriesInThisPage := 0
		
		for entryIndex < len(ss.entries) && entriesInThisPage < entriesPerPage {
			entry := ss.entries[entryIndex]
			binary.Write(pageBuf, ByteOrder, entry.Offset)
			binary.Write(pageBuf, ByteOrder, entry.Length)
			binary.Write(pageBuf, ByteOrder, entry.Hash)
			entryIndex++
			entriesInThisPage++
		}
		
		newDirPage.Header.EntryCount = uint32(entriesInThisPage)
		
		// Reserve space for next page pointer
		binary.Write(pageBuf, ByteOrder, uint64(0))
		
		newDirPage.Header.DataSize = uint32(pageBuf.Len())
		
		currentDirPage = newDirPage
		previousDirPage = currentDirPage
	}
	
	// Write the last directory page
	if currentDirPage != nil {
		if err := ss.pageManager.WritePage(currentDirPage); err != nil {
			return err
		}
	}
	
	// Update and write the main directory header page
	dirPage.Header.DataSize = 16 // header size
	if err := ss.pageManager.WritePage(dirPage); err != nil {
		return err
	}
	
	// Write string data pages
	currentPage := ss.pageManager.AllocatePage(PageTypeStringSegment)
	currentPage.Header.ParentPageID = dirPage.Header.PageID
	pageOffset := 0
	
	// Link directory to first data page
	dirPage.Header.NextPageID = currentPage.Header.PageID
	
	// Write ordered strings based on offsets
	orderedStrings := make([]string, 0, len(ss.offsetMap))
	offsets := make([]uint64, 0, len(ss.offsetMap))
	for offset := range ss.offsetMap {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	for _, offset := range offsets {
		orderedStrings = append(orderedStrings, ss.offsetMap[offset])
	}
	
	for _, str := range orderedStrings {
		strBytes := []byte(str)
		lengthBytes := make([]byte, 4)
		ByteOrder.PutUint32(lengthBytes, uint32(len(strBytes)))
		
		// Check if string fits in current page
		totalBytes := 4 + len(strBytes)
		if pageOffset+totalBytes > len(currentPage.Data) {
			// Allocate new page and link it
			newPage := ss.pageManager.AllocatePage(PageTypeStringSegment)
			newPage.Header.ParentPageID = dirPage.Header.PageID
			
			// Set next page pointer and data size
			currentPage.Header.NextPageID = newPage.Header.PageID
			currentPage.Header.DataSize = uint32(pageOffset)
			
			// Write current page with the link
			if err := ss.pageManager.WritePage(currentPage); err != nil {
				return err
			}
			
			currentPage = newPage
			pageOffset = 0
		}
		
		// Write length and string data
		copy(currentPage.Data[pageOffset:], lengthBytes)
		copy(currentPage.Data[pageOffset+4:], strBytes)
		pageOffset += totalBytes
	}
	
	// Write final page
	currentPage.Header.DataSize = uint32(pageOffset)
	if err := ss.pageManager.WritePage(currentPage); err != nil {
		return err
	}
	
	// Update and rewrite directory page with correct NextPageID
	return ss.pageManager.WritePage(dirPage)
}

// loadStringFromDisk loads a string from disk given its offset
func (ss *StringSegment) loadStringFromDisk(offset uint64) (string, error) {
	// Load directory page
	dirPage, err := ss.pageManager.ReadPage(ss.rootPageID)
	if err != nil {
		return "", err
	}
	
	// Find which data page contains this offset
	dataPageID := dirPage.Header.NextPageID
	currentOffset := uint64(0)
	
	for dataPageID != 0 {
		dataPage, err := ss.pageManager.ReadPage(dataPageID)
		if err != nil {
			return "", err
		}
		
		// Check if offset is in this page
		if offset >= currentOffset && offset < currentOffset+uint64(dataPage.Header.DataSize) {
			// Found the page, extract the string
			pageOffset := offset - currentOffset
			
			// Read length
			if pageOffset+4 > uint64(dataPage.Header.DataSize) {
				return "", ErrInvalidOffset
			}
			
			length := ByteOrder.Uint32(dataPage.Data[pageOffset : pageOffset+4])
			
			// Read string data
			if pageOffset+4+uint64(length) > uint64(dataPage.Header.DataSize) {
				return "", ErrInvalidOffset
			}
			
			strBytes := dataPage.Data[pageOffset+4 : pageOffset+4+uint64(length)]
			return string(strBytes), nil
		}
		
		currentOffset += uint64(dataPage.Header.DataSize)
		dataPageID = dataPage.Header.NextPageID
	}
	
	return "", ErrStringNotFound
}

// StringComparator implements string comparison using offsets
type StringComparator struct {
	segment *StringSegment
}

// NewStringComparator creates a new string comparator
func NewStringComparator(segment *StringSegment) *StringComparator {
	return &StringComparator{segment: segment}
}

// Compare compares two string offsets
func (sc *StringComparator) Compare(a, b uint64) (int, error) {
	if a == b {
		return 0, nil
	}
	
	strA, err := sc.segment.GetString(a)
	if err != nil {
		return 0, err
	}
	
	strB, err := sc.segment.GetString(b)
	if err != nil {
		return 0, err
	}
	
	return strings.Compare(strA, strB), nil
}

// FindNextGreaterOffset finds the smallest offset with string > target
func (ss *StringSegment) FindNextGreaterOffset(target string) (uint64, error) {
	// Sort entries by string value
	type offsetString struct {
		offset uint64
		str    string
	}
	
	sorted := make([]offsetString, 0, len(ss.offsetMap))
	for offset, str := range ss.offsetMap {
		sorted = append(sorted, offsetString{offset, str})
	}
	
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].str < sorted[j].str
	})
	
	// Binary search for next greater
	left, right := 0, len(sorted)-1
	result := uint64(0)
	found := false
	
	for left <= right {
		mid := (left + right) / 2
		if sorted[mid].str > target {
			result = sorted[mid].offset
			found = true
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	
	if !found {
		return 0, fmt.Errorf("no string greater than %s found", target)
	}
	
	return result, nil
}

// GetOffsetsInRange returns all offsets for strings in the range [min, max]
func (ss *StringSegment) GetOffsetsInRange(min, max string) []uint64 {
	// Sort entries by string value
	type offsetString struct {
		offset uint64
		str    string
	}
	
	sorted := make([]offsetString, 0, len(ss.offsetMap))
	for offset, str := range ss.offsetMap {
		sorted = append(sorted, offsetString{offset, str})
	}
	
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].str < sorted[j].str
	})
	
	// Find all offsets in range
	offsets := make([]uint64, 0)
	for _, entry := range sorted {
		if entry.str >= min && entry.str <= max {
			offsets = append(offsets, entry.offset)
		} else if entry.str > max {
			break
		}
	}
	
	return offsets
}

// FindNextSmallerOffset finds the largest offset with string < target
func (ss *StringSegment) FindNextSmallerOffset(target string) (uint64, error) {
	// Sort entries by string value
	type offsetString struct {
		offset uint64
		str    string
	}
	
	sorted := make([]offsetString, 0, len(ss.offsetMap))
	for offset, str := range ss.offsetMap {
		sorted = append(sorted, offsetString{offset, str})
	}
	
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].str < sorted[j].str
	})
	
	// Binary search for next smaller
	left, right := 0, len(sorted)-1
	result := uint64(0)
	found := false
	
	for left <= right {
		mid := (left + right) / 2
		if sorted[mid].str < target {
			result = sorted[mid].offset
			found = true
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	if !found {
		return 0, fmt.Errorf("no string smaller than %s found", target)
	}
	
	return result, nil
}

// loadDirectory loads the string directory from disk to rebuild mappings
func (ss *StringSegment) loadDirectory() error {
	if ss.rootPageID == 0 {
		return fmt.Errorf("no root page ID set")
	}
	
	// Initialize maps if nil
	if ss.stringMap == nil {
		ss.stringMap = make(map[string]uint64)
	}
	if ss.offsetMap == nil {
		ss.offsetMap = make(map[uint64]string)
	}
	
	// Load directory page
	dirPage, err := ss.pageManager.ReadPage(ss.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to read directory page %d: %w", ss.rootPageID, err)
	}
	
	// Verify it's a string segment page
	if dirPage.Header.PageType != PageTypeStringSegment {
		return fmt.Errorf("expected string segment page, got %v", dirPage.Header.PageType)
	}
	
	// Read directory header
	buf := bytes.NewReader(dirPage.Data[:])
	var stringCount uint32
	var totalSize uint32
	var firstDirEntryPageID uint64
	
	binary.Read(buf, ByteOrder, &stringCount)
	binary.Read(buf, ByteOrder, &totalSize)
	binary.Read(buf, ByteOrder, &firstDirEntryPageID)
	
	ss.entries = make([]StringEntry, 0, stringCount)
	ss.totalSize = uint64(totalSize)
	
	// Read directory entries from multiple pages
	dirEntryPageID := firstDirEntryPageID
	
	for dirEntryPageID != 0 && len(ss.entries) < int(stringCount) {
		dirEntryPage, err := ss.pageManager.ReadPage(dirEntryPageID)
		if err != nil {
			return fmt.Errorf("failed to read directory entry page %d: %w", dirEntryPageID, err)
		}
		
		// Read entries from this page
		entryBuf := bytes.NewReader(dirEntryPage.Data[:dirEntryPage.Header.DataSize])
		entriesInThisPage := int(dirEntryPage.Header.EntryCount)
		
		for i := 0; i < entriesInThisPage && len(ss.entries) < int(stringCount); i++ {
			var entry StringEntry
			binary.Read(entryBuf, ByteOrder, &entry.Offset)
			binary.Read(entryBuf, ByteOrder, &entry.Length)
			binary.Read(entryBuf, ByteOrder, &entry.Hash)
			ss.entries = append(ss.entries, entry)
		}
		
		// Read next page pointer (it's the last 8 bytes of the data)
		nextPtrOffset := dirEntryPage.Header.DataSize - 8
		dirEntryPageID = ByteOrder.Uint64(dirEntryPage.Data[nextPtrOffset:])
	}
	
	// Load actual strings from data pages
	dataPageID := dirPage.Header.NextPageID
	currentOffset := uint64(0)
	
	for dataPageID != 0 {
		dataPage, err := ss.pageManager.ReadPage(dataPageID)
		if err != nil {
			return err
		}
		
		pageData := dataPage.Data[:dataPage.Header.DataSize]
		pageOffset := 0
		
		for pageOffset < len(pageData) {
			// Read length
			if pageOffset+4 > len(pageData) {
				break
			}
			
			length := ByteOrder.Uint32(pageData[pageOffset : pageOffset+4])
			pageOffset += 4
			
			// Read string
			if pageOffset+int(length) > len(pageData) {
				break
			}
			
			str := string(pageData[pageOffset : pageOffset+int(length)])
			pageOffset += int(length)
			
			// Add to maps
			ss.stringMap[str] = currentOffset
			ss.offsetMap[currentOffset] = str
			
			currentOffset += uint64(4 + length)
		}
		
		dataPageID = dataPage.Header.NextPageID
	}
	
	ss.totalSize = currentOffset
	
	return nil
}