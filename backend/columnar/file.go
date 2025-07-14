package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// ColumnarFile represents a complete columnar file
type ColumnarFile struct {
	filename      string
	pageManager   *PageManager
	header        *FileHeader
	columns       map[string]*Column
	columnOrder   []string
}

// Column represents a single column with its metadata and indexes
type Column struct {
	metadata      *ColumnMetadata
	btree         *BPlusTree
	stringSegment *StringSegment // For string columns
	bitmapManager *BitmapManager
}

// CreateFile creates a new columnar file
func CreateFile(filename string) (*ColumnarFile, error) {
	// Create page manager
	pm, err := NewPageManager(filename, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}
	
	// Create file header
	header := &FileHeader{
		MagicNumber:       MagicNumber,
		MajorVersion:      MajorVersion,
		MinorVersion:      MinorVersion,
		PageSize:          PageSize,
		CreationTimestamp: uint64(time.Now().Unix()),
	}
	
	cf := &ColumnarFile{
		filename:    filename,
		pageManager: pm,
		header:      header,
		columns:     make(map[string]*Column),
		columnOrder: make([]string, 0),
	}
	
	// Write initial header
	if err := cf.writeHeader(); err != nil {
		pm.Close()
		return nil, err
	}
	
	return cf, nil
}

// OpenFile opens an existing columnar file
func OpenFile(filename string) (*ColumnarFile, error) {
	// Open page manager
	pm, err := NewPageManager(filename, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open page manager: %w", err)
	}
	
	cf := &ColumnarFile{
		filename:    filename,
		pageManager: pm,
		columns:     make(map[string]*Column),
		columnOrder: make([]string, 0),
	}
	
	// Read header
	if err := cf.readHeader(); err != nil {
		pm.Close()
		return nil, err
	}
	
	// Read column metadata
	if err := cf.readColumnMetadata(); err != nil {
		pm.Close()
		return nil, err
	}
	
	return cf, nil
}

// AddColumn adds a new column to the file
func (cf *ColumnarFile) AddColumn(name string, dataType DataType, nullable bool) error {
	if _, exists := cf.columns[name]; exists {
		return fmt.Errorf("column %s already exists", name)
	}
	
	// Create column metadata
	metadata := &ColumnMetadata{
		DataType:   dataType,
		IsNullable: nullable,
	}
	copy(metadata.ColumnName[:], name)
	
	// Create column
	col := &Column{
		metadata:      metadata,
		btree:         NewBPlusTree(cf.pageManager, dataType, nil),
		bitmapManager: NewBitmapManager(cf.pageManager),
	}
	
	// For string columns, create string segment
	if dataType == DataTypeString {
		col.stringSegment = NewStringSegment(cf.pageManager)
	}
	
	cf.columns[name] = col
	cf.columnOrder = append(cf.columnOrder, name)
	cf.header.ColumnCount++
	
	return nil
}

// LoadIntColumn loads data into an integer column
func (cf *ColumnarFile) LoadIntColumn(columnName string, data []struct{ Key int64; RowNum uint64 }) error {
	col, exists := cf.columns[columnName]
	if !exists {
		return fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeInt64 {
		return fmt.Errorf("column %s is not int64 type", columnName)
	}
	
	// Convert to uint64 keys for B+ tree
	keyRows := make([]struct{ Key uint64; RowNum uint64 }, len(data))
	for i, d := range data {
		keyRows[i] = struct{ Key uint64; RowNum uint64 }{
			Key:    uint64(d.Key),
			RowNum: d.RowNum,
		}
	}
	
	// Bulk load with duplicates
	if err := col.btree.BulkLoadWithDuplicates(keyRows); err != nil {
		return err
	}
	
	// Update metadata
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(data))
	
	return nil
}

// LoadStringColumn loads data into a string column
func (cf *ColumnarFile) LoadStringColumn(columnName string, data []struct{ Key string; RowNum uint64 }) error {
	col, exists := cf.columns[columnName]
	if !exists {
		return fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeString {
		return fmt.Errorf("column %s is not string type", columnName)
	}
	
	// Build string to rows mapping
	stringRows := make(map[string][]uint64)
	for _, d := range data {
		stringRows[d.Key] = append(stringRows[d.Key], d.RowNum)
	}
	
	// Add strings to segment first
	for str := range stringRows {
		col.stringSegment.AddString(str)
	}
	
	// Build string segment (this will sort and rebuild offsets)
	if err := col.stringSegment.Build(); err != nil {
		return err
	}
	
	// NOW collect the correct offsets after building
	keyRows := make([]struct{ Key uint64; RowNum uint64 }, 0)
	for str, rows := range stringRows {
		offset, found := col.stringSegment.FindOffset(str)
		if !found {
			return fmt.Errorf("string %s not found after build", str)
		}
		for _, row := range rows {
			keyRows = append(keyRows, struct{ Key uint64; RowNum uint64 }{
				Key:    offset,
				RowNum: row,
			})
		}
	}
	
	// Create comparator for string offsets
	col.btree = NewBPlusTree(cf.pageManager, DataTypeString, NewStringComparator(col.stringSegment))
	
	// Bulk load B+ tree
	if err := col.btree.BulkLoadWithDuplicates(keyRows); err != nil {
		return err
	}
	
	// Update metadata
	col.metadata.RootPageID = col.btree.GetRootPageID()
	col.metadata.TotalKeys = uint64(len(data))
	col.metadata.StringSegmentStart = col.stringSegment.rootPageID
	col.metadata.DistinctCount = uint64(len(stringRows))
	
	return nil
}

// QueryInt performs an equality query on an integer column
func (cf *ColumnarFile) QueryInt(columnName string, value int64) ([]uint64, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeInt64 {
		return nil, fmt.Errorf("column %s is not int64 type", columnName)
	}
	
	return col.btree.Find(uint64(value))
}

// QueryString performs an equality query on a string column
func (cf *ColumnarFile) QueryString(columnName string, value string) ([]uint64, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeString {
		return nil, fmt.Errorf("column %s is not string type", columnName)
	}
	
	// Find string offset
	offset, found := col.stringSegment.FindOffset(value)
	if !found {
		return []uint64{}, nil // String not found
	}
	
	return col.btree.Find(offset)
}

// RangeQueryInt performs a range query on an integer column
func (cf *ColumnarFile) RangeQueryInt(columnName string, min, max int64) ([]uint64, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeInt64 {
		return nil, fmt.Errorf("column %s is not int64 type", columnName)
	}
	
	return col.btree.RangeSearch(uint64(min), uint64(max))
}

// RangeQueryString performs a range query on a string column
func (cf *ColumnarFile) RangeQueryString(columnName string, min, max string) ([]uint64, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeString {
		return nil, fmt.Errorf("column %s is not string type", columnName)
	}
	
	// Get all offsets for strings in range
	offsets := col.stringSegment.GetOffsetsInRange(min, max)
	if len(offsets) == 0 {
		return []uint64{}, nil
	}
	
	// Query each offset and collect results
	results := make([]uint64, 0)
	for _, offset := range offsets {
		rows, err := col.btree.Find(offset)
		if err != nil {
			continue
		}
		results = append(results, rows...)
	}
	
	return results, nil
}

// Close closes the columnar file
func (cf *ColumnarFile) Close() error {
	// Update file size in header
	cf.header.FileSize = uint64(cf.pageManager.GetPageCount()) * PageSize
	cf.header.TotalPages = cf.pageManager.GetPageCount()
	cf.header.RowCount = cf.calculateTotalRows()
	
	// Write updated header
	if err := cf.writeHeader(); err != nil {
		return err
	}
	
	// Write column metadata
	if err := cf.writeColumnMetadata(); err != nil {
		return err
	}
	
	// Close page manager
	return cf.pageManager.Close()
}

// writeHeader writes the file header
func (cf *ColumnarFile) writeHeader() error {
	headerPage, err := cf.pageManager.ReadPage(0)
	if err != nil {
		return err
	}
	
	buf := bytes.NewBuffer(headerPage.Data[:0])
	
	// Write header fields
	binary.Write(buf, ByteOrder, cf.header.MagicNumber)
	binary.Write(buf, ByteOrder, cf.header.MajorVersion)
	binary.Write(buf, ByteOrder, cf.header.MinorVersion)
	binary.Write(buf, ByteOrder, cf.header.FileSize)
	binary.Write(buf, ByteOrder, cf.header.PageSize)
	binary.Write(buf, ByteOrder, cf.header.RowCount)
	binary.Write(buf, ByteOrder, cf.header.ColumnCount)
	binary.Write(buf, ByteOrder, cf.header.MetadataOffset)
	binary.Write(buf, ByteOrder, cf.header.PageDirectoryOffset)
	binary.Write(buf, ByteOrder, cf.header.ValueStorageOffset)
	binary.Write(buf, ByteOrder, cf.header.CreationTimestamp)
	binary.Write(buf, ByteOrder, cf.header.TotalPages)
	binary.Write(buf, ByteOrder, cf.header.CompressionType)
	binary.Write(buf, ByteOrder, cf.header.HeaderChecksum)
	
	// Also write column metadata page IDs after the header
	// For now, we know metadata starts at page 1
	binary.Write(buf, ByteOrder, uint64(1)) // First metadata page
	
	headerPage.Header.DataSize = uint32(buf.Len())
	return cf.pageManager.WritePage(headerPage)
}

// readHeader reads the file header
func (cf *ColumnarFile) readHeader() error {
	headerPage, err := cf.pageManager.ReadPage(0)
	if err != nil {
		return err
	}
	
	if headerPage.Header.PageType != PageTypeFileHeader {
		return ErrInvalidPageType
	}
	
	buf := bytes.NewReader(headerPage.Data[:])
	cf.header = &FileHeader{}
	
	// Read header fields
	binary.Read(buf, ByteOrder, &cf.header.MagicNumber)
	binary.Read(buf, ByteOrder, &cf.header.MajorVersion)
	binary.Read(buf, ByteOrder, &cf.header.MinorVersion)
	binary.Read(buf, ByteOrder, &cf.header.FileSize)
	binary.Read(buf, ByteOrder, &cf.header.PageSize)
	binary.Read(buf, ByteOrder, &cf.header.RowCount)
	binary.Read(buf, ByteOrder, &cf.header.ColumnCount)
	binary.Read(buf, ByteOrder, &cf.header.MetadataOffset)
	binary.Read(buf, ByteOrder, &cf.header.PageDirectoryOffset)
	binary.Read(buf, ByteOrder, &cf.header.ValueStorageOffset)
	binary.Read(buf, ByteOrder, &cf.header.CreationTimestamp)
	binary.Read(buf, ByteOrder, &cf.header.TotalPages)
	binary.Read(buf, ByteOrder, &cf.header.CompressionType)
	binary.Read(buf, ByteOrder, &cf.header.HeaderChecksum)
	
	// Validate magic number
	if cf.header.MagicNumber != MagicNumber {
		return ErrInvalidMagicNumber
	}
	
	// Validate version
	if cf.header.MajorVersion != MajorVersion {
		return ErrInvalidVersion
	}
	
	return nil
}

// writeColumnMetadata writes column metadata
func (cf *ColumnarFile) writeColumnMetadata() error {
	// Store metadata page IDs for each column
	metadataPageIDs := make(map[string]uint64)
	
	// Don't hardcode page IDs - let the page manager allocate them
	for _, colName := range cf.columnOrder {
		col := cf.columns[colName]
		
		page := cf.pageManager.AllocatePage(PageTypePageDirectory)
		metadataPageIDs[colName] = page.Header.PageID
		
		// Serialize column metadata
		buf := bytes.NewBuffer(page.Data[:0])
		
		// Write all fields
		buf.Write(col.metadata.ColumnName[:])
		binary.Write(buf, ByteOrder, col.metadata.DataType)
		binary.Write(buf, ByteOrder, col.metadata.IsNullable)
		binary.Write(buf, ByteOrder, col.metadata.Flags)
		binary.Write(buf, ByteOrder, col.metadata.RootPageID)
		binary.Write(buf, ByteOrder, col.metadata.TreeHeight)
		binary.Write(buf, ByteOrder, col.metadata.TotalKeys)
		binary.Write(buf, ByteOrder, col.metadata.NullCount)
		binary.Write(buf, ByteOrder, col.metadata.DistinctCount)
		binary.Write(buf, ByteOrder, col.metadata.MinValueOffset)
		binary.Write(buf, ByteOrder, col.metadata.MaxValueOffset)
		binary.Write(buf, ByteOrder, col.metadata.TotalSizeBytes)
		binary.Write(buf, ByteOrder, col.metadata.BitmapPagesCount)
		binary.Write(buf, ByteOrder, col.metadata.AverageKeySize)
		binary.Write(buf, ByteOrder, col.metadata.StringSegmentStart)
		
		page.Header.DataSize = uint32(buf.Len())
		if err := cf.pageManager.WritePage(page); err != nil {
			return err
		}
		
		// Store the metadata page ID
		col.metadata.MetadataPageID = page.Header.PageID
	}
	
	// Update header with first metadata page ID
	if len(metadataPageIDs) > 0 {
		cf.header.MetadataOffset = metadataPageIDs[cf.columnOrder[0]]
	}
	
	return nil
}

// readColumnMetadata reads column metadata
func (cf *ColumnarFile) readColumnMetadata() error {
	// We need to find the metadata pages - scan for PageTypePageDirectory
	pageID := uint64(1)
	columnsFound := 0
	
	for columnsFound < int(cf.header.ColumnCount) && pageID < cf.pageManager.GetPageCount() {
		page, err := cf.pageManager.ReadPage(pageID)
		if err != nil {
			pageID++
			continue
		}
		
		// Skip if not a metadata page
		if page.Header.PageType != PageTypePageDirectory {
			pageID++
			continue
		}
		
		// Deserialize column metadata
		metadata := &ColumnMetadata{}
		buf := bytes.NewReader(page.Data[:])
		
		// Read all fields
		buf.Read(metadata.ColumnName[:])
		binary.Read(buf, ByteOrder, &metadata.DataType)
		binary.Read(buf, ByteOrder, &metadata.IsNullable)
		binary.Read(buf, ByteOrder, &metadata.Flags)
		binary.Read(buf, ByteOrder, &metadata.RootPageID)
		binary.Read(buf, ByteOrder, &metadata.TreeHeight)
		binary.Read(buf, ByteOrder, &metadata.TotalKeys)
		binary.Read(buf, ByteOrder, &metadata.NullCount)
		binary.Read(buf, ByteOrder, &metadata.DistinctCount)
		binary.Read(buf, ByteOrder, &metadata.MinValueOffset)
		binary.Read(buf, ByteOrder, &metadata.MaxValueOffset)
		binary.Read(buf, ByteOrder, &metadata.TotalSizeBytes)
		binary.Read(buf, ByteOrder, &metadata.BitmapPagesCount)
		binary.Read(buf, ByteOrder, &metadata.AverageKeySize)
		binary.Read(buf, ByteOrder, &metadata.StringSegmentStart)
		
		// Extract column name
		colName := string(bytes.TrimRight(metadata.ColumnName[:], "\x00"))
		
		// Create column
		col := &Column{
			metadata:      metadata,
			bitmapManager: NewBitmapManager(cf.pageManager),
		}
		
		// Create B+ tree
		if metadata.DataType == DataTypeString && metadata.StringSegmentStart != 0 {
			// Load string segment and rebuild the in-memory mapping
			col.stringSegment = NewStringSegment(cf.pageManager)
			col.stringSegment.rootPageID = metadata.StringSegmentStart
			
			// Load string directory to rebuild mappings
			if err := col.stringSegment.loadDirectory(); err != nil {
				return fmt.Errorf("failed to load string directory for column %s: %w", colName, err)
			}
			
			col.btree = NewBPlusTree(cf.pageManager, metadata.DataType, NewStringComparator(col.stringSegment))
		} else {
			col.btree = NewBPlusTree(cf.pageManager, metadata.DataType, nil)
		}
		
		col.btree.SetRootPageID(metadata.RootPageID)
		
		cf.columns[colName] = col
		cf.columnOrder = append(cf.columnOrder, colName)
		
		columnsFound++
		pageID++
	}
	
	if columnsFound < int(cf.header.ColumnCount) {
		return fmt.Errorf("expected %d columns, found %d", cf.header.ColumnCount, columnsFound)
	}
	
	return nil
}

// calculateTotalRows calculates total unique rows across all columns
func (cf *ColumnarFile) calculateTotalRows() uint64 {
	// For now, return the max row number seen
	// In production, this would be more sophisticated
	maxRow := uint64(0)
	
	// This is a simplified calculation
	// In real implementation, we'd scan all columns
	return maxRow
}

// GetColumns returns the list of column names
func (cf *ColumnarFile) GetColumns() []string {
	return cf.columnOrder
}

// GetColumnType returns the data type of a column
func (cf *ColumnarFile) GetColumnType(columnName string) (DataType, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return 0, fmt.Errorf("column %s not found", columnName)
	}
	return col.metadata.DataType, nil
}

// GetStats returns basic statistics for a column
func (cf *ColumnarFile) GetStats(columnName string) (map[string]interface{}, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	stats := make(map[string]interface{})
	stats["total_keys"] = col.metadata.TotalKeys
	stats["distinct_count"] = col.metadata.DistinctCount
	stats["null_count"] = col.metadata.NullCount
	stats["data_type"] = col.metadata.DataType
	
	return stats, nil
}