package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// ColumnarFile represents a complete columnar file
type ColumnarFile struct {
	filename           string
	pageManager        *PageManager
	header             *FileHeader
	columns            map[string]*Column
	columnOrder        []string
	compressionOptions *CompressionOptions
	compressionStats   *CompressionStats
	readOnly           bool // Whether the file is opened in read-only mode
	deletedBitmap      *roaring.Bitmap // Bitmap tracking deleted rows
}

// Column represents a single column with its metadata and indexes
type Column struct {
	metadata      *ColumnMetadata
	btree         *BPlusTree      // Key→Row index
	rowKeyBTree   *BPlusTree      // Row→Key index (reverse lookup)
	stringSegment *StringSegment // For string columns
	bitmapManager *BitmapManager
	nullBitmap    *roaring.Bitmap // Bitmap tracking NULL rows (nil if no nulls)
}

// CreateFile creates a new columnar file with .bytedb extension
func CreateFile(filename string) (*ColumnarFile, error) {
	return CreateFileWithOptions(filename, nil)
}

// CreateFileWithOptions creates a new columnar file with compression options
func CreateFileWithOptions(filename string, options *CompressionOptions) (*ColumnarFile, error) {
	// Ensure .bytedb extension
	if !strings.HasSuffix(filename, ".bytedb") {
		return nil, fmt.Errorf("invalid filename: %s - columnar files must have .bytedb extension", filename)
	}
	
	// Use default options if none provided
	if options == nil {
		options = NewCompressionOptions()
	}
	
	// Create page manager with compression options
	pm, err := NewPageManagerWithOptions(filename, true, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}
	
	// Create file header with compression metadata
	header := &FileHeader{
		MagicNumber:         MagicNumber,
		MajorVersion:        MajorVersion,
		MinorVersion:        MinorVersion,
		PageSize:            uint32(options.PageSize),
		CreationTimestamp:   uint64(time.Now().Unix()),
		DefaultCompression:  uint8(options.DefaultPageCompression),
		LeafCompression:     uint8(options.LeafPageCompression),
		InternalCompression: uint8(options.InternalPageCompression),
		StringCompression:   uint8(options.StringPageCompression),
		BitmapCompression:   uint8(options.BitmapPageCompression),
		CompressionLevel:    uint8(options.DefaultCompressionLevel),
	}
	
	cf := &ColumnarFile{
		filename:           filename,
		pageManager:        pm,
		header:             header,
		columns:            make(map[string]*Column),
		columnOrder:        make([]string, 0),
		compressionOptions: options,
		compressionStats:   &CompressionStats{
			PageCompressions: make(map[PageType]int64),
			ColumnStats:      make(map[string]*ColumnCompressionStats),
		},
		deletedBitmap:      roaring.New(),
	}
	
	// Write initial header
	if err := cf.writeHeader(); err != nil {
		pm.Close()
		return nil, err
	}
	
	return cf, nil
}

// OpenFile opens an existing columnar file in read-write mode
func OpenFile(filename string) (*ColumnarFile, error) {
	return openFileWithMode(filename, false)
}

// OpenFileReadOnly opens an existing columnar file in read-only mode
func OpenFileReadOnly(filename string) (*ColumnarFile, error) {
	return openFileWithMode(filename, true)
}

// openFileWithMode opens an existing columnar file with specified mode
func openFileWithMode(filename string, readOnly bool) (*ColumnarFile, error) {
	// Ensure .bytedb extension
	if !strings.HasSuffix(filename, ".bytedb") {
		return nil, fmt.Errorf("invalid filename: %s - columnar files must have .bytedb extension", filename)
	}
	
	// Open page manager without compression first to read header
	var pm *PageManager
	var err error
	
	if readOnly {
		pm, err = NewPageManagerReadOnly(filename)
	} else {
		pm, err = NewPageManager(filename, false)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to open page manager: %w", err)
	}
	
	cf := &ColumnarFile{
		filename:    filename,
		pageManager: pm,
		columns:     make(map[string]*Column),
		columnOrder: make([]string, 0),
		readOnly:    readOnly,
	}
	
	// Read header
	if err := cf.readHeader(); err != nil {
		pm.Close()
		return nil, err
	}
	
	// Reconstruct compression options from header
	compressionOptions := &CompressionOptions{
		PageSize:                int(cf.header.PageSize),
		DefaultPageCompression:  CompressionType(cf.header.DefaultCompression),
		LeafPageCompression:     CompressionType(cf.header.LeafCompression),
		InternalPageCompression: CompressionType(cf.header.InternalCompression),
		StringPageCompression:   CompressionType(cf.header.StringCompression),
		BitmapPageCompression:   CompressionType(cf.header.BitmapCompression),
		DefaultCompressionLevel: CompressionLevel(cf.header.CompressionLevel),
		ColumnStrategies:        make(map[string]*ColumnCompressionStrategy),
		MinPageSizeToCompress:   512,
	}
	
	// Reinitialize page manager with compression support
	pm.compressionOptions = compressionOptions
	pm.compressors = make(map[CompressionType]Compressor)
	pm.compressionStats = &CompressionStats{
		PageCompressions: make(map[PageType]int64),
		ColumnStats:      make(map[string]*ColumnCompressionStats),
	}
	if err := pm.initializeCompressors(); err != nil {
		pm.Close()
		return nil, err
	}
	
	cf.compressionOptions = compressionOptions
	cf.compressionStats = pm.compressionStats
	
	// Read column metadata
	if err := cf.readColumnMetadata(); err != nil {
		pm.Close()
		return nil, err
	}
	
	// Load deleted bitmap if it exists
	cf.deletedBitmap = roaring.New()
	if cf.header.DeletedBitmapOffset > 0 {
		bitmapMgr := NewBitmapManager(cf.pageManager)
		deletedBitmap, err := bitmapMgr.LoadBitmap(cf.header.DeletedBitmapOffset)
		if err != nil {
			// Log error but don't fail - deleted bitmap is optional
			fmt.Printf("Warning: failed to load deleted bitmap: %v\n", err)
		} else {
			cf.deletedBitmap = deletedBitmap
		}
	}
	
	return cf, nil
}

// checkWriteAllowed returns an error if the file is read-only
func (cf *ColumnarFile) checkWriteAllowed() error {
	if cf.readOnly {
		return fmt.Errorf("cannot perform write operation: file is opened in read-only mode")
	}
	return nil
}

// AddColumn adds a new column to the file
func (cf *ColumnarFile) AddColumn(name string, dataType DataType, nullable bool) error {
	if err := cf.checkWriteAllowed(); err != nil {
		return err
	}
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
		rowKeyBTree:   NewBPlusTree(cf.pageManager, DataTypeUint64, nil), // Row numbers are always uint64
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

// LoadIntColumn loads data into an integer column (automatically handles nulls)
func (cf *ColumnarFile) LoadIntColumn(columnName string, data []IntData) error {
	if err := cf.checkWriteAllowed(); err != nil {
		return err
	}
	
	col, exists := cf.columns[columnName]
	if !exists {
		return fmt.Errorf("column %s not found", columnName)
	}
	
	// Allow all integer and boolean types
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		// All these types can be loaded as integers
	default:
		return fmt.Errorf("column %s is not an integer type", columnName)
	}
	
	// Validate nullable constraint before processing data
	for _, d := range data {
		if d.IsNull && !col.metadata.IsNullable {
			return fmt.Errorf("column %s is declared as non-nullable but received NULL value at row %d", columnName, d.RowNum)
		}
	}
	
	// Separate null and non-null data
	var keyRows []struct{ Key uint64; RowNum uint64 }
	var nullRows *roaring.Bitmap
	hasNulls := false
	
	// First pass: detect nulls and separate data
	for _, d := range data {
		if d.IsNull {
			// Found a null value
			if !hasNulls {
				hasNulls = true
				nullRows = roaring.New()
			}
			nullRows.Add(uint32(d.RowNum))
		} else {
			// Non-null value - convert to uint64 for B+ tree
			keyRows = append(keyRows, struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(d.Value),
				RowNum: d.RowNum,
			})
		}
	}
	
	// Handle null bitmap if nulls were found
	if hasNulls {
		col.nullBitmap = nullRows
		col.metadata.NullCount = uint64(nullRows.GetCardinality())
		
		// Store null bitmap to pages
		nullBitmapPageID, err := col.bitmapManager.StoreBitmap(nullRows)
		if err != nil {
			return fmt.Errorf("failed to store null bitmap: %w", err)
		}
		col.metadata.NullBitmapPageID = nullBitmapPageID
	} else {
		col.metadata.NullCount = 0
		col.metadata.NullBitmapPageID = 0
	}
	
	// Load non-null values into B+ tree (if any)
	if len(keyRows) > 0 {
		// Bulk load with duplicates and get statistics in single pass
		stats, err := col.btree.BulkLoadWithDuplicates(keyRows)
		if err != nil {
			return err
		}
		
		// Build row→key index
		rowKeyEntries := make([]BTreeLeafEntry, 0, len(keyRows))
		for _, kr := range keyRows {
			rowKeyEntries = append(rowKeyEntries, BTreeLeafEntry{
				Key:   kr.RowNum,
				Value: NewRowNumberValue(kr.Key), // Store the key as the value
			})
		}
		
		// Bulk load row→key index
		if err := col.rowKeyBTree.BulkLoad(rowKeyEntries); err != nil {
			return fmt.Errorf("failed to build row-key index: %w", err)
		}
		
		// Update metadata with calculated statistics
		col.metadata.RootPageID = col.btree.GetRootPageID()
		col.metadata.TreeHeight = col.btree.GetHeight()
		col.metadata.RowKeyRootPageID = col.rowKeyBTree.GetRootPageID()
		col.metadata.RowKeyTreeHeight = col.rowKeyBTree.GetHeight()
		col.metadata.TotalKeys = stats.TotalKeys
		col.metadata.DistinctCount = stats.DistinctCount
		col.metadata.MinValueOffset = stats.MinValue
		col.metadata.MaxValueOffset = stats.MaxValue
		col.metadata.AverageKeySize = stats.AverageKeySize
		col.metadata.BitmapPagesCount = 0 // Will be calculated when closing file
	} else {
		// All values are NULL
		col.metadata.RootPageID = 0
		col.metadata.TreeHeight = 0
		col.metadata.TotalKeys = 0
		col.metadata.DistinctCount = 0
	}
	
	return nil
}

// LoadStringColumn loads data into a string column (automatically handles nulls)
func (cf *ColumnarFile) LoadStringColumn(columnName string, data []StringData) error {
	if err := cf.checkWriteAllowed(); err != nil {
		return err
	}
	
	col, exists := cf.columns[columnName]
	if !exists {
		return fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeString {
		return fmt.Errorf("column %s is not string type", columnName)
	}
	
	// Validate nullable constraint before processing data
	for _, d := range data {
		if d.IsNull && !col.metadata.IsNullable {
			return fmt.Errorf("column %s is declared as non-nullable but received NULL value at row %d", columnName, d.RowNum)
		}
	}
	
	// Separate null and non-null data
	stringRows := make(map[string][]uint64)
	var nullRows *roaring.Bitmap
	hasNulls := false
	
	// First pass: detect nulls and separate data
	for _, d := range data {
		if d.IsNull {
			// Found a null value
			if !hasNulls {
				hasNulls = true
				nullRows = roaring.New()
			}
			nullRows.Add(uint32(d.RowNum))
		} else {
			// Non-null value
			stringRows[d.Value] = append(stringRows[d.Value], d.RowNum)
		}
	}
	
	// Handle null bitmap if nulls were found
	if hasNulls {
		col.nullBitmap = nullRows
		col.metadata.NullCount = uint64(nullRows.GetCardinality())
		
		// Store null bitmap to pages
		nullBitmapPageID, err := col.bitmapManager.StoreBitmap(nullRows)
		if err != nil {
			return fmt.Errorf("failed to store null bitmap: %w", err)
		}
		col.metadata.NullBitmapPageID = nullBitmapPageID
	} else {
		col.metadata.NullCount = 0
		col.metadata.NullBitmapPageID = 0
	}
	
	// Process non-null values (if any)
	if len(stringRows) > 0 {
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
		
		// Bulk load with duplicates and get statistics
		stats, err := col.btree.BulkLoadWithDuplicates(keyRows)
		if err != nil {
			return err
		}
		
		// Build row→key index for strings
		// For strings, we store the string offset as the value
		rowKeyEntries := make([]BTreeLeafEntry, 0, len(keyRows))
		for _, kr := range keyRows {
			rowKeyEntries = append(rowKeyEntries, BTreeLeafEntry{
				Key:   kr.RowNum,
				Value: NewRowNumberValue(kr.Key), // Store the string offset as the value
			})
		}
		
		// Bulk load row→key index
		if err := col.rowKeyBTree.BulkLoad(rowKeyEntries); err != nil {
			return fmt.Errorf("failed to build row-key index: %w", err)
		}
		
		// Update metadata
		col.metadata.RootPageID = col.btree.GetRootPageID()
		col.metadata.TreeHeight = col.btree.GetHeight()
		col.metadata.RowKeyRootPageID = col.rowKeyBTree.GetRootPageID()
		col.metadata.RowKeyTreeHeight = col.rowKeyBTree.GetHeight()
		col.metadata.TotalKeys = stats.TotalKeys
		col.metadata.DistinctCount = uint64(len(stringRows))
		col.metadata.StringSegmentStart = col.stringSegment.rootPageID
		col.metadata.MinValueOffset = stats.MinValue
		col.metadata.MaxValueOffset = stats.MaxValue
		col.metadata.AverageKeySize = stats.AverageKeySize
		col.metadata.BitmapPagesCount = 0
	} else {
		// All values are NULL
		col.metadata.RootPageID = 0
		col.metadata.TreeHeight = 0
		col.metadata.TotalKeys = 0
		col.metadata.DistinctCount = 0
	}
	
	return nil
}

// QueryInt performs an equality query on an integer column
func (cf *ColumnarFile) QueryInt(columnName string, value int64) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeInt64 {
		return nil, fmt.Errorf("column %s is not int64 type", columnName)
	}
	
	return col.btree.FindBitmap(uint64(value))
}

// QueryString performs an equality query on a string column
func (cf *ColumnarFile) QueryString(columnName string, value string) (*roaring.Bitmap, error) {
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
		return roaring.New(), nil // String not found, return empty bitmap
	}
	
	return col.btree.FindBitmap(offset)
}

// RangeQueryInt performs a range query on an integer column
func (cf *ColumnarFile) RangeQueryInt(columnName string, min, max int64) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Allow all integer types
	switch col.metadata.DataType {
	case DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		return col.btree.RangeSearchBitmap(uint64(min), uint64(max))
	default:
		return nil, fmt.Errorf("column %s is not an integer type", columnName)
	}
}

// RangeQueryString performs a range query on a string column
func (cf *ColumnarFile) RangeQueryString(columnName string, min, max string) (*roaring.Bitmap, error) {
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
		return roaring.New(), nil
	}
	
	// Query each offset and collect results using bitmaps
	result := roaring.New()
	for _, offset := range offsets {
		bitmap, err := col.btree.FindBitmap(offset)
		if err != nil {
			continue
		}
		result.Or(bitmap)
	}
	
	return result, nil
}

// QueryGreaterThan performs a > query on a column
func (cf *ColumnarFile) QueryGreaterThan(columnName string, value interface{}) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		// For numeric types, use range search from value+1 to max
		intVal := toUint64(value)
		return col.btree.RangeSearchGreaterThanBitmap(intVal)
		
	case DataTypeString:
		// For strings, find all strings > value
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		return cf.queryStringGreaterThan(col, strVal)
		
	default:
		return nil, fmt.Errorf("greater than not supported for data type %v", col.metadata.DataType)
	}
}

// QueryGreaterThanOrEqual performs a >= query on a column
func (cf *ColumnarFile) QueryGreaterThanOrEqual(columnName string, value interface{}) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		// For numeric types, use range search from value to max
		intVal := toUint64(value)
		return col.btree.RangeSearchGreaterThanOrEqualBitmap(intVal)
		
	case DataTypeString:
		// For strings, find all strings >= value
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		return cf.queryStringGreaterThanOrEqual(col, strVal)
		
	default:
		return nil, fmt.Errorf("greater than or equal not supported for data type %v", col.metadata.DataType)
	}
}

// QueryLessThan performs a < query on a column
func (cf *ColumnarFile) QueryLessThan(columnName string, value interface{}) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		// For numeric types, use range search from min to value-1
		intVal := toUint64(value)
		return col.btree.RangeSearchLessThanBitmap(intVal)
		
	case DataTypeString:
		// For strings, find all strings < value
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		return cf.queryStringLessThan(col, strVal)
		
	default:
		return nil, fmt.Errorf("less than not supported for data type %v", col.metadata.DataType)
	}
}

// QueryLessThanOrEqual performs a <= query on a column
func (cf *ColumnarFile) QueryLessThanOrEqual(columnName string, value interface{}) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		// For numeric types, use range search from min to value
		intVal := toUint64(value)
		return col.btree.RangeSearchLessThanOrEqualBitmap(intVal)
		
	case DataTypeString:
		// For strings, find all strings <= value
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		return cf.queryStringLessThanOrEqual(col, strVal)
		
	default:
		return nil, fmt.Errorf("less than or equal not supported for data type %v", col.metadata.DataType)
	}
}

// QueryAnd performs an AND operation on multiple bitmaps
func (cf *ColumnarFile) QueryAnd(bitmaps ...*roaring.Bitmap) *roaring.Bitmap {
	if len(bitmaps) == 0 {
		return roaring.New()
	}
	
	// Filter out nil bitmaps
	validBitmaps := make([]*roaring.Bitmap, 0, len(bitmaps))
	for _, bitmap := range bitmaps {
		if bitmap != nil {
			validBitmaps = append(validBitmaps, bitmap)
		}
	}
	
	if len(validBitmaps) == 0 {
		return roaring.New()
	}
	if len(validBitmaps) == 1 {
		return validBitmaps[0].Clone()
	}
	
	// Perform intersection
	result := validBitmaps[0].Clone()
	for i := 1; i < len(validBitmaps); i++ {
		result.And(validBitmaps[i])
	}
	
	return result
}

// QueryOr performs an OR operation on multiple bitmaps
func (cf *ColumnarFile) QueryOr(bitmaps ...*roaring.Bitmap) *roaring.Bitmap {
	if len(bitmaps) == 0 {
		return roaring.New()
	}
	
	result := roaring.New()
	for _, bitmap := range bitmaps {
		if bitmap != nil {
			result.Or(bitmap)
		}
	}
	
	return result
}


// BitmapToSlice converts a bitmap to a slice of row numbers
func BitmapToSlice(bitmap *roaring.Bitmap) []uint64 {
	rows := make([]uint64, 0, bitmap.GetCardinality())
	iter := bitmap.Iterator()
	for iter.HasNext() {
		rows = append(rows, uint64(iter.Next()))
	}
	return rows
}

// QueryNot performs a NOT operation (returns all rows except those in the bitmap)
func (cf *ColumnarFile) QueryNot(columnName string, bitmap *roaring.Bitmap) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// For NOT operation, we need to know all possible row numbers
	// Since we don't track max row number, we'll collect all rows from the column
	allRowsBitmap := roaring.New()
	
	// Traverse the B+ tree to get all row numbers
	if col.btree.rootPageID != 0 {
		// Start from leftmost leaf
		currentPageID := col.btree.rootPageID
		
		// Navigate to leftmost leaf
		for {
			page, err := cf.pageManager.ReadPage(currentPageID)
			if err != nil {
				return nil, err
			}
			
			if page.Header.PageType == PageTypeBTreeLeaf {
				break
			}
			
			// For internal nodes, always go to first child
			currentPageID = ByteOrder.Uint64(page.Data[0:8])
		}
		
		// Scan all leaves
		for currentPageID != 0 {
			page, err := cf.pageManager.ReadPage(currentPageID)
			if err != nil {
				return nil, err
			}
			
			entries, err := col.btree.readLeafEntries(page)
			if err != nil {
				return nil, err
			}
			
			for _, entry := range entries {
				if entry.Value.IsRowNumber() {
					allRowsBitmap.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					rows, err := col.btree.loadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						continue
					}
					for _, row := range rows {
						allRowsBitmap.Add(uint32(row))
					}
				}
			}
			
			currentPageID = page.Header.NextPageID
		}
	}
	
	// Perform NOT operation (difference)
	allRowsBitmap.AndNot(bitmap)
	
	return allRowsBitmap, nil
}

// QueryNull returns rows where the specified column is NULL
func (cf *ColumnarFile) QueryNull(columnName string) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if !col.metadata.IsNullable {
		// Non-nullable column can't have nulls
		return roaring.New(), nil
	}
	
	// Return a copy of the null bitmap
	if col.nullBitmap != nil {
		return col.nullBitmap.Clone(), nil
	}
	
	return roaring.New(), nil
}

// QueryNotNull returns rows where the specified column is NOT NULL
func (cf *ColumnarFile) QueryNotNull(columnName string) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	if !col.metadata.IsNullable {
		// Non-nullable column - all rows are not null
		// Return all rows in the column
		return cf.getAllRowsForColumn(col)
	}
	
	// Get all rows and exclude nulls
	allRows, err := cf.getAllRowsForColumn(col)
	if err != nil {
		return nil, err
	}
	
	if col.nullBitmap != nil {
		allRows.AndNot(col.nullBitmap)
	}
	
	return allRows, nil
}

// getAllRowsForColumn returns all row numbers stored in a column
func (cf *ColumnarFile) getAllRowsForColumn(col *Column) (*roaring.Bitmap, error) {
	allRows := roaring.New()
	
	// Add null rows first if they exist
	if col.nullBitmap != nil {
		allRows.Or(col.nullBitmap)
	}
	
	// Traverse the B+ tree to get all non-null row numbers
	if col.btree.rootPageID != 0 {
		// Start from leftmost leaf
		currentPageID := col.btree.rootPageID
		
		// Navigate to leftmost leaf
		for {
			page, err := col.btree.pageManager.ReadPage(currentPageID)
			if err != nil {
				return nil, err
			}
			
			if page.Header.PageType == PageTypeBTreeLeaf {
				break
			}
			
			// For internal nodes, get first child from childPageMap
			children, exists := col.btree.childPageMap[currentPageID]
			if !exists || len(children) == 0 {
				return nil, fmt.Errorf("no children found for internal page %d", currentPageID)
			}
			currentPageID = children[0]
		}
		
		// Scan all leaf pages
		for currentPageID != 0 {
			page, err := col.btree.pageManager.ReadPage(currentPageID)
			if err != nil {
				return nil, err
			}
			
			entries, err := col.btree.readLeafEntries(page)
			if err != nil {
				return nil, err
			}
			
			for _, entry := range entries {
				if entry.Value.IsRowNumber() {
					allRows.Add(uint32(entry.Value.GetRowNumber()))
				} else {
					// Load bitmap
					bitmap, err := col.btree.bitmapManager.LoadBitmap(entry.Value.GetBitmapOffset())
					if err != nil {
						continue
					}
					// Extract row numbers and add to result
					rows := col.btree.bitmapManager.ExtractRowNumbers(bitmap)
					for _, row := range rows {
						allRows.Add(uint32(row))
					}
				}
			}
			
			currentPageID = page.Header.NextPageID
		}
	}
	
	return allRows, nil
}

// RangeQueryIntWithNulls performs a range query on an integer column with null handling
func (cf *ColumnarFile) RangeQueryIntWithNulls(columnName string, min, max int64, nullOrder NullSortOrder, includeNulls bool) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Allow all integer types
	switch col.metadata.DataType {
	case DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		
		// Get non-null results from range query
		result, err := col.btree.RangeSearchBitmap(uint64(min), uint64(max))
		if err != nil {
			return nil, err
		}
		
		// Handle nulls based on sort order and includeNulls flag
		if includeNulls && col.metadata.IsNullable && col.nullBitmap != nil {
			// Add nulls based on sort order
			switch nullOrder {
			case NullsFirst:
				// Nulls come first - add them to the result
				result.Or(col.nullBitmap)
			case NullsLast:
				// Nulls come last - add them to the result  
				result.Or(col.nullBitmap)
			}
		}
		
		return result, nil
		
	default:
		return nil, fmt.Errorf("column %s is not an integer type", columnName)
	}
}

// RangeQueryStringWithNulls performs a range query on a string column with null handling
func (cf *ColumnarFile) RangeQueryStringWithNulls(columnName string, min, max string, nullOrder NullSortOrder, includeNulls bool) (*roaring.Bitmap, error) {
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
		result := roaring.New()
		
		// Handle nulls if requested
		if includeNulls && col.metadata.IsNullable && col.nullBitmap != nil {
			result.Or(col.nullBitmap)
		}
		
		return result, nil
	}
	
	// Query each offset and collect results
	result := roaring.New()
	for _, offset := range offsets {
		bitmap, err := col.btree.FindBitmap(offset)
		if err != nil {
			continue
		}
		result.Or(bitmap)
	}
	
	// Handle nulls based on sort order and includeNulls flag
	if includeNulls && col.metadata.IsNullable && col.nullBitmap != nil {
		switch nullOrder {
		case NullsFirst:
			// Nulls come first - add them to the result
			result.Or(col.nullBitmap)
		case NullsLast:
			// Nulls come last - add them to the result
			result.Or(col.nullBitmap)
		}
	}
	
	return result, nil
}

// QueryLessThanWithNulls performs a < query with null handling
func (cf *ColumnarFile) QueryLessThanWithNulls(columnName string, value interface{}, nullOrder NullSortOrder) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	var result *roaring.Bitmap
	var err error
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		intVal := toUint64(value)
		result, err = col.btree.RangeSearchLessThanBitmap(intVal)
		
	case DataTypeString:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		result, err = cf.queryStringLessThan(col, strVal)
		
	default:
		return nil, fmt.Errorf("less than not supported for data type %v", col.metadata.DataType)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Handle nulls based on sort order
	if col.metadata.IsNullable && col.nullBitmap != nil {
		switch nullOrder {
		case NullsFirst:
			// Nulls come first, so they're included in "less than" result
			result.Or(col.nullBitmap)
		case NullsLast:
			// Nulls come last, so they're excluded from "less than" result
			// (already excluded since they're not in the B+ tree)
		}
	}
	
	return result, nil
}

// QueryGreaterThanWithNulls performs a > query with null handling  
func (cf *ColumnarFile) QueryGreaterThanWithNulls(columnName string, value interface{}, nullOrder NullSortOrder) (*roaring.Bitmap, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	var result *roaring.Bitmap
	var err error
	
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		intVal := toUint64(value)
		result, err = col.btree.RangeSearchGreaterThanBitmap(intVal)
		
	case DataTypeString:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value for string column")
		}
		result, err = cf.queryStringGreaterThan(col, strVal)
		
	default:
		return nil, fmt.Errorf("greater than not supported for data type %v", col.metadata.DataType)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Handle nulls based on sort order
	if col.metadata.IsNullable && col.nullBitmap != nil {
		switch nullOrder {
		case NullsFirst:
			// Nulls come first, so they're excluded from "greater than" result
			// (already excluded since they're not in the B+ tree)
		case NullsLast:
			// Nulls come last, so they're included in "greater than" result
			result.Or(col.nullBitmap)
		}
	}
	
	return result, nil
}

// Close closes the columnar file
func (cf *ColumnarFile) Close() error {
	// Update file size in header
	cf.header.FileSize = uint64(cf.pageManager.GetPageCount()) * uint64(cf.pageManager.GetPageSize())
	cf.header.TotalPages = cf.pageManager.GetPageCount()
	cf.header.RowCount = cf.calculateTotalRows()
	
	// Save deleted bitmap if it has any entries
	if cf.deletedBitmap != nil && cf.deletedBitmap.GetCardinality() > 0 {
		// Create a bitmap manager to save the deleted bitmap
		bitmapMgr := NewBitmapManager(cf.pageManager)
		offset, err := bitmapMgr.StoreBitmap(cf.deletedBitmap)
		if err != nil {
			return fmt.Errorf("failed to save deleted bitmap: %w", err)
		}
		cf.header.DeletedBitmapOffset = offset
	} else {
		cf.header.DeletedBitmapOffset = 0
	}
	
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
	// Write compression metadata
	binary.Write(buf, ByteOrder, cf.header.DefaultCompression)
	binary.Write(buf, ByteOrder, cf.header.LeafCompression)
	binary.Write(buf, ByteOrder, cf.header.InternalCompression)
	binary.Write(buf, ByteOrder, cf.header.StringCompression)
	binary.Write(buf, ByteOrder, cf.header.BitmapCompression)
	binary.Write(buf, ByteOrder, cf.header.CompressionLevel)
	binary.Write(buf, ByteOrder, cf.header.CompressionFlags)
	binary.Write(buf, ByteOrder, cf.header.DeletedBitmapOffset)
	
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
	// Read compression metadata (may not exist in older files)
	if buf.Len() >= 7 { // Check if we have compression metadata
		binary.Read(buf, ByteOrder, &cf.header.DefaultCompression)
		binary.Read(buf, ByteOrder, &cf.header.LeafCompression)
		binary.Read(buf, ByteOrder, &cf.header.InternalCompression)
		binary.Read(buf, ByteOrder, &cf.header.StringCompression)
		binary.Read(buf, ByteOrder, &cf.header.BitmapCompression)
		binary.Read(buf, ByteOrder, &cf.header.CompressionLevel)
		binary.Read(buf, ByteOrder, &cf.header.CompressionFlags)
		
		// Read deleted bitmap offset (may not exist in older files)
		if buf.Len() >= 8 {
			binary.Read(buf, ByteOrder, &cf.header.DeletedBitmapOffset)
		}
	}
	
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
		binary.Write(buf, ByteOrder, col.metadata.NullBitmapPageID)
		binary.Write(buf, ByteOrder, col.metadata.RowKeyRootPageID)
		binary.Write(buf, ByteOrder, col.metadata.RowKeyTreeHeight)
		
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
		binary.Read(buf, ByteOrder, &metadata.NullBitmapPageID)
		binary.Read(buf, ByteOrder, &metadata.RowKeyRootPageID)
		binary.Read(buf, ByteOrder, &metadata.RowKeyTreeHeight)
		
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
		col.btree.SetHeight(metadata.TreeHeight)
		
		// Reconstruct child page mapping for optimized B+ tree navigation
		if err := col.btree.ReconstructChildPageMapping(); err != nil {
			return fmt.Errorf("failed to reconstruct child page mapping for column %s: %w", colName, err)
		}
		
		// Create row→key B+ tree
		col.rowKeyBTree = NewBPlusTree(cf.pageManager, DataTypeUint64, nil)
		col.rowKeyBTree.SetRootPageID(metadata.RowKeyRootPageID)
		col.rowKeyBTree.SetHeight(metadata.RowKeyTreeHeight)
		
		// Reconstruct child page mapping for row→key B+ tree
		if metadata.RowKeyRootPageID != 0 {
			if err := col.rowKeyBTree.ReconstructChildPageMapping(); err != nil {
				return fmt.Errorf("failed to reconstruct row-key child page mapping for column %s: %w", colName, err)
			}
		}
		
		// Load null bitmap if it exists
		if metadata.NullBitmapPageID != 0 {
			nullBitmap, err := col.bitmapManager.LoadBitmap(metadata.NullBitmapPageID)
			if err != nil {
				return fmt.Errorf("failed to load null bitmap for column %s: %w", colName, err)
			}
			col.nullBitmap = nullBitmap
		}
		
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
	stats["min_value_offset"] = col.metadata.MinValueOffset
	stats["max_value_offset"] = col.metadata.MaxValueOffset
	stats["total_size_bytes"] = col.metadata.TotalSizeBytes
	stats["bitmap_pages_count"] = col.metadata.BitmapPagesCount
	stats["average_key_size"] = col.metadata.AverageKeySize
	
	return stats, nil
}

// LookupIntByRow retrieves the integer value for a given row number
func (cf *ColumnarFile) LookupIntByRow(columnName string, rowNum uint64) (int64, bool, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return 0, false, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeInt64 {
		return 0, false, fmt.Errorf("column %s is not int64 type", columnName)
	}
	
	// Check if row is null
	if col.nullBitmap != nil && col.nullBitmap.Contains(uint32(rowNum)) {
		return 0, false, nil // Row is NULL
	}
	
	// Lookup in row→key index
	rows, err := col.rowKeyBTree.Find(rowNum)
	if err != nil {
		return 0, false, err
	}
	
	if len(rows) == 0 {
		return 0, false, nil // Row not found
	}
	
	// The value stored in the B+ tree is the actual int value
	return int64(rows[0]), true, nil
}

// LookupStringByRow retrieves the string value for a given row number
func (cf *ColumnarFile) LookupStringByRow(columnName string, rowNum uint64) (string, bool, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return "", false, fmt.Errorf("column %s not found", columnName)
	}
	
	if col.metadata.DataType != DataTypeString {
		return "", false, fmt.Errorf("column %s is not string type", columnName)
	}
	
	// Check if row is null
	if col.nullBitmap != nil && col.nullBitmap.Contains(uint32(rowNum)) {
		return "", false, nil // Row is NULL
	}
	
	// Lookup in row→key index
	rows, err := col.rowKeyBTree.Find(rowNum)
	if err != nil {
		return "", false, err
	}
	
	if len(rows) == 0 {
		return "", false, nil // Row not found
	}
	
	// The value stored is the string offset
	offset := rows[0]
	
	// Get string from string segment
	str, err := col.stringSegment.GetString(offset)
	if err != nil {
		return "", false, err
	}
	
	return str, true, nil
}

// LookupValueByRow retrieves the value for a given row number (generic version)
func (cf *ColumnarFile) LookupValueByRow(columnName string, rowNum uint64) (interface{}, bool, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, false, fmt.Errorf("column %s not found", columnName)
	}
	
	// Check if row is null
	if col.nullBitmap != nil && col.nullBitmap.Contains(uint32(rowNum)) {
		return nil, false, nil // Row is NULL
	}
	
	// Lookup in row→key index
	rows, err := col.rowKeyBTree.Find(rowNum)
	if err != nil {
		return nil, false, err
	}
	
	if len(rows) == 0 {
		return nil, false, nil // Row not found
	}
	
	// Convert based on data type
	switch col.metadata.DataType {
	case DataTypeInt8:
		return int8(rows[0]), true, nil
	case DataTypeInt16:
		return int16(rows[0]), true, nil
	case DataTypeInt32:
		return int32(rows[0]), true, nil
	case DataTypeInt64:
		return int64(rows[0]), true, nil
	case DataTypeUint8:
		return uint8(rows[0]), true, nil
	case DataTypeUint16:
		return uint16(rows[0]), true, nil
	case DataTypeUint32:
		return uint32(rows[0]), true, nil
	case DataTypeUint64:
		return rows[0], true, nil
	case DataTypeBool:
		return rows[0] != 0, true, nil
	case DataTypeString:
		// For strings, the value is an offset into the string segment
		str, err := col.stringSegment.GetString(rows[0])
		if err != nil {
			return nil, false, err
		}
		return str, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported data type %v", col.metadata.DataType)
	}
}

// GetRowsForColumn returns all row numbers that have values in the specified column
func (cf *ColumnarFile) GetRowsForColumn(columnName string) ([]uint64, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	allRows, err := cf.getAllRowsForColumn(col)
	if err != nil {
		return nil, err
	}
	
	return BitmapToSlice(allRows), nil
}

// Statistics calculation helper functions (deprecated - use BulkLoadWithDuplicates for efficiency)

// UpdateStatisticsForUintColumn updates statistics for a column after bulk loading uint64 data
// This function is now deprecated - use BulkLoadWithDuplicates which returns statistics directly
func (cf *ColumnarFile) UpdateStatisticsForUintColumn(columnName string, data []struct{ Key uint64; RowNum uint64 }) error {
	col, exists := cf.columns[columnName]
	if !exists {
		return fmt.Errorf("column %s not found", columnName)
	}
	
	// For backward compatibility, manually calculate statistics
	// Note: This is less efficient than using BulkLoadWithDuplicates directly
	distinctKeys := make(map[uint64]bool)
	var minVal, maxVal uint64 = ^uint64(0), 0
	
	for _, d := range data {
		distinctKeys[d.Key] = true
		if d.Key < minVal {
			minVal = d.Key
		}
		if d.Key > maxVal {
			maxVal = d.Key
		}
	}
	
	col.metadata.TotalKeys = uint64(len(data))
	col.metadata.DistinctCount = uint64(len(distinctKeys))
	col.metadata.NullCount = 0 // No null support yet
	col.metadata.MinValueOffset = minVal
	col.metadata.MaxValueOffset = maxVal
	col.metadata.AverageKeySize = uint8(GetDataTypeSize(col.metadata.DataType))
	col.metadata.BitmapPagesCount = 0 // Will be calculated when closing file
	
	return nil
}

// Helper functions for type conversion
func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	case int:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// String query helper functions
func (cf *ColumnarFile) queryStringGreaterThan(col *Column, value string) (*roaring.Bitmap, error) {
	// Find the next greater string offset
	offset, err := col.stringSegment.FindNextGreaterOffset(value)
	if err != nil {
		// No strings greater than value
		return roaring.New(), nil
	}
	
	// Get the string at this offset to use as minimum for range query
	minStr, err := col.stringSegment.GetString(offset)
	if err != nil {
		return nil, err
	}
	
	// Find maximum string in the segment
	maxStr := ""
	for _, str := range col.stringSegment.offsetMap {
		if str > maxStr {
			maxStr = str
		}
	}
	
	// Get all offsets in range
	offsets := col.stringSegment.GetOffsetsInRange(minStr, maxStr)
	
	// Query each offset using bitmaps
	result := roaring.New()
	for _, off := range offsets {
		bitmap, err := col.btree.FindBitmap(off)
		if err != nil {
			continue
		}
		result.Or(bitmap)
	}
	
	return result, nil
}

func (cf *ColumnarFile) queryStringGreaterThanOrEqual(col *Column, value string) (*roaring.Bitmap, error) {
	// Check if value exists
	if offset, found := col.stringSegment.FindOffset(value); found {
		// Include the value itself
		bitmap, err := col.btree.FindBitmap(offset)
		if err != nil {
			return nil, err
		}
		
		// Also get all greater values
		greaterBitmap, err := cf.queryStringGreaterThan(col, value)
		if err != nil {
			return bitmap, nil // Return at least the equal values
		}
		
		// Combine results
		return cf.QueryOr(bitmap, greaterBitmap), nil
	}
	
	// Value doesn't exist, just return greater than
	return cf.queryStringGreaterThan(col, value)
}

func (cf *ColumnarFile) queryStringLessThan(col *Column, value string) (*roaring.Bitmap, error) {
	// Find the next smaller string offset
	offset, err := col.stringSegment.FindNextSmallerOffset(value)
	if err != nil {
		// No strings less than value
		return roaring.New(), nil
	}
	
	// Get the string at this offset to use as maximum for range query
	maxStr, err := col.stringSegment.GetString(offset)
	if err != nil {
		return nil, err
	}
	
	// Find minimum string in the segment
	minStr := ""
	firstTime := true
	for _, str := range col.stringSegment.offsetMap {
		if firstTime || str < minStr {
			minStr = str
			firstTime = false
		}
	}
	
	// Get all offsets in range
	offsets := col.stringSegment.GetOffsetsInRange(minStr, maxStr)
	
	// Query each offset using bitmaps
	result := roaring.New()
	for _, off := range offsets {
		bitmap, err := col.btree.FindBitmap(off)
		if err != nil {
			continue
		}
		result.Or(bitmap)
	}
	
	return result, nil
}

func (cf *ColumnarFile) queryStringLessThanOrEqual(col *Column, value string) (*roaring.Bitmap, error) {
	// Check if value exists
	if offset, found := col.stringSegment.FindOffset(value); found {
		// Include the value itself
		bitmap, err := col.btree.FindBitmap(offset)
		if err != nil {
			return nil, err
		}
		
		// Also get all lesser values
		lesserBitmap, err := cf.queryStringLessThan(col, value)
		if err != nil {
			return bitmap, nil // Return at least the equal values
		}
		
		// Combine results
		return cf.QueryOr(bitmap, lesserBitmap), nil
	}
	
	// Value doesn't exist, just return less than
	return cf.queryStringLessThan(col, value)
}

// Iterator methods

// NewIterator creates a new iterator for the entire column in sorted order
func (cf *ColumnarFile) NewIterator(columnName string) (ColumnIterator, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Support all data types with B-tree
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		return NewBTreeIterator(col.btree, col.metadata.DataType, nil), nil
	case DataTypeString:
		return NewBTreeIterator(col.btree, col.metadata.DataType, col.stringSegment), nil
	default:
		return nil, fmt.Errorf("iterators not yet supported for data type %v", col.metadata.DataType)
	}
}

// NewRangeIterator creates a new iterator for a range of values
func (cf *ColumnarFile) NewRangeIterator(columnName string, min, max interface{}) (RangeIterator, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Support all data types with B-tree
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		
		// Convert min/max to uint64
		minVal := toUint64(min)
		maxVal := toUint64(max)
		
		return NewBTreeRangeIterator(col.btree, col.metadata.DataType, nil, minVal, maxVal), nil
	case DataTypeString:
		// For strings, convert min/max to string offsets
		minStr, ok := min.(string)
		if !ok {
			return nil, fmt.Errorf("min value must be string for string column")
		}
		maxStr, ok := max.(string)
		if !ok {
			return nil, fmt.Errorf("max value must be string for string column")
		}
		
		// Find offsets for min and max strings
		minOffset, foundMin := col.stringSegment.FindOffset(minStr)
		maxOffset, foundMax := col.stringSegment.FindOffset(maxStr)
		
		// If exact min string not found, find the next closest one
		if !foundMin {
			minOffset = col.stringSegment.FindClosestOffset(minStr)
		}
		
		// For max string, if not found, we want to include all strings up to it
		// So find the closest one and if it's less than maxStr, include it
		if !foundMax {
			// Find the largest string that is <= maxStr
			largestOffset := uint64(0)
			foundLargest := false
			for str, offset := range col.stringSegment.stringMap {
				if str <= maxStr {
					if !foundLargest || offset > largestOffset {
						largestOffset = offset
						foundLargest = true
					}
				}
			}
			if foundLargest {
				maxOffset = largestOffset
			} else {
				// No string <= maxStr, use max uint64 to indicate no upper bound
				maxOffset = ^uint64(0)
			}
		}
		
		return NewBTreeRangeIterator(col.btree, col.metadata.DataType, col.stringSegment, minOffset, maxOffset), nil
	default:
		return nil, fmt.Errorf("range iterators not yet supported for data type %v", col.metadata.DataType)
	}
}

// NewRangeIteratorInt creates a new iterator for a range of int64 values (convenience method)
func (cf *ColumnarFile) NewRangeIteratorInt(columnName string, min, max int64) (RangeIterator, error) {
	return cf.NewRangeIterator(columnName, min, max)
}

// BitmapToIterator converts a bitmap result to an iterator
func (cf *ColumnarFile) BitmapToIterator(columnName string, bitmap *roaring.Bitmap) (ColumnIterator, error) {
	col, exists := cf.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	
	// Support all data types with B-tree
	switch col.metadata.DataType {
	case DataTypeBool, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
		 DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		return NewBitmapIterator(col.btree, col.metadata.DataType, nil, bitmap), nil
	case DataTypeString:
		return NewBitmapIterator(col.btree, col.metadata.DataType, col.stringSegment, bitmap), nil
	default:
		return nil, fmt.Errorf("bitmap iterators not yet supported for data type %v", col.metadata.DataType)
	}
}

// BitmapIterator implements ColumnIterator for bitmap results
type BitmapIterator struct {
	btree         *BPlusTree
	dataType      DataType
	stringSegment *StringSegment
	bitmap        *roaring.Bitmap
	iter          roaring.IntIterable
	current       uint32
	valid         bool
	err           error
}

// NewBitmapIterator creates a new bitmap iterator
func NewBitmapIterator(btree *BPlusTree, dataType DataType, stringSegment *StringSegment, bitmap *roaring.Bitmap) *BitmapIterator {
	iter := &BitmapIterator{
		btree:         btree,
		dataType:      dataType,
		stringSegment: stringSegment,
		bitmap:        bitmap,
		iter:          bitmap.Iterator(),
	}
	
	// Don't call Next() here - let the first Next() call return the first element
	return iter
}

// Next moves to the next item
func (iter *BitmapIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	
	if iter.iter.HasNext() {
		iter.current = iter.iter.Next()
		iter.valid = true
		return true
	}
	
	iter.valid = false
	return false
}

// Prev moves to the previous item (not efficiently supported for bitmap iterator)
func (iter *BitmapIterator) Prev() bool {
	// Not efficiently supported for bitmap iterator
	iter.err = fmt.Errorf("reverse iteration not supported for bitmap iterator")
	return false
}

// Seek positions the iterator at the first row >= value
func (iter *BitmapIterator) Seek(value interface{}) bool {
	// Not efficiently supported for bitmap iterator
	iter.err = fmt.Errorf("seek not supported for bitmap iterator")
	return false
}

// SeekFirst positions at the first item
func (iter *BitmapIterator) SeekFirst() bool {
	iter.iter = iter.bitmap.Iterator()
	return iter.Next()
}

// SeekLast positions at the last item
func (iter *BitmapIterator) SeekLast() bool {
	// Not efficiently supported for bitmap iterator
	iter.err = fmt.Errorf("seek last not supported for bitmap iterator")
	return false
}

// Key returns the current key value (requires lookup)
func (iter *BitmapIterator) Key() interface{} {
	if !iter.valid {
		return nil
	}
	
	// Look up the key value for this row
	key, found, err := iter.btree.LookupByRow(uint64(iter.current))
	if err != nil {
		iter.err = err
		return nil
	}
	
	if !found {
		iter.err = fmt.Errorf("key not found for row %d", iter.current)
		return nil
	}
	
	// Handle different data types
	switch iter.dataType {
	case DataTypeBool:
		return key != 0
	case DataTypeInt8:
		return int8(key)
	case DataTypeInt16:
		return int16(key)
	case DataTypeInt32:
		return int32(key)
	case DataTypeInt64:
		return int64(key)
	case DataTypeUint8:
		return uint8(key)
	case DataTypeUint16:
		return uint16(key)
	case DataTypeUint32:
		return uint32(key)
	case DataTypeUint64:
		return key
	case DataTypeString:
		// For strings, key is the offset in the string segment
		if iter.stringSegment != nil {
			str, err := iter.stringSegment.GetString(key)
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

// Rows returns a bitmap with the current row
func (iter *BitmapIterator) Rows() *roaring.Bitmap {
	if !iter.valid {
		return nil
	}
	bitmap := roaring.New()
	bitmap.Add(iter.current)
	return bitmap
}

// Valid returns true if the iterator is positioned at a valid entry
func (iter *BitmapIterator) Valid() bool {
	return iter.valid && iter.err == nil
}

// Error returns any error that occurred
func (iter *BitmapIterator) Error() error {
	return iter.err
}

// Close releases resources
func (iter *BitmapIterator) Close() error {
	iter.bitmap = nil
	iter.iter = nil
	iter.valid = false
	return nil
}

// DeleteRow marks a single row as deleted
func (cf *ColumnarFile) DeleteRow(rowNum uint64) error {
	if cf.readOnly {
		return fmt.Errorf("cannot delete rows from read-only file")
	}
	
	cf.deletedBitmap.Add(uint32(rowNum))
	return nil
}

// DeleteRows marks multiple rows as deleted
func (cf *ColumnarFile) DeleteRows(rows *roaring.Bitmap) error {
	if cf.readOnly {
		return fmt.Errorf("cannot delete rows from read-only file")
	}
	
	if rows == nil {
		return nil
	}
	
	cf.deletedBitmap.Or(rows)
	return nil
}

// GetDeletedRows returns the bitmap of deleted rows
func (cf *ColumnarFile) GetDeletedRows() *roaring.Bitmap {
	return cf.deletedBitmap.Clone()
}

// IsRowDeleted checks if a specific row is marked as deleted
func (cf *ColumnarFile) IsRowDeleted(rowNum uint64) bool {
	return cf.deletedBitmap.Contains(uint32(rowNum))
}

// GetDeletedCount returns the number of deleted rows
func (cf *ColumnarFile) GetDeletedCount() uint64 {
	return cf.deletedBitmap.GetCardinality()
}

// UndeleteRow removes a row from the deleted bitmap (restores it)
func (cf *ColumnarFile) UndeleteRow(rowNum uint64) error {
	if cf.readOnly {
		return fmt.Errorf("cannot undelete rows in read-only file")
	}
	
	cf.deletedBitmap.Remove(uint32(rowNum))
	return nil
}

// UndeleteRows removes multiple rows from the deleted bitmap (restores them)
func (cf *ColumnarFile) UndeleteRows(rows *roaring.Bitmap) error {
	if cf.readOnly {
		return fmt.Errorf("cannot undelete rows in read-only file")
	}
	
	if rows == nil {
		return nil
	}
	
	cf.deletedBitmap.AndNot(rows)
	return nil
}

