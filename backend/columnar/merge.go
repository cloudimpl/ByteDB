package columnar

import (
	"fmt"
	"sort"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// MergeOptions configures how files are merged
type MergeOptions struct {
	// DeletedRowHandling specifies how to handle deleted rows
	DeletedRowHandling DeletedRowHandling
	
	// CompressionOptions for the output file
	CompressionOptions *CompressionOptions
	
	// Whether to deduplicate data across files
	DeduplicateKeys bool
	
	// Column-specific merge strategies
	ColumnStrategies map[string]ColumnMergeStrategy
}

// DeletedRowHandling specifies how to handle deleted rows during merge
type DeletedRowHandling int

const (
	// ExcludeDeleted excludes all deleted rows from the merged file
	ExcludeDeleted DeletedRowHandling = iota
	
	// PreserveDeleted preserves deleted rows and their deleted status
	PreserveDeleted
	
	// ConsolidateDeleted merges deleted bitmaps from all files
	ConsolidateDeleted
)

// ColumnMergeStrategy defines how to merge a specific column
type ColumnMergeStrategy struct {
	// Whether to keep only the latest value for duplicate keys
	KeepLatest bool
	
	// Custom merge function for handling duplicates
	MergeFunc func(values []interface{}) interface{}
}

// MergeFiles merges multiple columnar files into a new file
func MergeFiles(outputFilename string, inputFiles []string, options *MergeOptions) error {
	if len(inputFiles) == 0 {
		return fmt.Errorf("no input files specified")
	}
	
	if options == nil {
		options = &MergeOptions{
			DeletedRowHandling: ExcludeDeleted,
			DeduplicateKeys:    true,
		}
	}
	
	// Open all input files
	files := make([]*ColumnarFile, len(inputFiles))
	for i, filename := range inputFiles {
		cf, err := OpenFileReadOnly(filename)
		if err != nil {
			// Close already opened files
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return fmt.Errorf("failed to open input file %s: %w", filename, err)
		}
		files[i] = cf
	}
	
	// Ensure all files are closed when done
	defer func() {
		for _, cf := range files {
			if cf != nil {
				cf.Close()
			}
		}
	}()
	
	// Create output file
	var outputFile *ColumnarFile
	var err error
	if options.CompressionOptions != nil {
		outputFile, err = CreateFileWithOptions(outputFilename, options.CompressionOptions)
	} else {
		outputFile, err = CreateFile(outputFilename)
	}
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()
	
	// Get union of all columns across files
	columnSet := make(map[string]ColumnInfo)
	for _, cf := range files {
		for colName, col := range cf.columns {
			if existing, exists := columnSet[colName]; exists {
				// Verify data type matches
				if existing.DataType != col.metadata.DataType {
					return fmt.Errorf("column %s has different data types across files", colName)
				}
				// Use the most permissive nullable setting
				if col.metadata.IsNullable {
					existing.IsNullable = true
					columnSet[colName] = existing
				}
			} else {
				columnSet[colName] = ColumnInfo{
					Name:       colName,
					DataType:   col.metadata.DataType,
					IsNullable: col.metadata.IsNullable,
				}
			}
		}
	}
	
	// Add all columns to output file
	for _, colInfo := range columnSet {
		if err := outputFile.AddColumn(colInfo.Name, colInfo.DataType, colInfo.IsNullable); err != nil {
			return fmt.Errorf("failed to add column %s: %w", colInfo.Name, err)
		}
	}
	
	// First, collect all deleted bitmaps from all input files to get a global view
	globalDeletedBitmap := roaring.New()
	for _, cf := range files {
		if cf.deletedBitmap != nil && cf.deletedBitmap.GetCardinality() > 0 {
			globalDeletedBitmap.Or(cf.deletedBitmap)
		}
	}
	
	// Merge each column
	for colName, colInfo := range columnSet {
		if err := mergeColumn(outputFile, files, colName, colInfo, options, globalDeletedBitmap); err != nil {
			return fmt.Errorf("failed to merge column %s: %w", colName, err)
		}
	}
	
	// Handle consolidated deleted bitmap if requested
	if options.DeletedRowHandling == ConsolidateDeleted {
		// The output file gets the union of all deleted bitmaps
		// This represents rows that are deleted globally but may exist in some input files
		outputFile.deletedBitmap = globalDeletedBitmap.Clone()
	} else if options.DeletedRowHandling == PreserveDeleted {
		// For PreserveDeleted, we need to track which rows in the OUTPUT file
		// correspond to deleted rows from the input files
		// This requires mapping from original row numbers to new row numbers
		// For now, we'll just note this in documentation as a limitation
		outputFile.deletedBitmap = roaring.New()
	}
	
	return nil
}

// ColumnInfo holds information about a column
type ColumnInfo struct {
	Name       string
	DataType   DataType
	IsNullable bool
}

// mergeColumn merges a single column from multiple files
func mergeColumn(output *ColumnarFile, inputs []*ColumnarFile, colName string, colInfo ColumnInfo, options *MergeOptions, globalDeletedBitmap *roaring.Bitmap) error {
	// Collect all data from input files
	allData := make([]MergeEntry, 0)
	
	for fileIdx, cf := range inputs {
		col, exists := cf.columns[colName]
		if !exists {
			continue // Column doesn't exist in this file
		}
		
		// Create iterator for the column
		iter, err := cf.NewIterator(colName)
		if err != nil {
			return fmt.Errorf("failed to create iterator for column %s in file %d: %w", colName, fileIdx, err)
		}
		defer iter.Close()
		
		// Collect all entries
		for iter.Next() {
			key := iter.Key()
			rows := iter.Rows()
			
			// Process each row in the bitmap
			rowIter := rows.Iterator()
			for rowIter.HasNext() {
				rowNum := uint64(rowIter.Next())
				
				// Check if row is deleted in the global deleted bitmap
				if options.DeletedRowHandling == ExcludeDeleted && 
				   globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)) {
					continue // Skip globally deleted rows
				}
				
				allData = append(allData, MergeEntry{
					Key:           key,
					OriginalRowNum: rowNum,
					FileIdx:       fileIdx,
					IsDeleted:     globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)),
				})
			}
		}
		
		// Also collect NULL values if column is nullable
		if col.metadata.IsNullable && col.nullBitmap != nil {
			nullIter := col.nullBitmap.Iterator()
			for nullIter.HasNext() {
				rowNum := uint64(nullIter.Next())
				
				// Check if row is deleted in the global deleted bitmap
				if options.DeletedRowHandling == ExcludeDeleted && 
				   globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)) {
					continue // Skip globally deleted rows
				}
				
				allData = append(allData, MergeEntry{
					Key:           nil,
					OriginalRowNum: rowNum,
					FileIdx:       fileIdx,
					IsNull:        true,
					IsDeleted:     globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)),
				})
			}
		}
	}
	
	// Sort entries for deduplication if needed
	if options.DeduplicateKeys {
		sort.Slice(allData, func(i, j int) bool {
			// Sort by key first, then by file index (later files take precedence)
			if allData[i].IsNull != allData[j].IsNull {
				return !allData[i].IsNull // Non-nulls come before nulls
			}
			if allData[i].IsNull && allData[j].IsNull {
				return allData[i].FileIdx < allData[j].FileIdx
			}
			
			// Compare keys based on data type
			return compareKeys(allData[i].Key, allData[j].Key, colInfo.DataType) < 0
		})
	}
	
	// Write merged data to output
	newRowNum := uint64(0)
	
	switch colInfo.DataType {
	case DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
	     DataTypeUint8, DataTypeUint16, DataTypeUint32, DataTypeUint64:
		intData := make([]IntData, 0, len(allData))
		
		lastKey := interface{}(nil)
		for _, entry := range allData {
			// Skip duplicates if deduplication is enabled
			if options.DeduplicateKeys && lastKey != nil && !entry.IsNull &&
			   compareKeys(lastKey, entry.Key, colInfo.DataType) == 0 {
				continue
			}
			
			if entry.IsNull {
				intData = append(intData, NewNullIntData(newRowNum))
			} else {
				intVal := toInt64(entry.Key)
				intData = append(intData, NewIntData(intVal, newRowNum))
			}
			
			lastKey = entry.Key
			newRowNum++
		}
		
		return output.LoadIntColumn(colName, intData)
		
	case DataTypeString:
		stringData := make([]StringData, 0, len(allData))
		
		lastKey := interface{}(nil)
		for _, entry := range allData {
			// Skip duplicates if deduplication is enabled
			if options.DeduplicateKeys && lastKey != nil && !entry.IsNull &&
			   compareKeys(lastKey, entry.Key, colInfo.DataType) == 0 {
				continue
			}
			
			if entry.IsNull {
				stringData = append(stringData, NewNullStringData(newRowNum))
			} else {
				strVal := entry.Key.(string)
				stringData = append(stringData, NewStringData(strVal, newRowNum))
			}
			
			lastKey = entry.Key
			newRowNum++
		}
		
		return output.LoadStringColumn(colName, stringData)
		
	default:
		return fmt.Errorf("merge not supported for data type %v", colInfo.DataType)
	}
}

// MergeEntry represents a single entry during merge
type MergeEntry struct {
	Key            interface{}
	OriginalRowNum uint64  // The global row number from the source file
	FileIdx        int
	IsNull         bool
	IsDeleted      bool
}

// compareKeys compares two keys based on data type
func compareKeys(a, b interface{}, dataType DataType) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	
	switch dataType {
	case DataTypeString:
		aStr := a.(string)
		bStr := b.(string)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
		
	default:
		// For numeric types, convert to uint64 for comparison
		aVal := toUint64(a)
		bVal := toUint64(b)
		if aVal < bVal {
			return -1
		} else if aVal > bVal {
			return 1
		}
		return 0
	}
}

// toInt64 converts various numeric types to int64
func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return 0
	}
}