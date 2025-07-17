package columnar

import (
	"fmt"
	"sort"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// mergeColumnStreaming implements true streaming merge preserving row numbers
func mergeColumnStreaming(output *ColumnarFile, inputs []*ColumnarFile, colName string, colInfo ColumnInfo, options *MergeOptions, globalDeletedBitmap *roaring.Bitmap) error {
	outputCol, exists := output.columns[colName]
	if !exists {
		return fmt.Errorf("column %s not found in output file", colName)
	}
	
	// Phase 1: Build main B-tree using streaming approach
	if err := buildMainBTreeStreaming(output, inputs, colName, colInfo, options, globalDeletedBitmap, outputCol); err != nil {
		return fmt.Errorf("failed to build main B-tree: %w", err)
	}
	
	// Phase 2: Build row-key index using row merge iterator
	if err := buildRowKeyIndexStreaming(output, inputs, colName, colInfo, globalDeletedBitmap, outputCol); err != nil {
		return fmt.Errorf("failed to build row-key index: %w", err)
	}
	
	return nil
}

// buildMainBTreeStreaming builds the main B-tree without buffering
func buildMainBTreeStreaming(output *ColumnarFile, inputs []*ColumnarFile, colName string, colInfo ColumnInfo, options *MergeOptions, globalDeletedBitmap *roaring.Bitmap, outputCol *Column) error {
	// Create iterators for all input files
	iterators := make([]ColumnIterator, 0, len(inputs))
	
	for _, cf := range inputs {
		_, exists := cf.columns[colName]
		if !exists {
			continue
		}
		
		iter, err := cf.NewIterator(colName)
		if err != nil {
			for _, it := range iterators {
				it.Close()
			}
			return fmt.Errorf("failed to create iterator: %w", err)
		}
		
		iterators = append(iterators, iter)
	}
	
	if len(iterators) == 0 {
		return nil
	}
	
	defer func() {
		for _, iter := range iterators {
			iter.Close()
		}
	}()
	
	// Create merge iterator
	mergeIter := NewMergeIterator(iterators, colInfo.DataType)
	defer mergeIter.Close()
	
	// Initialize streaming B-tree builder
	builder := NewBTreeStreamingBuilder(outputCol.btree)
	currentStringOffset := uint64(0)
	
	// Statistics
	totalKeys := uint64(0)
	distinctKeys := uint64(0)
	minValue := uint64(^uint64(0))
	maxValue := uint64(0)
	
	// Process merged entries in streaming fashion
	for mergeIter.Next() {
		key := mergeIter.Key()
		rows := mergeIter.Rows()
		
		// Convert key to uint64
		var keyUint64 uint64
		switch colInfo.DataType {
		case DataTypeString:
			strKey := key.(string)
			keyUint64 = currentStringOffset
			outputCol.stringSegment.AddString(strKey)
			currentStringOffset += uint64(4 + len(strKey))
		default:
			keyUint64 = toUint64WithType(key, colInfo.DataType)
			// Debug: print conversion
			// fmt.Printf("Converting key %v (type %T) to uint64: %d\n", key, key, keyUint64)
		}
		
		// Filter deleted rows from the bitmap
		validRows := roaring.New()
		if globalDeletedBitmap != nil {
			validRows.Or(rows)
			validRows.AndNot(globalDeletedBitmap)
		} else {
			validRows.Or(rows)
		}
		
		if validRows.GetCardinality() == 0 {
			continue
		}
		
		totalKeys += uint64(validRows.GetCardinality())
		distinctKeys++
		
		// Create value
		var value Value
		if validRows.GetCardinality() == 1 {
			iter := validRows.Iterator()
			iter.HasNext()
			rowNum := uint64(iter.Next())
			value = NewRowNumberValue(rowNum)
		} else {
			bitmapPageID, err := outputCol.bitmapManager.StoreBitmap(validRows)
			if err != nil {
				return fmt.Errorf("failed to store bitmap: %w", err)
			}
			value = NewBitmapValue(bitmapPageID)
		}
		
		// Add to B-tree using streaming approach
		if err := builder.Add(keyUint64, value); err != nil {
			return fmt.Errorf("failed to add entry to B-tree: %w", err)
		}
		
		if keyUint64 < minValue {
			minValue = keyUint64
		}
		if keyUint64 > maxValue {
			maxValue = keyUint64
		}
	}
	
	// Finalize the B-tree
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("failed to finalize B-tree: %w", err)
	}
	
	// Build string segment if needed
	if colInfo.DataType == DataTypeString {
		if err := outputCol.stringSegment.Build(); err != nil {
			return fmt.Errorf("failed to build string segment: %w", err)
		}
		outputCol.metadata.StringSegmentStart = outputCol.stringSegment.rootPageID
	}
	
	// Update metadata
	outputCol.metadata.RootPageID = outputCol.btree.GetRootPageID()
	outputCol.metadata.TreeHeight = outputCol.btree.GetHeight()
	outputCol.metadata.TotalKeys = totalKeys
	outputCol.metadata.DistinctCount = distinctKeys
	outputCol.metadata.MinValueOffset = minValue
	outputCol.metadata.MaxValueOffset = maxValue
	outputCol.metadata.AverageKeySize = 8
	
	// Handle NULL values
	nullBitmap := roaring.New()
	for _, cf := range inputs {
		col, exists := cf.columns[colName]
		if exists && col.metadata.IsNullable && col.nullBitmap != nil {
			nullIter := col.nullBitmap.Iterator()
			for nullIter.HasNext() {
				rowNum := uint64(nullIter.Next())
				
				if globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)) {
					continue
				}
				
				nullBitmap.Add(uint32(rowNum))
			}
		}
	}
	
	if nullBitmap.GetCardinality() > 0 {
		outputCol.nullBitmap = nullBitmap
		nullBitmapPageID, err := outputCol.bitmapManager.StoreBitmap(nullBitmap)
		if err != nil {
			return err
		}
		outputCol.metadata.NullBitmapPageID = nullBitmapPageID
		outputCol.metadata.NullCount = uint64(nullBitmap.GetCardinality())
	}
	
	return nil
}

// buildRowKeyIndexStreaming builds the row-key index using row merge iterator
func buildRowKeyIndexStreaming(output *ColumnarFile, inputs []*ColumnarFile, colName string, colInfo ColumnInfo, globalDeletedBitmap *roaring.Bitmap, outputCol *Column) error {
	// Create row iterators for all input files
	rowIterators := make([]RowIterator, 0, len(inputs))
	
	for _, cf := range inputs {
		col, exists := cf.columns[colName]
		if !exists {
			continue
		}
		
		iter, err := NewColumnRowIterator(col, colInfo.DataType)
		if err != nil {
			for _, it := range rowIterators {
				it.Close()
			}
			return fmt.Errorf("failed to create row iterator: %w", err)
		}
		
		rowIterators = append(rowIterators, iter)
	}
	
	if len(rowIterators) == 0 {
		return nil
	}
	
	defer func() {
		for _, iter := range rowIterators {
			iter.Close()
		}
	}()
	
	// Create row merge iterator
	rowMergeIter := NewRowMergeIterator(rowIterators)
	defer rowMergeIter.Close()
	
	// Initialize streaming B-tree builder for row-key index
	builder := NewBTreeStreamingBuilder(outputCol.rowKeyBTree)
	
	// Process rows in order
	for rowMergeIter.Next() {
		rowNum := rowMergeIter.RowNum()
		
		// Skip deleted rows
		if globalDeletedBitmap != nil && globalDeletedBitmap.Contains(uint32(rowNum)) {
			continue
		}
		
		keyValue := rowMergeIter.KeyValue()
		
		// Handle different value types
		var keyUint64 uint64
		if keyValue == nil {
			// NULL value
			keyUint64 = 0
		} else {
			switch colInfo.DataType {
			case DataTypeString:
				// For strings, need to find the new offset
				strValue := keyValue.(string)
				offset, found, err := outputCol.btree.FindStringOffset(strValue, outputCol.stringSegment)
				if err != nil {
					return fmt.Errorf("failed to find string offset: %w", err)
				}
				if !found {
					// This shouldn't happen if merge was successful
					continue
				}
				keyUint64 = offset
			default:
				// For non-strings, use the value directly
				keyUint64 = toUint64WithType(keyValue, colInfo.DataType)
			}
		}
		
		// Add to row-key B-tree
		value := NewRowNumberValue(keyUint64)
		if err := builder.Add(rowNum, value); err != nil {
			return fmt.Errorf("failed to add row-key entry: %w", err)
		}
	}
	
	// Also add null rows
	if outputCol.nullBitmap != nil {
		nullRows := make([]uint64, 0)
		nullIter := outputCol.nullBitmap.Iterator()
		for nullIter.HasNext() {
			nullRows = append(nullRows, uint64(nullIter.Next()))
		}
		
		// Sort null rows
		sort.Slice(nullRows, func(i, j int) bool {
			return nullRows[i] < nullRows[j]
		})
		
		// Add null rows to row-key index
		for _, rowNum := range nullRows {
			value := NewRowNumberValue(0) // NULL rows get value 0
			if err := builder.Add(rowNum, value); err != nil {
				return fmt.Errorf("failed to add null row-key entry: %w", err)
			}
		}
	}
	
	// Finalize the row-key B-tree
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("failed to finalize row-key B-tree: %w", err)
	}
	
	// Update metadata
	outputCol.metadata.RowKeyRootPageID = outputCol.rowKeyBTree.GetRootPageID()
	outputCol.metadata.RowKeyTreeHeight = outputCol.rowKeyBTree.GetHeight()
	
	return nil
}