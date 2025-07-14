package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestSpaceEfficiencyDemo(t *testing.T) {
	// Test with data that has many distinct values (no duplicates)
	// This will show the true space savings in the B+ tree structure
	
	numEntries := 10000
	
	// Test 1: Byte keys (0-255 range)
	t.Run("ByteKeys", func(t *testing.T) {
		tmpFile := "test_byte_keys.bytedb"
		defer os.Remove(tmpFile)
		
		cf, _ := CreateFile(tmpFile)
		cf.AddColumn("value", DataTypeUint8, false)
		
		// Create data with unique values
		data := make([]struct{ Key uint64; RowNum uint64 }, numEntries)
		for i := 0; i < numEntries; i++ {
			data[i] = struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(i % 256), // Byte range
				RowNum: uint64(i),
			}
		}
		
		col := cf.columns["value"]
		col.btree.BulkLoadWithDuplicates(data)
		col.metadata.RootPageID = col.btree.GetRootPageID()
		
		leafPages := countPagesOfType(cf, PageTypeBTreeLeaf)
		internalPages := countPagesOfType(cf, PageTypeBTreeInternal)
		bitmapPages := countPagesOfType(cf, PageTypeRoaringBitmap)
		
		cf.Close()
		
		info, _ := os.Stat(tmpFile)
		t.Logf("Byte keys - File size: %d bytes, Leaf pages: %d, Internal pages: %d, Bitmap pages: %d",
			info.Size(), leafPages, internalPages, bitmapPages)
	})
	
	// Test 2: Same data with uint64 keys
	t.Run("Uint64Keys", func(t *testing.T) {
		tmpFile := "test_uint64_keys.bytedb"
		defer os.Remove(tmpFile)
		
		cf, _ := CreateFile(tmpFile)
		cf.AddColumn("value", DataTypeUint64, false)
		
		// Create same data
		data := make([]struct{ Key uint64; RowNum uint64 }, numEntries)
		for i := 0; i < numEntries; i++ {
			data[i] = struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(i % 256), // Same range as byte test
				RowNum: uint64(i),
			}
		}
		
		col := cf.columns["value"]
		col.btree.BulkLoadWithDuplicates(data)
		col.metadata.RootPageID = col.btree.GetRootPageID()
		
		leafPages := countPagesOfType(cf, PageTypeBTreeLeaf)
		internalPages := countPagesOfType(cf, PageTypeBTreeInternal)
		bitmapPages := countPagesOfType(cf, PageTypeRoaringBitmap)
		
		cf.Close()
		
		info, _ := os.Stat(tmpFile)
		t.Logf("Uint64 keys - File size: %d bytes, Leaf pages: %d, Internal pages: %d, Bitmap pages: %d",
			info.Size(), leafPages, internalPages, bitmapPages)
	})
	
	// Test 3: Unique values only (no bitmaps needed)
	t.Run("UniqueByteKeys", func(t *testing.T) {
		tmpFile := "test_unique_byte.bytedb"
		defer os.Remove(tmpFile)
		
		cf, _ := CreateFile(tmpFile)
		cf.AddColumn("value", DataTypeUint8, false)
		
		// Create data with unique values only
		uniqueEntries := 256 // Max for uint8
		data := make([]struct{ Key uint64; RowNum uint64 }, uniqueEntries)
		for i := 0; i < uniqueEntries; i++ {
			data[i] = struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(i),
				RowNum: uint64(i),
			}
		}
		
		col := cf.columns["value"]
		col.btree.BulkLoadWithDuplicates(data)
		col.metadata.RootPageID = col.btree.GetRootPageID()
		
		leafPages := countPagesOfType(cf, PageTypeBTreeLeaf)
		internalPages := countPagesOfType(cf, PageTypeBTreeInternal)
		
		cf.Close()
		
		info, _ := os.Stat(tmpFile)
		
		// Calculate entries per leaf page
		keySize := GetDataTypeSize(DataTypeUint8) // 1 byte
		valueSize := 8 // row number
		entrySize := keySize + valueSize
		maxEntriesPerPage := (PageSize - PageHeaderSize) / entrySize
		
		t.Logf("Unique byte keys - File size: %d bytes, Leaf pages: %d, Internal pages: %d", 
			info.Size(), leafPages, internalPages)
		t.Logf("Entry size: %d bytes, Max entries per page: %d", entrySize, maxEntriesPerPage)
	})
	
	// Test 4: Same with uint64
	t.Run("UniqueUint64Keys", func(t *testing.T) {
		tmpFile := "test_unique_uint64.bytedb"
		defer os.Remove(tmpFile)
		
		cf, _ := CreateFile(tmpFile)
		cf.AddColumn("value", DataTypeUint64, false)
		
		// Create same unique data
		uniqueEntries := 256
		data := make([]struct{ Key uint64; RowNum uint64 }, uniqueEntries)
		for i := 0; i < uniqueEntries; i++ {
			data[i] = struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(i),
				RowNum: uint64(i),
			}
		}
		
		col := cf.columns["value"]
		col.btree.BulkLoadWithDuplicates(data)
		col.metadata.RootPageID = col.btree.GetRootPageID()
		
		leafPages := countPagesOfType(cf, PageTypeBTreeLeaf)
		internalPages := countPagesOfType(cf, PageTypeBTreeInternal)
		
		cf.Close()
		
		info, _ := os.Stat(tmpFile)
		
		// Calculate entries per leaf page
		keySize := GetDataTypeSize(DataTypeUint64) // 8 bytes
		valueSize := 8 // row number
		entrySize := keySize + valueSize
		maxEntriesPerPage := (PageSize - PageHeaderSize) / entrySize
		
		t.Logf("Unique uint64 keys - File size: %d bytes, Leaf pages: %d, Internal pages: %d", 
			info.Size(), leafPages, internalPages)
		t.Logf("Entry size: %d bytes, Max entries per page: %d", entrySize, maxEntriesPerPage)
	})
}

func countPagesOfType(cf *ColumnarFile, pageType PageType) int {
	count := 0
	for i := uint64(0); i < cf.pageManager.GetPageCount(); i++ {
		page, err := cf.pageManager.ReadPage(i)
		if err == nil && page.Header.PageType == pageType {
			count++
		}
	}
	return count
}

func TestActualSpaceSavings(t *testing.T) {
	// Create a scenario where space savings are clearly visible
	numEntries := 5000
	
	results := make(map[string]int64)
	
	// Test different data types
	dataTypes := []struct {
		name string
		dt   DataType
	}{
		{"Bool", DataTypeBool},
		{"Uint8", DataTypeUint8},
		{"Uint16", DataTypeUint16},
		{"Uint32", DataTypeUint32},
		{"Uint64", DataTypeUint64},
	}
	
	for _, tc := range dataTypes {
		tmpFile := fmt.Sprintf("test_%s.bytedb", tc.name)
		defer os.Remove(tmpFile)
		
		cf, _ := CreateFile(tmpFile)
		cf.AddColumn("value", tc.dt, false)
		
		// Create data with good distribution
		data := make([]struct{ Key uint64; RowNum uint64 }, numEntries)
		for i := 0; i < numEntries; i++ {
			// Spread values across the full range to minimize bitmap usage
			data[i] = struct{ Key uint64; RowNum uint64 }{
				Key:    uint64(i),
				RowNum: uint64(i),
			}
		}
		
		col := cf.columns["value"]
		col.btree.BulkLoadWithDuplicates(data)
		col.metadata.RootPageID = col.btree.GetRootPageID()
		col.metadata.TotalKeys = uint64(len(data))
		
		cf.Close()
		
		info, _ := os.Stat(tmpFile)
		results[tc.name] = info.Size()
	}
	
	// Print comparison
	t.Log("\nSpace usage comparison for 5000 unique entries:")
	t.Log("================================================")
	baseline := results["Uint64"]
	for _, tc := range dataTypes {
		size := results[tc.name]
		savings := baseline - size
		percent := float64(savings) / float64(baseline) * 100
		t.Logf("%s: %d bytes (%.1f%% savings)", tc.name, size, percent)
	}
}