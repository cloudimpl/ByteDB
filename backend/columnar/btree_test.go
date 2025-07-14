package columnar

import (
	"os"
	"testing"
)

func TestBPlusTreeBasic(t *testing.T) {
	tmpFile := "test_btree.db"
	defer os.Remove(tmpFile)
	
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	// Test 1: Empty tree
	t.Run("EmptyTree", func(t *testing.T) {
		tree := NewBPlusTree(pm, DataTypeInt64, nil)
		
		results, err := tree.Find(42)
		if err != nil {
			t.Errorf("Find on empty tree failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected empty results from empty tree, got %v", results)
		}
		
		results, err = tree.RangeSearch(10, 50)
		if err != nil {
			t.Errorf("RangeSearch on empty tree failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected empty results from empty tree range search, got %v", results)
		}
	})
	
	// Test 2: Bulk load and search
	t.Run("BulkLoadAndSearch", func(t *testing.T) {
		tree := NewBPlusTree(pm, DataTypeInt64, nil)
		
		// Create test entries
		entries := []BTreeLeafEntry{
			{Key: 10, Value: NewRowNumberValue(100)},
			{Key: 20, Value: NewRowNumberValue(200)},
			{Key: 30, Value: NewRowNumberValue(300)},
			{Key: 40, Value: NewRowNumberValue(400)},
			{Key: 50, Value: NewRowNumberValue(500)},
		}
		
		// Bulk load
		if err := tree.BulkLoad(entries); err != nil {
			t.Fatalf("BulkLoad failed: %v", err)
		}
		
		// Test exact searches
		testCases := []struct {
			key      uint64
			expected uint64
		}{
			{10, 100},
			{30, 300},
			{50, 500},
		}
		
		for _, tc := range testCases {
			results, err := tree.Find(tc.key)
			if err != nil {
				t.Errorf("Find(%d) failed: %v", tc.key, err)
				continue
			}
			
			if len(results) != 1 || results[0] != tc.expected {
				t.Errorf("Find(%d) = %v, expected [%d]", tc.key, results, tc.expected)
			}
			
			// Validate using bitmap API as well
			bitmap, err := tree.FindBitmap(tc.key)
			if err != nil {
				t.Errorf("FindBitmap(%d) failed: %v", tc.key, err)
				continue
			}
			bitmapResults := BitmapToSlice(bitmap)
			if len(bitmapResults) != 1 || bitmapResults[0] != tc.expected {
				t.Errorf("FindBitmap(%d) = %v, expected [%d]", tc.key, bitmapResults, tc.expected)
			}
		}
		
		// Test not found
		results, err := tree.Find(25)
		if err != nil {
			t.Errorf("Find(25) failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Find(25) = %v, expected empty", results)
		}
	})
	
	// Test 3: Range searches
	t.Run("RangeSearch", func(t *testing.T) {
		tree := NewBPlusTree(pm, DataTypeInt64, nil)
		
		// Create more entries
		entries := make([]BTreeLeafEntry, 0)
		for i := uint64(0); i < 100; i += 10 {
			entries = append(entries, BTreeLeafEntry{
				Key:   i,
				Value: NewRowNumberValue(i * 10),
			})
		}
		
		if err := tree.BulkLoad(entries); err != nil {
			t.Fatalf("BulkLoad failed: %v", err)
		}
		
		// Test various ranges
		testCases := []struct {
			min      uint64
			max      uint64
			expected int
		}{
			{20, 50, 4},   // 20, 30, 40, 50
			{0, 30, 4},    // 0, 10, 20, 30
			{75, 95, 2},   // 80, 90
			{100, 200, 0}, // Out of range
		}
		
		for _, tc := range testCases {
			results, err := tree.RangeSearch(tc.min, tc.max)
			if err != nil {
				t.Errorf("RangeSearch(%d, %d) failed: %v", tc.min, tc.max, err)
				continue
			}
			
			if len(results) != tc.expected {
				t.Errorf("RangeSearch(%d, %d) returned %d results, expected %d",
					tc.min, tc.max, len(results), tc.expected)
			}
			
			// Validate actual data: all results should be in range
			for _, result := range results {
				// Convert back to key: result = key * 10, so key = result / 10
				key := result / 10
				if key < tc.min || key > tc.max {
					t.Errorf("RangeSearch(%d, %d) returned result %d (key %d) outside range",
						tc.min, tc.max, result, key)
				}
			}
			
			// Test bitmap version as well
			bitmap, err := tree.RangeSearchBitmap(tc.min, tc.max)
			if err != nil {
				t.Errorf("RangeSearchBitmap(%d, %d) failed: %v", tc.min, tc.max, err)
				continue
			}
			bitmapResults := BitmapToSlice(bitmap)
			if len(bitmapResults) != tc.expected {
				t.Errorf("RangeSearchBitmap(%d, %d) returned %d results, expected %d",
					tc.min, tc.max, len(bitmapResults), tc.expected)
			}
			
			// Both methods should return same results
			if len(results) == len(bitmapResults) {
				for i, result := range results {
					if result != bitmapResults[i] {
						t.Errorf("RangeSearch and RangeSearchBitmap returned different results at index %d: %d vs %d",
							i, result, bitmapResults[i])
					}
				}
			}
		}
	})
}

func TestBPlusTreeWithStringKeys(t *testing.T) {
	tmpFile := "test_btree_string.db"
	defer os.Remove(tmpFile)
	
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	// Create string segment
	ss := NewStringSegment(pm)
	
	// Add test strings and get their offsets
	testData := []struct {
		str    string
		rowNum uint64
	}{
		{"apple", 1},
		{"banana", 2},
		{"cherry", 3},
		{"date", 4},
		{"elderberry", 5},
	}
	
	entries := make([]BTreeLeafEntry, 0)
	for _, td := range testData {
		offset := ss.AddString(td.str)
		entries = append(entries, BTreeLeafEntry{
			Key:   offset,
			Value: NewRowNumberValue(td.rowNum),
		})
	}
	
	// Build string segment
	if err := ss.Build(); err != nil {
		t.Fatalf("Failed to build string segment: %v", err)
	}
	
	// Create B+ tree with string comparator
	comparator := NewStringComparator(ss)
	tree := NewBPlusTree(pm, DataTypeString, comparator)
	
	// Bulk load
	if err := tree.BulkLoad(entries); err != nil {
		t.Fatalf("BulkLoad failed: %v", err)
	}
	
	// Test searches
	t.Run("StringKeySearch", func(t *testing.T) {
		// Find "cherry"
		cherryOffset, found := ss.FindOffset("cherry")
		if !found {
			t.Fatal("Failed to find cherry offset")
		}
		
		results, err := tree.Find(cherryOffset)
		if err != nil {
			t.Errorf("Find(cherry) failed: %v", err)
		}
		if len(results) != 1 || results[0] != 3 {
			t.Errorf("Find(cherry) = %v, expected [3]", results)
		}
		
		// Range search from "banana" to "date"
		bananaOffset, _ := ss.FindOffset("banana")
		dateOffset, _ := ss.FindOffset("date")
		
		results, err = tree.RangeSearch(bananaOffset, dateOffset)
		if err != nil {
			t.Errorf("RangeSearch failed: %v", err)
		}
		
		// Should return rows for banana(2), cherry(3), date(4)
		if len(results) != 3 {
			t.Errorf("RangeSearch returned %d results, expected 3", len(results))
		}
	})
}

func TestBPlusTreeLargeScale(t *testing.T) {
	tmpFile := "test_btree_large.db"
	defer os.Remove(tmpFile)
	
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	tree := NewBPlusTree(pm, DataTypeInt64, nil)
	
	// Create many entries
	numEntries := 10000
	entries := make([]BTreeLeafEntry, 0, numEntries)
	
	for i := 0; i < numEntries; i++ {
		entries = append(entries, BTreeLeafEntry{
			Key:   uint64(i * 2), // Even numbers
			Value: NewRowNumberValue(uint64(i)),
		})
	}
	
	// Bulk load
	if err := tree.BulkLoad(entries); err != nil {
		t.Fatalf("BulkLoad failed: %v", err)
	}
	
	// Test searches
	t.Run("LargeScaleSearch", func(t *testing.T) {
		// Test exact searches
		for i := 0; i < 100; i++ {
			key := uint64(i * 20)
			results, err := tree.Find(key)
			if err != nil {
				t.Errorf("Find(%d) failed: %v", key, err)
				continue
			}
			
			if len(results) != 1 || results[0] != uint64(i*10) {
				t.Errorf("Find(%d) = %v, expected [%d]", key, results, i*10)
			}
		}
		
		// Test range search
		results, err := tree.RangeSearch(1000, 2000)
		if err != nil {
			t.Errorf("RangeSearch failed: %v", err)
		}
		
		// Should return 501 entries (1000, 1002, ..., 2000)
		if len(results) != 501 {
			t.Errorf("RangeSearch returned %d results, expected 501", len(results))
		}
	})
	
	// Verify tree structure
	t.Run("TreeStructure", func(t *testing.T) {
		rootPage, err := pm.ReadPage(tree.GetRootPageID())
		if err != nil {
			t.Fatalf("Failed to read root page: %v", err)
		}
		
		// With 10000 entries and ~100 entries per leaf, we should have internal nodes
		if rootPage.Header.PageType == PageTypeBTreeLeaf {
			t.Error("Expected internal node as root for large tree")
		}
		
		// Verify tree height
		if tree.height < 2 {
			t.Errorf("Expected tree height >= 2, got %d", tree.height)
		}
	})
}

func TestValueTypes(t *testing.T) {
	// Test Value type methods
	t.Run("RowNumberValue", func(t *testing.T) {
		v := NewRowNumberValue(42)
		
		if !v.IsRowNumber() {
			t.Error("Expected IsRowNumber() to be true")
		}
		if v.IsBitmap() {
			t.Error("Expected IsBitmap() to be false")
		}
		if v.GetRowNumber() != 42 {
			t.Errorf("Expected row number 42, got %d", v.GetRowNumber())
		}
	})
	
	t.Run("BitmapValue", func(t *testing.T) {
		v := NewBitmapValue(1024)
		
		if v.IsRowNumber() {
			t.Error("Expected IsRowNumber() to be false")
		}
		if !v.IsBitmap() {
			t.Error("Expected IsBitmap() to be true")
		}
		if v.GetBitmapOffset() != 1024 {
			t.Errorf("Expected bitmap offset 1024, got %d", v.GetBitmapOffset())
		}
	})
	
	// Test that high bit is properly masked
	t.Run("ValueMasking", func(t *testing.T) {
		// Test with maximum valid value
		maxValue := ValueMask
		v := NewRowNumberValue(maxValue)
		if v.GetRowNumber() != maxValue {
			t.Errorf("Value masking failed: expected %d, got %d", maxValue, v.GetRowNumber())
		}
		
		// Test bitmap with maximum offset
		v = NewBitmapValue(maxValue)
		if v.GetBitmapOffset() != maxValue {
			t.Errorf("Bitmap offset masking failed: expected %d, got %d", maxValue, v.GetBitmapOffset())
		}
	})
}

// Benchmark tests
func BenchmarkBPlusTreeFind(b *testing.B) {
	tmpFile := "bench_btree.db"
	defer os.Remove(tmpFile)
	
	pm, _ := NewPageManager(tmpFile, true)
	defer pm.Close()
	
	tree := NewBPlusTree(pm, DataTypeInt64, nil)
	
	// Load test data
	entries := make([]BTreeLeafEntry, 100000)
	for i := range entries {
		entries[i] = BTreeLeafEntry{
			Key:   uint64(i),
			Value: NewRowNumberValue(uint64(i)),
		}
	}
	tree.BulkLoad(entries)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		key := uint64(i % 100000)
		tree.Find(key)
	}
}

func BenchmarkBPlusTreeRangeSearch(b *testing.B) {
	tmpFile := "bench_btree_range.db"
	defer os.Remove(tmpFile)
	
	pm, _ := NewPageManager(tmpFile, true)
	defer pm.Close()
	
	tree := NewBPlusTree(pm, DataTypeInt64, nil)
	
	// Load test data
	entries := make([]BTreeLeafEntry, 100000)
	for i := range entries {
		entries[i] = BTreeLeafEntry{
			Key:   uint64(i),
			Value: NewRowNumberValue(uint64(i)),
		}
	}
	tree.BulkLoad(entries)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		min := uint64(i % 90000)
		max := min + 1000
		tree.RangeSearch(min, max)
	}
}