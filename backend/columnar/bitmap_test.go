package columnar

import (
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func TestBitmapManager(t *testing.T) {
	tmpFile := "test_bitmap.db"
	defer os.Remove(tmpFile)
	
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	bm := NewBitmapManager(pm)
	
	// Test 1: Store and load small bitmap
	t.Run("SmallBitmap", func(t *testing.T) {
		// Create bitmap
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(2)
		bitmap.Add(3)
		bitmap.Add(5)
		bitmap.Add(8)
		bitmap.Add(13)
		bitmap.Add(21)
		
		// Store bitmap
		pageID, err := bm.StoreBitmap(bitmap)
		if err != nil {
			t.Fatalf("Failed to store bitmap: %v", err)
		}
		
		// Load bitmap
		loaded, err := bm.LoadBitmap(pageID)
		if err != nil {
			t.Fatalf("Failed to load bitmap: %v", err)
		}
		
		// Verify
		if !bitmap.Equals(loaded) {
			t.Errorf("Loaded bitmap doesn't match original")
		}
		
		// Extract row numbers
		rows := bm.ExtractRowNumbers(loaded)
		expected := []uint64{1, 2, 3, 5, 8, 13, 21}
		
		if len(rows) != len(expected) {
			t.Errorf("Expected %d rows, got %d", len(expected), len(rows))
		}
		
		for i, row := range rows {
			if row != expected[i] {
				t.Errorf("Row %d: expected %d, got %d", i, expected[i], row)
			}
		}
	})
	
	// Test 2: Large bitmap (multi-page)
	t.Run("LargeBitmap", func(t *testing.T) {
		// Create large bitmap
		bitmap := roaring.New()
		for i := uint32(0); i < 100000; i += 3 {
			bitmap.Add(i)
		}
		
		// Store bitmap
		pageID, err := bm.StoreBitmap(bitmap)
		if err != nil {
			t.Fatalf("Failed to store large bitmap: %v", err)
		}
		
		// Load bitmap
		loaded, err := bm.LoadBitmap(pageID)
		if err != nil {
			t.Fatalf("Failed to load large bitmap: %v", err)
		}
		
		// Verify
		if !bitmap.Equals(loaded) {
			t.Errorf("Loaded large bitmap doesn't match original")
		}
		
		// Verify cardinality
		if loaded.GetCardinality() != bitmap.GetCardinality() {
			t.Errorf("Cardinality mismatch: expected %d, got %d",
				bitmap.GetCardinality(), loaded.GetCardinality())
		}
	})
	
	// Test 3: Bitmap operations
	t.Run("BitmapOperations", func(t *testing.T) {
		// Create test bitmaps
		bitmap1 := roaring.New()
		bitmap1.AddMany([]uint32{1, 3, 5, 7, 9})
		
		bitmap2 := roaring.New()
		bitmap2.AddMany([]uint32{2, 3, 5, 8, 9})
		
		bitmap3 := roaring.New()
		bitmap3.AddMany([]uint32{3, 5, 9, 11, 13})
		
		// Store bitmaps
		pageID1, _ := bm.StoreBitmap(bitmap1)
		pageID2, _ := bm.StoreBitmap(bitmap2)
		pageID3, _ := bm.StoreBitmap(bitmap3)
		
		// Test intersection
		intersect, err := bm.IntersectBitmaps([]uint64{pageID1, pageID2, pageID3})
		if err != nil {
			t.Fatalf("Intersection failed: %v", err)
		}
		
		// Should contain only 3, 5, 9
		expectedIntersect := roaring.New()
		expectedIntersect.AddMany([]uint32{3, 5, 9})
		
		if !intersect.Equals(expectedIntersect) {
			t.Errorf("Intersection result incorrect")
		}
		
		// Test union
		union, err := bm.UnionBitmaps([]uint64{pageID1, pageID2, pageID3})
		if err != nil {
			t.Fatalf("Union failed: %v", err)
		}
		
		// Should contain all unique values
		expectedUnion := roaring.New()
		expectedUnion.AddMany([]uint32{1, 2, 3, 5, 7, 8, 9, 11, 13})
		
		if !union.Equals(expectedUnion) {
			t.Errorf("Union result incorrect")
		}
	})
	
	// Test 4: CreateBitmapFromRows
	t.Run("CreateFromRows", func(t *testing.T) {
		rows := []uint64{10, 20, 30, 40, 50}
		
		bitmap, pageID, err := bm.CreateBitmapFromRows(rows)
		if err != nil {
			t.Fatalf("CreateBitmapFromRows failed: %v", err)
		}
		
		// Verify bitmap contents
		for _, row := range rows {
			if !bitmap.Contains(uint32(row)) {
				t.Errorf("Bitmap missing row %d", row)
			}
		}
		
		// Load and verify
		loaded, err := bm.LoadBitmap(pageID)
		if err != nil {
			t.Fatalf("Failed to load created bitmap: %v", err)
		}
		
		if !bitmap.Equals(loaded) {
			t.Errorf("Loaded bitmap doesn't match created bitmap")
		}
	})
	
	// Test 5: GetCardinality without loading full bitmap
	t.Run("GetCardinality", func(t *testing.T) {
		bitmap := roaring.New()
		bitmap.AddMany([]uint32{1, 10, 100, 1000, 10000})
		
		pageID, _ := bm.StoreBitmap(bitmap)
		
		cardinality, err := bm.GetCardinality(pageID)
		if err != nil {
			t.Fatalf("GetCardinality failed: %v", err)
		}
		
		if cardinality != 5 {
			t.Errorf("Expected cardinality 5, got %d", cardinality)
		}
	})
}

func TestBPlusTreeWithBitmaps(t *testing.T) {
	tmpFile := "test_btree_bitmap.db"
	defer os.Remove(tmpFile)
	
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	tree := NewBPlusTree(pm, DataTypeInt64, nil)
	
	// Test BulkLoadWithDuplicates
	t.Run("BulkLoadWithDuplicates", func(t *testing.T) {
		// Create test data with duplicates
		keyRows := []struct{Key uint64; RowNum uint64}{
			{10, 100},
			{10, 101},
			{10, 102},
			{20, 200}, // Single value
			{30, 300},
			{30, 301},
			{30, 302},
			{30, 303},
			{30, 304},
		}
		
		_, err := tree.BulkLoadWithDuplicates(keyRows)
		if err != nil {
			t.Fatalf("BulkLoadWithDuplicates failed: %v", err)
		}
		
		// Test searches
		
		// Key 10 should return 3 rows
		results, err := tree.Find(10)
		if err != nil {
			t.Errorf("Find(10) failed: %v", err)
		}
		if len(results) != 3 {
			t.Errorf("Find(10) returned %d results, expected 3", len(results))
		}
		
		// Key 20 should return 1 row
		results, err = tree.Find(20)
		if err != nil {
			t.Errorf("Find(20) failed: %v", err)
		}
		if len(results) != 1 || results[0] != 200 {
			t.Errorf("Find(20) = %v, expected [200]", results)
		}
		
		// Key 30 should return 5 rows
		results, err = tree.Find(30)
		if err != nil {
			t.Errorf("Find(30) failed: %v", err)
		}
		if len(results) != 5 {
			t.Errorf("Find(30) returned %d results, expected 5", len(results))
		}
		
		// Range search should work correctly
		results, err = tree.RangeSearch(10, 30)
		if err != nil {
			t.Errorf("RangeSearch failed: %v", err)
		}
		if len(results) != 9 { // 3 + 1 + 5
			t.Errorf("RangeSearch returned %d results, expected 9", len(results))
		}
	})
}

func BenchmarkBitmapOperations(b *testing.B) {
	tmpFile := "bench_bitmap.db"
	defer os.Remove(tmpFile)
	
	pm, _ := NewPageManager(tmpFile, true)
	defer pm.Close()
	
	bm := NewBitmapManager(pm)
	
	// Create test bitmaps
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()
	
	for i := uint32(0); i < 1000000; i++ {
		if i%2 == 0 {
			bitmap1.Add(i)
		}
		if i%3 == 0 {
			bitmap2.Add(i)
		}
	}
	
	pageID1, _ := bm.StoreBitmap(bitmap1)
	pageID2, _ := bm.StoreBitmap(bitmap2)
	
	b.ResetTimer()
	
	b.Run("Intersection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.IntersectBitmaps([]uint64{pageID1, pageID2})
		}
	})
	
	b.Run("Union", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bm.UnionBitmaps([]uint64{pageID1, pageID2})
		}
	})
}