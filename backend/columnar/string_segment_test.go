package columnar

import (
	"fmt"
	"os"
	"testing"
)

func TestStringSegment(t *testing.T) {
	// Create temporary file
	tmpFile := "test_string_segment.db"
	defer os.Remove(tmpFile)
	
	// Create page manager
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	// Test 1: Basic string addition and retrieval
	t.Run("BasicAddAndGet", func(t *testing.T) {
		ss := NewStringSegment(pm)
		
		// Add some strings
		offset1 := ss.AddString("hello")
		offset2 := ss.AddString("world")
		offset3 := ss.AddString("hello") // Duplicate
		
		// Verify duplicate returns same offset
		if offset1 != offset3 {
			t.Errorf("Duplicate string should return same offset: got %d and %d", offset1, offset3)
		}
		
		// Verify different strings have different offsets
		if offset1 == offset2 {
			t.Errorf("Different strings should have different offsets")
		}
		
		// Retrieve strings
		str1, err := ss.GetString(offset1)
		if err != nil || str1 != "hello" {
			t.Errorf("Failed to retrieve string: got %s, err %v", str1, err)
		}
		
		str2, err := ss.GetString(offset2)
		if err != nil || str2 != "world" {
			t.Errorf("Failed to retrieve string: got %s, err %v", str2, err)
		}
	})
	
	// Test 2: Build and persist
	t.Run("BuildAndPersist", func(t *testing.T) {
		ss := NewStringSegment(pm)
		
		// Add various strings
		testStrings := []string{
			"apple", "banana", "cherry", "date", "elderberry",
			"fig", "grape", "honeydew", "kiwi", "lemon",
		}
		
		offsets := make(map[string]uint64)
		for _, str := range testStrings {
			offsets[str] = ss.AddString(str)
		}
		
		// Build the segment
		if err := ss.Build(); err != nil {
			t.Fatalf("Failed to build segment: %v", err)
		}
		
		// Verify strings are still accessible
		for str := range offsets {
			actualOffset, found := ss.FindOffset(str)
			if !found {
				t.Errorf("String %s not found after build", str)
			}
			
			// Note: offsets may change after build due to sorting
			retrievedStr, err := ss.GetString(actualOffset)
			if err != nil || retrievedStr != str {
				t.Errorf("Failed to retrieve %s after build: got %s, err %v", str, retrievedStr, err)
			}
		}
	})
	
	// Test 3: String comparator
	t.Run("StringComparator", func(t *testing.T) {
		ss := NewStringSegment(pm)
		
		// Add strings
		offset1 := ss.AddString("apple")
		offset2 := ss.AddString("banana")
		offset3 := ss.AddString("apple") // Duplicate
		
		comp := NewStringComparator(ss)
		
		// Test comparisons
		result, err := comp.Compare(offset1, offset2)
		if err != nil || result >= 0 {
			t.Errorf("Expected apple < banana, got %d, err %v", result, err)
		}
		
		result, err = comp.Compare(offset2, offset1)
		if err != nil || result <= 0 {
			t.Errorf("Expected banana > apple, got %d, err %v", result, err)
		}
		
		result, err = comp.Compare(offset1, offset3)
		if err != nil || result != 0 {
			t.Errorf("Expected apple == apple, got %d, err %v", result, err)
		}
	})
	
	// Test 4: Find next greater/smaller
	t.Run("FindNextGreaterSmaller", func(t *testing.T) {
		ss := NewStringSegment(pm)
		
		// Add sorted strings
		strings := []string{"apple", "banana", "cherry", "date", "fig"}
		for _, str := range strings {
			ss.AddString(str)
		}
		
		// Find next greater than "banana"
		offset, err := ss.FindNextGreaterOffset("banana")
		if err != nil {
			t.Fatalf("Failed to find next greater: %v", err)
		}
		
		str, err := ss.GetString(offset)
		if err != nil || str != "cherry" {
			t.Errorf("Expected cherry as next greater than banana, got %s", str)
		}
		
		// Find next smaller than "cherry"
		offset, err = ss.FindNextSmallerOffset("cherry")
		if err != nil {
			t.Fatalf("Failed to find next smaller: %v", err)
		}
		
		str, err = ss.GetString(offset)
		if err != nil || str != "banana" {
			t.Errorf("Expected banana as next smaller than cherry, got %s", str)
		}
		
		// Test boundary cases
		_, err = ss.FindNextGreaterOffset("zebra")
		if err == nil {
			t.Error("Expected error when finding next greater than largest string")
		}
		
		_, err = ss.FindNextSmallerOffset("aardvark")
		if err == nil {
			t.Error("Expected error when finding next smaller than smallest string")
		}
	})
}

func TestStringSegmentLargeScale(t *testing.T) {
	// Create temporary file
	tmpFile := "test_string_segment_large.db"
	defer os.Remove(tmpFile)
	
	// Create page manager
	pm, err := NewPageManager(tmpFile, true)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pm.Close()
	
	ss := NewStringSegment(pm)
	
	// Generate many strings with duplicates
	uniqueCount := 1000
	totalCount := 10000
	
	// Add strings (many duplicates)
	offsets := make(map[string]uint64)
	for i := 0; i < totalCount; i++ {
		str := fmt.Sprintf("string_%d", i%uniqueCount)
		offset := ss.AddString(str)
		
		if existingOffset, exists := offsets[str]; exists {
			if offset != existingOffset {
				t.Errorf("Duplicate string returned different offset: %d vs %d", offset, existingOffset)
			}
		} else {
			offsets[str] = offset
		}
	}
	
	// Verify we have exactly uniqueCount unique strings
	if len(ss.stringMap) != uniqueCount {
		t.Errorf("Expected %d unique strings, got %d", uniqueCount, len(ss.stringMap))
	}
	
	// Build and verify
	if err := ss.Build(); err != nil {
		t.Fatalf("Failed to build large segment: %v", err)
	}
	
	// Spot check some strings
	for i := 0; i < 100; i++ {
		str := fmt.Sprintf("string_%d", i*10)
		offset, found := ss.FindOffset(str)
		if !found {
			t.Errorf("String %s not found", str)
			continue
		}
		
		retrieved, err := ss.GetString(offset)
		if err != nil || retrieved != str {
			t.Errorf("Failed to retrieve %s: got %s, err %v", str, retrieved, err)
		}
	}
}

func TestStringSegmentPersistence(t *testing.T) {
	tmpFile := "test_string_segment_persist.db"
	defer os.Remove(tmpFile)
	
	var rootPageID uint64
	
	// Phase 1: Create and write
	{
		pm, err := NewPageManager(tmpFile, true)
		if err != nil {
			t.Fatalf("Failed to create page manager: %v", err)
		}
		
		ss := NewStringSegment(pm)
		
		// Add strings
		testStrings := []string{"persistent", "data", "test", "verification"}
		for _, str := range testStrings {
			ss.AddString(str)
		}
		
		// Build
		if err := ss.Build(); err != nil {
			t.Fatalf("Failed to build: %v", err)
		}
		
		rootPageID = ss.rootPageID
		
		pm.Close()
	}
	
	// Phase 2: Read back
	{
		pm, err := NewPageManager(tmpFile, false)
		if err != nil {
			t.Fatalf("Failed to open page manager: %v", err)
		}
		defer pm.Close()
		
		ss := NewStringSegment(pm)
		ss.rootPageID = rootPageID
		
		// Test loading strings from disk
		// Note: We need to manually set up the offset since we're loading from disk
		// In real usage, this would be loaded from metadata
		
		// For this test, we'll just verify the segment structure is valid
		dirPage, err := pm.ReadPage(rootPageID)
		if err != nil {
			t.Fatalf("Failed to read directory page: %v", err)
		}
		
		if dirPage.Header.PageType != PageTypeStringSegment {
			t.Errorf("Expected string segment page type, got %v", dirPage.Header.PageType)
		}
		
		// Read string count from directory
		stringCount := ByteOrder.Uint32(dirPage.Data[0:4])
		if stringCount != 4 {
			t.Errorf("Expected 4 strings, got %d", stringCount)
		}
	}
}