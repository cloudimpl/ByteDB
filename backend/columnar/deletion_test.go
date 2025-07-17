package columnar

import (
	"os"
	"testing"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// TestDeletedBitmap tests basic deletion functionality
func TestDeletedBitmap(t *testing.T) {
	filename := "test_deletion.bytedb"
	defer os.Remove(filename)
	
	// Create file
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	// Add column
	cf.AddColumn("id", DataTypeInt64, false)
	
	// Load data
	data := []IntData{
		NewIntData(100, 0),
		NewIntData(200, 1),
		NewIntData(300, 2),
		NewIntData(400, 3),
		NewIntData(500, 4),
	}
	cf.LoadIntColumn("id", data)
	
	// Test single row deletion
	t.Run("SingleRowDeletion", func(t *testing.T) {
		err := cf.DeleteRow(1)
		if err != nil {
			t.Fatal(err)
		}
		
		if !cf.IsRowDeleted(1) {
			t.Error("Row 1 should be marked as deleted")
		}
		
		if cf.IsRowDeleted(0) {
			t.Error("Row 0 should not be marked as deleted")
		}
		
		if cf.GetDeletedCount() != 1 {
			t.Errorf("Expected 1 deleted row, got %d", cf.GetDeletedCount())
		}
	})
	
	// Test multiple row deletion
	t.Run("MultipleRowDeletion", func(t *testing.T) {
		bitmap := roaring.New()
		bitmap.Add(2)
		bitmap.Add(3)
		
		err := cf.DeleteRows(bitmap)
		if err != nil {
			t.Fatal(err)
		}
		
		if !cf.IsRowDeleted(2) || !cf.IsRowDeleted(3) {
			t.Error("Rows 2 and 3 should be marked as deleted")
		}
		
		if cf.GetDeletedCount() != 3 {
			t.Errorf("Expected 3 deleted rows, got %d", cf.GetDeletedCount())
		}
	})
	
	// Test undelete
	t.Run("UndeleteRow", func(t *testing.T) {
		err := cf.UndeleteRow(1)
		if err != nil {
			t.Fatal(err)
		}
		
		if cf.IsRowDeleted(1) {
			t.Error("Row 1 should no longer be marked as deleted")
		}
		
		if cf.GetDeletedCount() != 2 {
			t.Errorf("Expected 2 deleted rows after undelete, got %d", cf.GetDeletedCount())
		}
	})
	
	// Test GetDeletedRows
	t.Run("GetDeletedRows", func(t *testing.T) {
		deletedBitmap := cf.GetDeletedRows()
		
		if deletedBitmap.GetCardinality() != 2 {
			t.Errorf("Expected 2 deleted rows, got %d", deletedBitmap.GetCardinality())
		}
		
		if !deletedBitmap.Contains(2) || !deletedBitmap.Contains(3) {
			t.Error("Deleted bitmap should contain rows 2 and 3")
		}
	})
	
	cf.Close()
	
	// Test persistence
	t.Run("DeletionPersistence", func(t *testing.T) {
		// Reopen file
		cf2, err := OpenFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		defer cf2.Close()
		
		// Check deleted rows are persisted
		if !cf2.IsRowDeleted(2) || !cf2.IsRowDeleted(3) {
			t.Error("Deleted rows should be persisted after reopening")
		}
		
		if cf2.GetDeletedCount() != 2 {
			t.Errorf("Expected 2 deleted rows after reopening, got %d", cf2.GetDeletedCount())
		}
	})
}

// TestDeletionWithNullData tests deletion with nullable columns
func TestDeletionWithNullData(t *testing.T) {
	filename := "test_deletion_null.bytedb"
	defer os.Remove(filename)
	
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Add nullable column
	cf.AddColumn("value", DataTypeInt64, true)
	
	// Load data with nulls
	data := []IntData{
		NewIntData(100, 0),
		NewNullIntData(1),
		NewIntData(300, 2),
		NewNullIntData(3),
		NewIntData(500, 4),
	}
	cf.LoadIntColumn("value", data)
	
	// Delete both null and non-null rows
	bitmap := roaring.New()
	bitmap.Add(1) // null row
	bitmap.Add(2) // non-null row
	
	err = cf.DeleteRows(bitmap)
	if err != nil {
		t.Fatal(err)
	}
	
	// Verify deletions
	if !cf.IsRowDeleted(1) || !cf.IsRowDeleted(2) {
		t.Error("Both null and non-null rows should be deletable")
	}
	
	if cf.GetDeletedCount() != 2 {
		t.Errorf("Expected 2 deleted rows, got %d", cf.GetDeletedCount())
	}
}

// TestReadOnlyDeletion tests that deletion fails on read-only files
func TestReadOnlyDeletion(t *testing.T) {
	filename := "test_readonly_deletion.bytedb"
	defer os.Remove(filename)
	
	// Create file with data
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	data := []IntData{NewIntData(100, 0)}
	cf.LoadIntColumn("id", data)
	cf.Close()
	
	// Reopen as read-only
	cf, err = OpenFileReadOnly(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	// Try to delete - should fail
	err = cf.DeleteRow(0)
	if err == nil {
		t.Error("DeleteRow should fail on read-only file")
	}
	
	bitmap := roaring.New()
	bitmap.Add(0)
	err = cf.DeleteRows(bitmap)
	if err == nil {
		t.Error("DeleteRows should fail on read-only file")
	}
	
	err = cf.UndeleteRow(0)
	if err == nil {
		t.Error("UndeleteRow should fail on read-only file")
	}
}

// TestEmptyDeletionBitmap tests behavior with no deletions
func TestEmptyDeletionBitmap(t *testing.T) {
	filename := "test_empty_deletion.bytedb"
	defer os.Remove(filename)
	
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	data := []IntData{
		NewIntData(100, 0),
		NewIntData(200, 1),
	}
	cf.LoadIntColumn("id", data)
	
	// Check no deletions
	if cf.GetDeletedCount() != 0 {
		t.Errorf("Expected 0 deleted rows, got %d", cf.GetDeletedCount())
	}
	
	if cf.IsRowDeleted(0) || cf.IsRowDeleted(1) {
		t.Error("No rows should be marked as deleted")
	}
	
	deletedBitmap := cf.GetDeletedRows()
	if deletedBitmap.GetCardinality() != 0 {
		t.Error("Deleted bitmap should be empty")
	}
	
	cf.Close()
	
	// Reopen and verify no deleted bitmap is saved
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()
	
	if cf2.GetDeletedCount() != 0 {
		t.Error("No deleted bitmap should be loaded for file with no deletions")
	}
}

// TestDeletionEdgeCases tests edge cases
func TestDeletionEdgeCases(t *testing.T) {
	filename := "test_deletion_edge.bytedb"
	defer os.Remove(filename)
	
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf.Close()
	
	cf.AddColumn("id", DataTypeInt64, false)
	
	// Test deletion on empty file
	err = cf.DeleteRow(0)
	if err != nil {
		t.Fatal(err)
	}
	
	if !cf.IsRowDeleted(0) {
		t.Error("Should be able to mark non-existent row as deleted")
	}
	
	// Test nil bitmap deletion
	err = cf.DeleteRows(nil)
	if err != nil {
		t.Error("DeleteRows with nil bitmap should not error")
	}
	
	// Test deleting already deleted row
	err = cf.DeleteRow(0)
	if err != nil {
		t.Error("Deleting already deleted row should not error")
	}
	
	if cf.GetDeletedCount() != 1 {
		t.Error("Deleting same row twice should not increase count")
	}
	
	// Test undeleting non-deleted row
	err = cf.UndeleteRow(100)
	if err != nil {
		t.Error("Undeleting non-deleted row should not error")
	}
}

// TestLargeDeletionBitmap tests with many deletions
func TestLargeDeletionBitmap(t *testing.T) {
	filename := "test_large_deletion.bytedb"
	defer os.Remove(filename)
	
	cf, err := CreateFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	
	cf.AddColumn("id", DataTypeInt64, false)
	
	// Create large dataset
	numRows := 10000
	data := make([]IntData, numRows)
	for i := 0; i < numRows; i++ {
		data[i] = NewIntData(int64(i*100), uint64(i))
	}
	cf.LoadIntColumn("id", data)
	
	// Delete every other row
	deleteBitmap := roaring.New()
	for i := 0; i < numRows; i += 2 {
		deleteBitmap.Add(uint32(i))
	}
	
	err = cf.DeleteRows(deleteBitmap)
	if err != nil {
		t.Fatal(err)
	}
	
	expectedDeleted := uint64(numRows / 2)
	if cf.GetDeletedCount() != expectedDeleted {
		t.Errorf("Expected %d deleted rows, got %d", expectedDeleted, cf.GetDeletedCount())
	}
	
	cf.Close()
	
	// Verify persistence with large bitmap
	cf2, err := OpenFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer cf2.Close()
	
	if cf2.GetDeletedCount() != expectedDeleted {
		t.Errorf("Expected %d deleted rows after reopening, got %d", expectedDeleted, cf2.GetDeletedCount())
	}
	
	// Spot check some deletions
	for i := 0; i < 100; i += 2 {
		if !cf2.IsRowDeleted(uint64(i)) {
			t.Errorf("Row %d should be deleted", i)
		}
		if cf2.IsRowDeleted(uint64(i + 1)) {
			t.Errorf("Row %d should not be deleted", i+1)
		}
	}
}