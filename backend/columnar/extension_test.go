package columnar

import (
	"os"
	"testing"
)

func TestFileExtensionValidation(t *testing.T) {
	t.Run("CreateFile_InvalidExtension", func(t *testing.T) {
		// Test various invalid extensions
		invalidFiles := []string{
			"test_file.db",
			"test_file.txt",
			"test_file.dat",
			"test_file",       // no extension
			"test.file.csv",
		}

		for _, filename := range invalidFiles {
			_, err := CreateFile(filename)
			if err == nil {
				t.Errorf("Expected error for filename %s, but got nil", filename)
			}
			expectedError := "columnar files must have .bytedb extension"
			if err != nil && !contains(err.Error(), expectedError) {
				t.Errorf("Expected error containing '%s', got: %v", expectedError, err)
			}
		}
	})

	t.Run("CreateFile_ValidExtension", func(t *testing.T) {
		validFile := "test_file.bytedb"
		defer os.Remove(validFile)

		cf, err := CreateFile(validFile)
		if err != nil {
			t.Fatalf("Failed to create file with valid extension: %v", err)
		}
		cf.Close()

		// Verify file was created
		if _, err := os.Stat(validFile); os.IsNotExist(err) {
			t.Error("File was not created despite valid extension")
		}
	})

	t.Run("OpenFile_InvalidExtension", func(t *testing.T) {
		// Test opening with invalid extensions
		invalidFiles := []string{
			"nonexistent.db",
			"nonexistent.txt",
			"nonexistent",
		}

		for _, filename := range invalidFiles {
			_, err := OpenFile(filename)
			if err == nil {
				t.Errorf("Expected error for filename %s, but got nil", filename)
			}
			expectedError := "columnar files must have .bytedb extension"
			if err != nil && !contains(err.Error(), expectedError) {
				t.Errorf("Expected error containing '%s', got: %v", expectedError, err)
			}
		}
	})

	t.Run("OpenFile_ValidExtension", func(t *testing.T) {
		// First create a valid file
		validFile := "test_open.bytedb"
		defer os.Remove(validFile)

		cf, err := CreateFile(validFile)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		cf.AddColumn("test_col", DataTypeInt64, false)
		cf.Close()

		// Now try to open it
		cf2, err := OpenFile(validFile)
		if err != nil {
			t.Fatalf("Failed to open file with valid extension: %v", err)
		}
		defer cf2.Close()

		// Verify we can access the column
		cols := cf2.GetColumns()
		if len(cols) != 1 || cols[0] != "test_col" {
			t.Error("Failed to read column from reopened file")
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr || 
		   len(s) >= len(substr) && s[:len(substr)] == substr ||
		   len(s) > len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}