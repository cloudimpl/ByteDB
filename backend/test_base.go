package main

import (
	"os"
	"testing"
)

// TestMain runs before all tests in the package
func TestMain(m *testing.M) {
	// Setup test data before running tests
	if err := SetupTestData(); err != nil {
		panic("Failed to setup test data: " + err.Error())
	}

	// Run tests
	code := m.Run()

	// Cleanup test data after tests
	if err := CleanupTestData(); err != nil {
		// Log but don't fail - tests already ran
		println("Warning: Failed to cleanup test data:", err.Error())
	}

	// Exit with test result code
	os.Exit(code)
}
