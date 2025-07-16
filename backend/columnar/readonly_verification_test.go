package columnar

import (
	"fmt"
	"os"
	"testing"
)

// TestReadOnlyVerification does a simple verification that read-only mode
// works correctly with compression
func TestReadOnlyVerification(t *testing.T) {
	compressionTypes := []struct {
		name  string
		cType CompressionType
	}{
		{"None", CompressionNone},
		{"Snappy", CompressionSnappy},
		{"Gzip", CompressionGzip},
		{"Zstd", CompressionZstd},
	}

	for _, ct := range compressionTypes {
		t.Run(ct.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_readonly_verify_%s.bytedb", ct.name)
			defer os.Remove(filename)

			// Create file with 100 simple records
			var cf *ColumnarFile
			var err error

			if ct.cType == CompressionNone {
				cf, err = CreateFile(filename)
			} else {
				opts := NewCompressionOptions().
					WithPageCompression(ct.cType, CompressionLevelDefault)
				cf, err = CreateFileWithOptions(filename, opts)
			}

			if err != nil {
				t.Fatal(err)
			}

			// Add simple columns
			cf.AddColumn("id", DataTypeInt64, false)
			cf.AddColumn("name", DataTypeString, false)

			// Add 100 rows
			intData := make([]IntData, 100)
			stringData := make([]StringData, 100)

			for i := 0; i < 100; i++ {
				intData[i] = IntData{Value: int64(i), RowNum: uint64(i)}
				stringData[i] = StringData{Value: fmt.Sprintf("Name_%d", i), RowNum: uint64(i)}
			}

			cf.LoadIntColumn("id", intData)
			cf.LoadStringColumn("name", stringData)
			
			// Get stats before closing
			pageCount := cf.pageManager.GetPageCount()
			stats := cf.pageManager.GetCompressionStats()
			
			cf.Close()

			// Get file size
			info, _ := os.Stat(filename)
			fileSize := info.Size()

			t.Logf("%s - File size: %d bytes, Pages: %d, Compression ratio: %.2f",
				ct.name, fileSize, pageCount, stats.CompressionRatio)

			// Open in read-only mode
			cfRO, err := OpenFileReadOnly(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer cfRO.Close()

			// Verify basic queries work
			tests := []struct {
				name string
				test func() error
			}{
				{
					"QueryInt",
					func() error {
						bitmap, err := cfRO.QueryInt("id", 50)
						if err != nil {
							return err
						}
						if bitmap.GetCardinality() != 1 {
							return fmt.Errorf("expected 1 result, got %d", bitmap.GetCardinality())
						}
						return nil
					},
				},
				{
					"RangeQuery",
					func() error {
						bitmap, err := cfRO.RangeQueryInt("id", 10, 20)
						if err != nil {
							return err
						}
						if bitmap.GetCardinality() != 11 {
							return fmt.Errorf("expected 11 results, got %d", bitmap.GetCardinality())
						}
						return nil
					},
				},
				{
					"StringQuery",
					func() error {
						bitmap, err := cfRO.QueryString("name", "Name_25")
						if err != nil {
							return err
						}
						if bitmap.GetCardinality() != 1 {
							return fmt.Errorf("expected 1 result, got %d", bitmap.GetCardinality())
						}
						return nil
					},
				},
				{
					"RowLookupInt",
					func() error {
						val, found, err := cfRO.LookupIntByRow("id", 75)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("row 75 not found")
						}
						if val != 75 {
							return fmt.Errorf("expected 75, got %d", val)
						}
						return nil
					},
				},
				{
					"RowLookupString",
					func() error {
						val, found, err := cfRO.LookupStringByRow("name", 33)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("row 33 not found")
						}
						if val != "Name_33" {
							return fmt.Errorf("expected Name_33, got %s", val)
						}
						return nil
					},
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					if err := test.test(); err != nil {
						t.Error(err)
					}
				})
			}

			// Verify write protection
			err = cfRO.AddColumn("new", DataTypeInt64, false)
			if err == nil {
				t.Error("Expected error when adding column in read-only mode")
			}

			err = cfRO.LoadIntColumn("id", []IntData{{Value: 999, RowNum: 999}})
			if err == nil {
				t.Error("Expected error when loading data in read-only mode")
			}
		})
	}
}