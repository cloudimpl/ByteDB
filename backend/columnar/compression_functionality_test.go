package columnar

import (
	"fmt"
	"os"
	"testing"
	
	"github.com/RoaringBitmap/roaring/v2"
)

func TestCompressionFunctionality(t *testing.T) {
	compressionTypes := []struct {
		name  string
		cType CompressionType
	}{
		{"NoCompression", CompressionNone},
		{"Snappy", CompressionSnappy},
		{"Gzip", CompressionGzip},
		{"Zstd", CompressionZstd},
	}
	
	for _, ct := range compressionTypes {
		t.Run(ct.name, func(t *testing.T) {
			filename := fmt.Sprintf("test_functionality_%s.bytedb", ct.name)
			defer os.Remove(filename)
			
			// Create file with compression
			opts := NewCompressionOptions().
				WithPageCompression(ct.cType, CompressionLevelDefault)
			
			cf, err := CreateFileWithOptions(filename, opts)
			if err != nil {
				t.Fatal(err)
			}
			
			// Add columns
			cf.AddColumn("id", DataTypeInt64, false)
			cf.AddColumn("name", DataTypeString, true)
			cf.AddColumn("category", DataTypeString, false)
			
			// Test data
			numRows := 1000
			intData := make([]IntData, numRows)
			nameData := make([]StringData, numRows)
			categoryData := make([]StringData, numRows)
			
			categories := []string{"A", "B", "C", "D", "E"}
			
			for i := 0; i < numRows; i++ {
				intData[i] = IntData{
					Value:  int64(i),
					RowNum: uint64(i),
				}
				
				// Some nulls in name
				if i%10 == 0 {
					nameData[i] = StringData{
						IsNull: true,
						RowNum: uint64(i),
					}
				} else {
					nameData[i] = StringData{
						Value:  fmt.Sprintf("Name_%d", i),
						RowNum: uint64(i),
					}
				}
				
				categoryData[i] = StringData{
					Value:  categories[i%len(categories)],
					RowNum: uint64(i),
				}
			}
			
			// Load data
			if err := cf.LoadIntColumn("id", intData); err != nil {
				t.Fatal(err)
			}
			if err := cf.LoadStringColumn("name", nameData); err != nil {
				t.Fatal(err)
			}
			if err := cf.LoadStringColumn("category", categoryData); err != nil {
				t.Fatal(err)
			}
			
			// Get compression stats
			stats := cf.pageManager.GetCompressionStats()
			
			// Close and reopen
			if err := cf.Close(); err != nil {
				t.Fatal(err)
			}
			
			cf, err = OpenFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer cf.Close()
			
			t.Logf("%s - Compression ratio: %.2f", ct.name, stats.CompressionRatio)
			
			// Test 1: Row→Key lookups (bidirectional)
			t.Run("RowKeyLookups", func(t *testing.T) {
				testRows := []uint64{0, 100, 500, 999}
				
				for _, row := range testRows {
					// Integer lookup
					val, found, err := cf.LookupIntByRow("id", row)
					if err != nil {
						t.Errorf("Row %d: error %v", row, err)
						continue
					}
					if !found {
						t.Errorf("Row %d: not found", row)
						continue
					}
					if val != int64(row) {
						t.Errorf("Row %d: expected %d, got %d", row, row, val)
					}
					
					// String lookup (name - may be null)
					strVal, found, err := cf.LookupStringByRow("name", row)
					if err != nil {
						t.Errorf("Row %d: name error %v", row, err)
						continue
					}
					
					if row%10 == 0 {
						// Should be null
						if found {
							t.Errorf("Row %d: expected null, got %s", row, strVal)
						}
					} else {
						if !found {
							t.Errorf("Row %d: name not found", row)
						} else if strVal != fmt.Sprintf("Name_%d", row) {
							t.Errorf("Row %d: name mismatch", row)
						}
					}
					
					// Category lookup
					catVal, found, err := cf.LookupStringByRow("category", row)
					if err != nil {
						t.Errorf("Row %d: category error %v", row, err)
						continue
					}
					if !found {
						t.Errorf("Row %d: category not found", row)
						continue
					}
					expectedCat := categories[row%uint64(len(categories))]
					if catVal != expectedCat {
						t.Errorf("Row %d: expected category %s, got %s", row, expectedCat, catVal)
					}
				}
			})
			
			// Test 2: Key→Row lookups (forward)
			t.Run("KeyRowLookups", func(t *testing.T) {
				// Integer exact match
				bitmap, err := cf.QueryInt("id", 500)
				if err != nil {
					t.Fatal(err)
				}
				rows := BitmapToSlice(bitmap)
				if len(rows) != 1 || rows[0] != 500 {
					t.Errorf("Expected row 500, got %v", rows)
				}
				
				// String exact match
				bitmap, err = cf.QueryString("name", "Name_123")
				if err != nil {
					t.Fatal(err)
				}
				rows = BitmapToSlice(bitmap)
				if len(rows) != 1 || rows[0] != 123 {
					t.Errorf("Expected row 123, got %v", rows)
				}
				
				// Category query (multiple matches)
				bitmap, err = cf.QueryString("category", "A")
				if err != nil {
					t.Fatal(err)
				}
				rows = BitmapToSlice(bitmap)
				expectedCount := numRows / len(categories)
				if len(rows) != expectedCount {
					t.Errorf("Expected %d rows for category A, got %d", expectedCount, len(rows))
				}
			})
			
			// Test 3: Range queries
			t.Run("RangeQueries", func(t *testing.T) {
				// Integer range
				bitmap, err := cf.RangeQueryInt("id", 100, 200)
				if err != nil {
					t.Fatal(err)
				}
				rows := BitmapToSlice(bitmap)
				if len(rows) != 101 {
					t.Errorf("Expected 101 rows, got %d", len(rows))
				}
				
				// String range
				bitmap, err = cf.RangeQueryString("name", "Name_100", "Name_110")
				if err != nil {
					t.Fatal(err)
				}
				rows = BitmapToSlice(bitmap)
				// Should include Name_100 through Name_110 (excluding nulls at 100 and 110)
				expectedRows := 9 // 101-109 (100 and 110 are null)
				if len(rows) != expectedRows {
					t.Errorf("Expected %d rows, got %d", expectedRows, len(rows))
				}
			})
			
			// Test 4: Null queries
			t.Run("NullQueries", func(t *testing.T) {
				// Query for nulls
				bitmap, err := cf.QueryNull("name")
				if err != nil {
					t.Fatal(err)
				}
				rows := BitmapToSlice(bitmap)
				expectedNulls := numRows / 10 // Every 10th row is null
				if len(rows) != expectedNulls {
					t.Errorf("Expected %d nulls, got %d", expectedNulls, len(rows))
				}
				
				// Query for not nulls
				bitmap, err = cf.QueryNotNull("name")
				if err != nil {
					t.Fatal(err)
				}
				rows = BitmapToSlice(bitmap)
				expectedNotNulls := numRows - expectedNulls
				if len(rows) != expectedNotNulls {
					t.Errorf("Expected %d not nulls, got %d", expectedNotNulls, len(rows))
				}
			})
			
			// Test 5: Complex queries
			t.Run("ComplexQueries", func(t *testing.T) {
				// AND query: id >= 500 AND category = 'A'
				bitmap1, _ := cf.QueryGreaterThanOrEqual("id", 500)
				bitmap2, _ := cf.QueryString("category", "A")
				result := cf.QueryAnd(bitmap1, bitmap2)
				
				rows := BitmapToSlice(result)
				// Should have rows 500, 505, 510, ... where category is A
				for _, row := range rows {
					if row < 500 {
						t.Errorf("Found row %d < 500", row)
					}
					if row%5 != 0 {
						t.Errorf("Row %d should have category A (row%%5==0)", row)
					}
				}
				
				// OR query: name IS NULL OR category = 'B'
				bitmap1, _ = cf.QueryNull("name")
				bitmap2, _ = cf.QueryString("category", "B")
				result = cf.QueryOr(bitmap1, bitmap2)
				
				rows = BitmapToSlice(result)
				for _, row := range rows {
					if row%10 != 0 && row%5 != 1 {
						t.Errorf("Row %d should be null (%%10==0) or category B (%%5==1)", row)
					}
				}
			})
		})
	}
}

func TestCompressionConsistency(t *testing.T) {
	// Test that compressed and uncompressed files return identical results
	
	numRows := 5000
	
	// Generate test data
	intData := make([]IntData, numRows)
	stringData := make([]StringData, numRows)
	
	for i := 0; i < numRows; i++ {
		intData[i] = IntData{
			Value:  int64(i * i), // Non-sequential for interesting B-tree
			RowNum: uint64(i),
		}
		stringData[i] = StringData{
			Value:  fmt.Sprintf("String_%05d_%s", i, string(rune('A'+i%26))),
			RowNum: uint64(i),
		}
	}
	
	// Create uncompressed file
	file1 := "test_consistency_uncompressed.bytedb"
	defer os.Remove(file1)
	
	cf1, _ := CreateFile(file1)
	cf1.AddColumn("value", DataTypeInt64, false)
	cf1.AddColumn("name", DataTypeString, false)
	cf1.LoadIntColumn("value", intData)
	cf1.LoadStringColumn("name", stringData)
	cf1.Close()
	
	// Create compressed file
	file2 := "test_consistency_compressed.bytedb"
	defer os.Remove(file2)
	
	opts := NewCompressionOptions().
		WithPageCompression(CompressionZstd, CompressionLevelDefault)
	
	cf2, _ := CreateFileWithOptions(file2, opts)
	cf2.AddColumn("value", DataTypeInt64, false)
	cf2.AddColumn("name", DataTypeString, false)
	cf2.LoadIntColumn("value", intData)
	cf2.LoadStringColumn("name", stringData)
	cf2.Close()
	
	// Reopen both
	cf1, _ = OpenFile(file1)
	defer cf1.Close()
	
	cf2, _ = OpenFile(file2)
	defer cf2.Close()
	
	// Test various queries return same results
	tests := []struct {
		name  string
		query func(*ColumnarFile) (*roaring.Bitmap, error)
	}{
		{
			"ExactMatch",
			func(cf *ColumnarFile) (*roaring.Bitmap, error) {
				return cf.QueryInt("value", 2500)
			},
		},
		{
			"RangeQuery",
			func(cf *ColumnarFile) (*roaring.Bitmap, error) {
				return cf.RangeQueryInt("value", 1000, 5000)
			},
		},
		{
			"GreaterThan",
			func(cf *ColumnarFile) (*roaring.Bitmap, error) {
				return cf.QueryGreaterThan("value", 10000)
			},
		},
		{
			"StringPrefix",
			func(cf *ColumnarFile) (*roaring.Bitmap, error) {
				return cf.RangeQueryString("name", "String_01", "String_02")
			},
		},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result1, err1 := test.query(cf1)
			result2, err2 := test.query(cf2)
			
			if err1 != nil || err2 != nil {
				t.Fatalf("Query errors: %v, %v", err1, err2)
			}
			
			rows1 := BitmapToSlice(result1)
			rows2 := BitmapToSlice(result2)
			
			if len(rows1) != len(rows2) {
				t.Errorf("Different result counts: %d vs %d", len(rows1), len(rows2))
			}
			
			// Compare actual rows
			for i := 0; i < len(rows1) && i < len(rows2); i++ {
				if rows1[i] != rows2[i] {
					t.Errorf("Row mismatch at %d: %d vs %d", i, rows1[i], rows2[i])
				}
			}
		})
	}
	
	// Test row lookups
	t.Run("RowLookups", func(t *testing.T) {
		testRows := []uint64{0, 1000, 2500, 4999}
		
		for _, row := range testRows {
			val1, found1, err1 := cf1.LookupIntByRow("value", row)
			val2, found2, err2 := cf2.LookupIntByRow("value", row)
			
			if err1 != nil || err2 != nil {
				t.Errorf("Row %d lookup errors: %v, %v", row, err1, err2)
				continue
			}
			
			if found1 != found2 {
				t.Errorf("Row %d: found mismatch %v vs %v", row, found1, found2)
			}
			
			if val1 != val2 {
				t.Errorf("Row %d: value mismatch %d vs %d", row, val1, val2)
			}
			
			// String lookups
			str1, found1, err1 := cf1.LookupStringByRow("name", row)
			str2, found2, err2 := cf2.LookupStringByRow("name", row)
			
			if err1 != nil || err2 != nil {
				t.Errorf("Row %d string lookup errors: %v, %v", row, err1, err2)
				continue
			}
			
			if found1 != found2 || str1 != str2 {
				t.Errorf("Row %d: string mismatch %s vs %s", row, str1, str2)
			}
		}
	})
}