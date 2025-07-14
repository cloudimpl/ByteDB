package columnar

// IntData represents a single integer value that can be null
type IntData struct {
	Value  int64  // The actual value (ignored if IsNull is true)
	RowNum uint64 // Row number for this value
	IsNull bool   // Whether this value is NULL
}

// StringData represents a single string value that can be null
type StringData struct {
	Value  string // The actual value (ignored if IsNull is true)
	RowNum uint64 // Row number for this value
	IsNull bool   // Whether this value is NULL
}

// Helper functions for creating non-null data
func NewIntData(value int64, rowNum uint64) IntData {
	return IntData{
		Value:  value,
		RowNum: rowNum,
		IsNull: false,
	}
}

func NewStringData(value string, rowNum uint64) StringData {
	return StringData{
		Value:  value,
		RowNum: rowNum,
		IsNull: false,
	}
}

// Helper functions for creating null data
func NewNullIntData(rowNum uint64) IntData {
	return IntData{
		Value:  0, // Doesn't matter
		RowNum: rowNum,
		IsNull: true,
	}
}

func NewNullStringData(rowNum uint64) StringData {
	return StringData{
		Value:  "", // Doesn't matter
		RowNum: rowNum,
		IsNull: true,
	}
}