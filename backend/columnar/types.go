package columnar

import (
	"encoding/binary"
	"errors"
)

// Constants
const (
	MagicNumber    = 0x42594442 // "BYDB"
	MajorVersion   = 1
	MinorVersion   = 0
	
	// Page header size
	PageHeaderSize = 64
	
	// Value flags
	ValueFlagBitmap    = uint64(1) << 63
	ValueMask          = ^ValueFlagBitmap
)

// CalculateMaxInternalEntries calculates max entries for internal pages based on page size
func CalculateMaxInternalEntries(pageSize int) int {
	return (pageSize - PageHeaderSize - 8) / 40
}

// CalculateMaxLeafEntries calculates max entries for leaf pages based on page size
func CalculateMaxLeafEntries(pageSize int) int {
	return (pageSize - PageHeaderSize - 16) / 16
}

// CalculateMaxStringsPerPage calculates max strings per page based on page size
func CalculateMaxStringsPerPage(pageSize int) int {
	return (pageSize - PageHeaderSize - 16) / 16
}

// Errors
var (
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("unsupported file version")
	ErrPageNotFound       = errors.New("page not found")
	ErrStringNotFound     = errors.New("string not found")
	ErrColumnNotFound     = errors.New("column not found")
	ErrInvalidPageType    = errors.New("invalid page type")
	ErrPageFull           = errors.New("page is full")
	ErrInvalidOffset      = errors.New("invalid offset")
)

// PageType represents different types of pages in the file
type PageType uint8

const (
	PageTypeFileHeader    PageType = 0x01
	PageTypeBTreeInternal PageType = 0x02
	PageTypeBTreeLeaf     PageType = 0x03
	PageTypeRoaringBitmap PageType = 0x04
	PageTypeStringSegment PageType = 0x05
	PageTypeBinarySegment PageType = 0x06
	PageTypePageDirectory PageType = 0x07
)

// NullSortOrder defines how NULL values are sorted in queries
type NullSortOrder int

const (
	NullsFirst NullSortOrder = iota // NULL values come before non-NULL values
	NullsLast                        // NULL values come after non-NULL values
)

// DataType represents the data type of a column
type DataType uint8

const (
	DataTypeInt8    DataType = iota // byte
	DataTypeInt16                   // short
	DataTypeInt32
	DataTypeInt64
	DataTypeUint8   // unsigned byte
	DataTypeUint16  // unsigned short
	DataTypeUint32
	DataTypeUint64
	DataTypeFloat32
	DataTypeFloat64
	DataTypeBool
	DataTypeString
	DataTypeBinary
)

// ByteOrder is the byte order used for encoding
var ByteOrder = binary.LittleEndian

// GetDataTypeSize returns the size in bytes of a data type
func GetDataTypeSize(dt DataType) int {
	switch dt {
	case DataTypeBool, DataTypeInt8, DataTypeUint8:
		return 1
	case DataTypeInt16, DataTypeUint16:
		return 2
	case DataTypeInt32, DataTypeUint32, DataTypeFloat32:
		return 4
	case DataTypeInt64, DataTypeUint64, DataTypeFloat64:
		return 8
	case DataTypeString, DataTypeBinary:
		return 8 // These use offset pointers
	default:
		return 8
	}
}


// FileHeader represents the file header structure
type FileHeader struct {
	MagicNumber          uint32
	MajorVersion         uint16
	MinorVersion         uint16
	FileSize             uint64
	PageSize             uint32
	RowCount             uint64
	ColumnCount          uint32
	MetadataOffset       uint64
	PageDirectoryOffset  uint64
	ValueStorageOffset   uint64
	CreationTimestamp    uint64
	TotalPages           uint64
	CompressionType      uint32 // Deprecated - use CompressionMetadata instead
	HeaderChecksum       uint64
	// New fields for compression metadata
	DefaultCompression   uint8
	LeafCompression      uint8
	InternalCompression  uint8
	StringCompression    uint8
	BitmapCompression    uint8
	CompressionLevel     uint8
	CompressionFlags     uint16 // For future flags like EnableAdaptiveCompression
	DeletedBitmapOffset  uint64 // Offset to the deleted rows bitmap
}

// ColumnMetadata represents metadata for a single column
type ColumnMetadata struct {
	ColumnName         [64]byte
	DataType           DataType
	IsNullable         bool
	Flags              uint16
	RootPageID         uint64
	TreeHeight         uint32
	TotalKeys          uint64
	NullCount          uint64
	DistinctCount      uint64
	MinValueOffset     uint64
	MaxValueOffset     uint64
	TotalSizeBytes     uint64
	BitmapPagesCount   uint64
	AverageKeySize     uint8
	StringSegmentStart uint64 // For string/binary columns
	NullBitmapPageID   uint64 // Page ID for null bitmap (0 if no nulls)
	MetadataPageID     uint64 // Page ID where this metadata is stored
	RowKeyRootPageID   uint64 // Root page ID for row→key B+ tree index
	RowKeyTreeHeight   uint32 // Height of row→key B+ tree
}

// ColumnStats represents statistics calculated during B+ tree construction
type ColumnStats struct {
	TotalKeys     uint64
	DistinctCount uint64
	NullCount     uint64
	MinValue      uint64
	MaxValue      uint64
	AverageKeySize uint8
}

// PageHeader represents the standard page header
type PageHeader struct {
	PageType       PageType
	CompressionType uint8
	Flags          uint16
	DataSize       uint32
	PageID         uint64
	ParentPageID   uint64
	NextPageID     uint64
	PrevPageID     uint64
	EntryCount     uint32
	FreeSpace      uint32
	Checksum       uint64
	Reserved       [16]byte
}

// Value represents a value that can be either a row number or bitmap offset
type Value struct {
	Data uint64
}

// IsRowNumber returns true if this value represents a single row number
func (v Value) IsRowNumber() bool {
	return (v.Data & ValueFlagBitmap) == 0
}

// IsBitmap returns true if this value represents a bitmap offset
func (v Value) IsBitmap() bool {
	return (v.Data & ValueFlagBitmap) != 0
}

// GetRowNumber returns the row number (only valid if IsRowNumber() is true)
func (v Value) GetRowNumber() uint64 {
	return v.Data & ValueMask
}

// GetBitmapOffset returns the bitmap page offset (only valid if IsBitmap() is true)
func (v Value) GetBitmapOffset() uint64 {
	return v.Data & ValueMask
}

// NewRowNumberValue creates a Value representing a single row number
func NewRowNumberValue(rowNum uint64) Value {
	return Value{Data: rowNum & ValueMask}
}

// NewBitmapValue creates a Value representing a bitmap offset
func NewBitmapValue(bitmapOffset uint64) Value {
	return Value{Data: (bitmapOffset & ValueMask) | ValueFlagBitmap}
}

// StringEntry represents an entry in the string segment directory
type StringEntry struct {
	Offset uint64
	Length uint32
	Hash   uint32
}

// BinaryEntry represents an entry in the binary segment directory
type BinaryEntry struct {
	Offset uint64
	Length uint32
	Hash   uint32
	Prefix [8]byte
}