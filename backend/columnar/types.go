package columnar

import (
	"encoding/binary"
	"errors"
)

// Constants
const (
	PageSize       = 4096 // 4KB pages
	MagicNumber    = 0x42594442 // "BYDB"
	MajorVersion   = 1
	MinorVersion   = 0
	
	// Page header size
	PageHeaderSize = 64
	
	// Maximum entries per page type
	MaxInternalEntries = (PageSize - PageHeaderSize - 8) / 40  // ~100 entries
	MaxLeafEntries     = (PageSize - PageHeaderSize - 16) / 16  // ~250 entries
	MaxStringsPerPage  = (PageSize - PageHeaderSize - 16) / 16  // ~250 strings
	
	// Value flags
	ValueFlagBitmap    = uint64(1) << 63
	ValueMask          = ^ValueFlagBitmap
)

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

// EncodeKey encodes a key value based on its data type into a byte slice
func EncodeKey(key uint64, dt DataType) []byte {
	size := GetDataTypeSize(dt)
	buf := make([]byte, size)
	
	switch dt {
	case DataTypeBool, DataTypeInt8:
		buf[0] = byte(key)
	case DataTypeUint8:
		buf[0] = uint8(key)
	case DataTypeInt16:
		ByteOrder.PutUint16(buf, uint16(int16(key)))
	case DataTypeUint16:
		ByteOrder.PutUint16(buf, uint16(key))
	case DataTypeInt32:
		ByteOrder.PutUint32(buf, uint32(int32(key)))
	case DataTypeUint32, DataTypeFloat32:
		ByteOrder.PutUint32(buf, uint32(key))
	case DataTypeInt64:
		ByteOrder.PutUint64(buf, key)
	case DataTypeUint64, DataTypeFloat64, DataTypeString, DataTypeBinary:
		ByteOrder.PutUint64(buf, key)
	}
	
	return buf
}

// DecodeKey decodes a key from bytes based on its data type
func DecodeKey(buf []byte, dt DataType) uint64 {
	switch dt {
	case DataTypeBool, DataTypeInt8:
		return uint64(int8(buf[0]))
	case DataTypeUint8:
		return uint64(buf[0])
	case DataTypeInt16:
		return uint64(int16(ByteOrder.Uint16(buf)))
	case DataTypeUint16:
		return uint64(ByteOrder.Uint16(buf))
	case DataTypeInt32:
		return uint64(int32(ByteOrder.Uint32(buf)))
	case DataTypeUint32, DataTypeFloat32:
		return uint64(ByteOrder.Uint32(buf))
	case DataTypeInt64:
		return ByteOrder.Uint64(buf)
	case DataTypeUint64, DataTypeFloat64, DataTypeString, DataTypeBinary:
		return ByteOrder.Uint64(buf)
	default:
		return 0
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
	CompressionType      uint32
	HeaderChecksum       uint64
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
	MetadataPageID     uint64 // Page ID where this metadata is stored
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