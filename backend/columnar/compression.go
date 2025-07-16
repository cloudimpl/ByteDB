package columnar

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// CompressionType represents different compression algorithms
type CompressionType uint8

const (
	CompressionNone   CompressionType = 0
	CompressionGzip   CompressionType = 1
	CompressionSnappy CompressionType = 2 // Reserved for future
	CompressionZstd   CompressionType = 3 // Reserved for future
	CompressionLZ4    CompressionType = 4 // Reserved for future
)

// CompressionLevel represents compression level for algorithms that support it
type CompressionLevel int

const (
	CompressionLevelFastest CompressionLevel = 1
	CompressionLevelDefault CompressionLevel = 0
	CompressionLevelBetter  CompressionLevel = 3
	CompressionLevelBest    CompressionLevel = 9
)

// ColumnCompressionStrategy represents compression strategy for a column
type ColumnCompressionStrategy struct {
	EnableDeltaEncoding      bool             // For sequential numeric data
	EnableDictionaryEncoding bool             // For low-cardinality data
	DictionaryThreshold      int              // Max unique values for dictionary encoding (default: 1000)
	EnableBitPacking         bool             // For numeric data with limited range
	PageCompression          CompressionType  // Page-level compression
	CompressionLevel         CompressionLevel // Compression level (for zstd, gzip)
}

// CompressionOptions represents file-level compression settings
type CompressionOptions struct {
	// Page configuration
	PageSize int // Page size in bytes (default: 16KB)
	
	// Global settings
	DefaultPageCompression  CompressionType
	DefaultCompressionLevel CompressionLevel

	// Page type specific compression
	LeafPageCompression     CompressionType
	InternalPageCompression CompressionType
	StringPageCompression   CompressionType
	BitmapPageCompression   CompressionType

	// Column-specific strategies
	ColumnStrategies map[string]*ColumnCompressionStrategy

	// Global flags
	EnableAdaptiveCompression bool // Auto-detect best compression
	MinPageSizeToCompress     int  // Don't compress small pages (default: 512 bytes)
}

// NewCompressionOptions creates default compression options
func NewCompressionOptions() *CompressionOptions {
	return &CompressionOptions{
		PageSize:                  4096, // Currently only 4KB is supported
		DefaultPageCompression:    CompressionNone,
		DefaultCompressionLevel:   CompressionLevelDefault,
		LeafPageCompression:       CompressionNone,
		InternalPageCompression:   CompressionNone,
		StringPageCompression:     CompressionNone,
		BitmapPageCompression:     CompressionNone,
		ColumnStrategies:          make(map[string]*ColumnCompressionStrategy),
		EnableAdaptiveCompression: false,
		MinPageSizeToCompress:     512,
	}
}

// WithPageSize sets the page size for the file
// NOTE: Dynamic page sizes are not yet fully implemented. Currently, only 4KB (4096) is supported.
// This method stores the configuration for future compatibility when dynamic page sizes are implemented.
func (opts *CompressionOptions) WithPageSize(pageSize int) *CompressionOptions {
	// Validate page size (must be power of 2, between 1KB and 128KB)
	if pageSize < 1024 || pageSize > 128*1024 || (pageSize&(pageSize-1)) != 0 {
		panic(fmt.Sprintf("Invalid page size: %d. Must be power of 2 between 1KB and 128KB", pageSize))
	}
	opts.PageSize = pageSize
	return opts
}

// ValidatePageSize checks if the configured page size is supported
// Currently only 4KB pages are fully implemented
func (opts *CompressionOptions) ValidatePageSize() error {
	if opts.PageSize != 4096 {
		return fmt.Errorf("dynamic page size not yet supported: requested %d bytes, system currently only supports 4096 bytes", 
			opts.PageSize)
	}
	return nil
}

// WithPageCompression sets default page compression
func (opts *CompressionOptions) WithPageCompression(compressionType CompressionType, level CompressionLevel) *CompressionOptions {
	opts.DefaultPageCompression = compressionType
	opts.DefaultCompressionLevel = level
	opts.LeafPageCompression = compressionType
	opts.StringPageCompression = compressionType
	// Keep internal pages uncompressed for performance
	opts.InternalPageCompression = CompressionNone
	// Bitmaps are already compressed by Roaring
	opts.BitmapPageCompression = CompressionNone
	return opts
}

// WithColumnStrategy sets compression strategy for a specific column
func (opts *CompressionOptions) WithColumnStrategy(columnName string, strategy *ColumnCompressionStrategy) *CompressionOptions {
	opts.ColumnStrategies[columnName] = strategy
	return opts
}

// CompressionStats tracks compression statistics
type CompressionStats struct {
	UncompressedBytes int64
	CompressedBytes   int64
	CompressionRatio  float64
	PageCompressions  map[PageType]int64
	ColumnStats       map[string]*ColumnCompressionStats
}

// ColumnCompressionStats tracks per-column compression statistics
type ColumnCompressionStats struct {
	DictionarySize    int
	DeltaEncodedPages int
	BitPackedPages    int
	UncompressedBytes int64
	CompressedBytes   int64
	CompressionRatio  float64
}

// Compressor interface for different compression algorithms
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Type() CompressionType
}

// SnappyCompressor implements Snappy compression
type SnappyCompressor struct{}

func (s *SnappyCompressor) Compress(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func (s *SnappyCompressor) Type() CompressionType {
	return CompressionSnappy
}

// ZstdCompressor implements Zstandard compression
type ZstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstdCompressor(level CompressionLevel) (*ZstdCompressor, error) {
	// Map our levels to zstd levels
	zstdLevel := zstd.SpeedDefault
	switch level {
	case CompressionLevelFastest:
		zstdLevel = zstd.SpeedFastest
	case CompressionLevelBetter:
		zstdLevel = zstd.SpeedBetterCompression
	case CompressionLevelBest:
		zstdLevel = zstd.SpeedBestCompression
	}
	
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstdLevel))
	if err != nil {
		return nil, err
	}
	
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, err
	}
	
	return &ZstdCompressor{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, nil), nil
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}

func (z *ZstdCompressor) Type() CompressionType {
	return CompressionZstd
}

func (z *ZstdCompressor) Close() {
	if z.encoder != nil {
		z.encoder.Close()
	}
}

// GzipCompressor implements Gzip compression
type GzipCompressor struct {
	level int
}

func NewGzipCompressor(level CompressionLevel) *GzipCompressor {
	gzipLevel := gzip.DefaultCompression
	switch level {
	case CompressionLevelFastest:
		gzipLevel = gzip.BestSpeed
	case CompressionLevelBest:
		gzipLevel = gzip.BestCompression
	}

	return &GzipCompressor{level: gzipLevel}
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, g.level)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(data)
	if err != nil {
		writer.Close()
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (g *GzipCompressor) Type() CompressionType {
	return CompressionGzip
}

// CreateCompressor creates compressor instances
func CreateCompressor(compressionType CompressionType, level CompressionLevel) (Compressor, error) {
	switch compressionType {
	case CompressionNone:
		return nil, nil
	case CompressionSnappy:
		return &SnappyCompressor{}, nil
	case CompressionZstd:
		return NewZstdCompressor(level)
	case CompressionGzip:
		return NewGzipCompressor(level), nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// DeltaEncoder implements delta encoding for sequential numeric data
type DeltaEncoder struct {
	baseValue uint64
	deltas    []int64
}

func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{
		deltas: make([]int64, 0),
	}
}

func (de *DeltaEncoder) Encode(values []uint64) []byte {
	if len(values) == 0 {
		return nil
	}

	// Set base value
	de.baseValue = values[0]
	de.deltas = make([]int64, len(values)-1)

	// Calculate deltas
	for i := 1; i < len(values); i++ {
		de.deltas[i-1] = int64(values[i]) - int64(values[i-1])
	}

	// Serialize
	buf := new(bytes.Buffer)
	binary.Write(buf, ByteOrder, de.baseValue)
	binary.Write(buf, ByteOrder, uint32(len(values)))

	// Find min/max delta for bit packing
	if len(de.deltas) > 0 {
		minDelta, maxDelta := de.deltas[0], de.deltas[0]
		for _, d := range de.deltas {
			if d < minDelta {
				minDelta = d
			}
			if d > maxDelta {
				maxDelta = d
			}
		}

		// For now, use simple encoding (can be optimized with bit packing)
		binary.Write(buf, ByteOrder, minDelta)
		binary.Write(buf, ByteOrder, maxDelta)

		for _, delta := range de.deltas {
			binary.Write(buf, ByteOrder, delta)
		}
	}

	return buf.Bytes()
}

func (de *DeltaEncoder) Decode(data []byte) ([]uint64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)

	// Read base value and count
	var baseValue uint64
	var count uint32
	if err := binary.Read(buf, ByteOrder, &baseValue); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, ByteOrder, &count); err != nil {
		return nil, err
	}

	values := make([]uint64, count)
	values[0] = baseValue

	if count > 1 {
		// Read min/max (for future bit packing)
		var minDelta, maxDelta int64
		binary.Read(buf, ByteOrder, &minDelta)
		binary.Read(buf, ByteOrder, &maxDelta)

		// Read deltas
		for i := 1; i < int(count); i++ {
			var delta int64
			if err := binary.Read(buf, ByteOrder, &delta); err != nil {
				return nil, err
			}
			values[i] = uint64(int64(values[i-1]) + delta)
		}
	}

	return values, nil
}

// DictionaryEncoder implements dictionary encoding for low-cardinality data
type DictionaryEncoder struct {
	dictionary map[uint64]uint32 // value -> dictionary index
	values     []uint64          // dictionary values
}

func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{
		dictionary: make(map[uint64]uint32),
		values:     make([]uint64, 0),
	}
}

func (de *DictionaryEncoder) CanUseDictionary(uniqueValues int, totalValues int) bool {
	// Use dictionary if cardinality is low
	if uniqueValues > 65536 { // Max uint16
		return false
	}

	// Use if compression ratio would be good
	dictSize := uniqueValues * 8 // 8 bytes per value
	indexSize := totalValues * 2 // 2 bytes per index (uint16)
	originalSize := totalValues * 8

	return dictSize+indexSize < originalSize
}

func (de *DictionaryEncoder) BuildDictionary(values []uint64) {
	de.dictionary = make(map[uint64]uint32)
	de.values = make([]uint64, 0)

	for _, v := range values {
		if _, exists := de.dictionary[v]; !exists {
			de.dictionary[v] = uint32(len(de.values))
			de.values = append(de.values, v)
		}
	}
}

func (de *DictionaryEncoder) Encode(values []uint64) []byte {
	buf := new(bytes.Buffer)

	// Write dictionary size
	binary.Write(buf, ByteOrder, uint32(len(de.values)))

	// Write dictionary values
	for _, v := range de.values {
		binary.Write(buf, ByteOrder, v)
	}

	// Write value count
	binary.Write(buf, ByteOrder, uint32(len(values)))

	// Write indices (using smallest possible size)
	if len(de.values) <= 256 {
		// Use uint8
		buf.WriteByte(1) // Index size
		for _, v := range values {
			idx := de.dictionary[v]
			buf.WriteByte(uint8(idx))
		}
	} else if len(de.values) <= 65536 {
		// Use uint16
		buf.WriteByte(2) // Index size
		for _, v := range values {
			idx := de.dictionary[v]
			binary.Write(buf, ByteOrder, uint16(idx))
		}
	} else {
		// Use uint32
		buf.WriteByte(4) // Index size
		for _, v := range values {
			idx := de.dictionary[v]
			binary.Write(buf, ByteOrder, idx)
		}
	}

	return buf.Bytes()
}

func (de *DictionaryEncoder) Decode(data []byte) ([]uint64, error) {
	buf := bytes.NewReader(data)

	// Read dictionary size
	var dictSize uint32
	if err := binary.Read(buf, ByteOrder, &dictSize); err != nil {
		return nil, err
	}

	// Read dictionary values
	dictionary := make([]uint64, dictSize)
	for i := uint32(0); i < dictSize; i++ {
		if err := binary.Read(buf, ByteOrder, &dictionary[i]); err != nil {
			return nil, err
		}
	}

	// Read value count
	var valueCount uint32
	if err := binary.Read(buf, ByteOrder, &valueCount); err != nil {
		return nil, err
	}

	// Read index size
	indexSize, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read indices and decode values
	values := make([]uint64, valueCount)

	switch indexSize {
	case 1:
		for i := uint32(0); i < valueCount; i++ {
			idx, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			values[i] = dictionary[idx]
		}
	case 2:
		for i := uint32(0); i < valueCount; i++ {
			var idx uint16
			if err := binary.Read(buf, ByteOrder, &idx); err != nil {
				return nil, err
			}
			values[i] = dictionary[idx]
		}
	case 4:
		for i := uint32(0); i < valueCount; i++ {
			var idx uint32
			if err := binary.Read(buf, ByteOrder, &idx); err != nil {
				return nil, err
			}
			values[i] = dictionary[idx]
		}
	default:
		return nil, fmt.Errorf("invalid index size: %d", indexSize)
	}

	return values, nil
}

// String compression helpers
func CompressStringPrefix(sortedStrings []string) []byte {
	if len(sortedStrings) == 0 {
		return nil
	}

	buf := new(bytes.Buffer)
	binary.Write(buf, ByteOrder, uint32(len(sortedStrings)))

	prevString := ""
	for _, str := range sortedStrings {
		// Find common prefix length
		prefixLen := 0
		minLen := len(prevString)
		if len(str) < minLen {
			minLen = len(str)
		}

		for i := 0; i < minLen; i++ {
			if prevString[i] == str[i] {
				prefixLen++
			} else {
				break
			}
		}

		// Write prefix length and suffix
		binary.Write(buf, ByteOrder, uint16(prefixLen))
		suffix := str[prefixLen:]
		binary.Write(buf, ByteOrder, uint32(len(suffix)))
		buf.WriteString(suffix)

		prevString = str
	}

	return buf.Bytes()
}

func DecompressStringPrefix(data []byte) ([]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(buf, ByteOrder, &count); err != nil {
		return nil, err
	}

	strings := make([]string, count)
	prevString := ""

	for i := uint32(0); i < count; i++ {
		var prefixLen uint16
		var suffixLen uint32

		if err := binary.Read(buf, ByteOrder, &prefixLen); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, ByteOrder, &suffixLen); err != nil {
			return nil, err
		}

		suffix := make([]byte, suffixLen)
		if _, err := buf.Read(suffix); err != nil {
			return nil, err
		}

		// Reconstruct string
		if int(prefixLen) <= len(prevString) {
			strings[i] = prevString[:prefixLen] + string(suffix)
		} else {
			strings[i] = string(suffix)
		}

		prevString = strings[i]
	}

	return strings, nil
}
