package columnar

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Page represents a single 4KB page
type Page struct {
	Header PageHeader
	Data   [PageSize - PageHeaderSize]byte
}

// NewPage creates a new page with the given type
func NewPage(pageType PageType, pageID uint64) *Page {
	return &Page{
		Header: PageHeader{
			PageType:  pageType,
			PageID:    pageID,
			FreeSpace: PageSize - PageHeaderSize,
		},
	}
}

// CalculateChecksum computes the CRC32 checksum of the page
func (p *Page) CalculateChecksum() uint32 {
	// Temporarily zero out the checksum field
	oldChecksum := p.Header.Checksum
	p.Header.Checksum = 0
	
	// Calculate checksum over entire page
	checksum := crc32.ChecksumIEEE(p.Marshal())
	
	// Restore checksum field
	p.Header.Checksum = uint64(oldChecksum)
	return checksum
}

// Marshal serializes the page to bytes
func (p *Page) Marshal() []byte {
	buf := make([]byte, PageSize)
	
	// Marshal header
	ByteOrder.PutUint16(buf[0:2], uint16(p.Header.PageType))
	buf[2] = p.Header.CompressionType
	ByteOrder.PutUint16(buf[3:5], p.Header.Flags)
	ByteOrder.PutUint32(buf[5:9], p.Header.DataSize)
	ByteOrder.PutUint64(buf[9:17], p.Header.PageID)
	ByteOrder.PutUint64(buf[17:25], p.Header.ParentPageID)
	ByteOrder.PutUint64(buf[25:33], p.Header.NextPageID)
	ByteOrder.PutUint64(buf[33:41], p.Header.PrevPageID)
	ByteOrder.PutUint32(buf[41:45], p.Header.EntryCount)
	ByteOrder.PutUint32(buf[45:49], p.Header.FreeSpace)
	ByteOrder.PutUint64(buf[49:57], p.Header.Checksum)
	copy(buf[57:PageHeaderSize], p.Header.Reserved[:])
	
	// Copy data
	copy(buf[PageHeaderSize:], p.Data[:])
	
	return buf
}

// Unmarshal deserializes the page from bytes
func (p *Page) Unmarshal(buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(buf))
	}
	
	// Unmarshal header
	p.Header.PageType = PageType(ByteOrder.Uint16(buf[0:2]))
	p.Header.CompressionType = buf[2]
	p.Header.Flags = ByteOrder.Uint16(buf[3:5])
	p.Header.DataSize = ByteOrder.Uint32(buf[5:9])
	p.Header.PageID = ByteOrder.Uint64(buf[9:17])
	p.Header.ParentPageID = ByteOrder.Uint64(buf[17:25])
	p.Header.NextPageID = ByteOrder.Uint64(buf[25:33])
	p.Header.PrevPageID = ByteOrder.Uint64(buf[33:41])
	p.Header.EntryCount = ByteOrder.Uint32(buf[41:45])
	p.Header.FreeSpace = ByteOrder.Uint32(buf[45:49])
	p.Header.Checksum = ByteOrder.Uint64(buf[49:57])
	copy(p.Header.Reserved[:], buf[57:PageHeaderSize])
	
	// Copy data
	copy(p.Data[:], buf[PageHeaderSize:])
	
	return nil
}

// PageManager manages pages in memory and on disk
type PageManager struct {
	file               *os.File
	cache              map[uint64]*Page
	cacheLock          sync.RWMutex
	nextPageID         uint64
	isDirty            map[uint64]bool
	compressionOptions *CompressionOptions
	compressors        map[CompressionType]Compressor
	compressionStats   *CompressionStats
}

// NewPageManager creates a new page manager
func NewPageManager(filename string, create bool) (*PageManager, error) {
	return NewPageManagerWithOptions(filename, create, nil)
}

// NewPageManagerReadOnly creates a new page manager in read-only mode
func NewPageManagerReadOnly(filename string) (*PageManager, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	
	// Use default options for read-only
	options := NewCompressionOptions()
	
	pm := &PageManager{
		file:               file,
		cache:              make(map[uint64]*Page),
		isDirty:            make(map[uint64]bool),
		compressionOptions: options,
		compressors:        make(map[CompressionType]Compressor),
		compressionStats: &CompressionStats{
			PageCompressions: make(map[PageType]int64),
			ColumnStats:      make(map[string]*ColumnCompressionStats),
		},
	}
	
	// Initialize compressors based on options
	if err := pm.initializeCompressors(); err != nil {
		file.Close()
		return nil, err
	}
	
	// Calculate next page ID based on file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	pm.nextPageID = uint64(info.Size() / PageSize)
	
	// Read and validate file header
	header, err := pm.ReadPage(0)
	if err != nil {
		file.Close()
		return nil, err
	}
	
	// Validate header
	if header.Header.PageType != PageTypeFileHeader {
		file.Close()
		return nil, ErrInvalidPageType
	}
	
	return pm, nil
}

// NewPageManagerWithOptions creates a new page manager with compression options
func NewPageManagerWithOptions(filename string, create bool, options *CompressionOptions) (*PageManager, error) {
	var file *os.File
	var err error
	
	if create {
		file, err = os.Create(filename)
	} else {
		file, err = os.OpenFile(filename, os.O_RDWR, 0644)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Use default options if none provided
	if options == nil {
		options = NewCompressionOptions()
	}
	
	pm := &PageManager{
		file:               file,
		cache:              make(map[uint64]*Page),
		isDirty:            make(map[uint64]bool),
		compressionOptions: options,
		compressors:        make(map[CompressionType]Compressor),
		compressionStats: &CompressionStats{
			PageCompressions: make(map[PageType]int64),
			ColumnStats:      make(map[string]*ColumnCompressionStats),
		},
	}
	
	// Initialize compressors based on options
	if err := pm.initializeCompressors(); err != nil {
		file.Close()
		return nil, err
	}
	
	if create {
		// Initialize with file header
		header := NewPage(PageTypeFileHeader, 0)
		if err := pm.WritePage(header); err != nil {
			file.Close()
			return nil, err
		}
		pm.nextPageID = 1
	} else {
		// Calculate next page ID based on file size first
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		pm.nextPageID = uint64(info.Size() / PageSize)
		
		// Now read file header to validate
		header, err := pm.ReadPage(0)
		if err != nil {
			file.Close()
			return nil, err
		}
		
		// Validate header
		if header.Header.PageType != PageTypeFileHeader {
			file.Close()
			return nil, ErrInvalidPageType
		}
	}
	
	return pm, nil
}

// AllocatePage allocates a new page
func (pm *PageManager) AllocatePage(pageType PageType) *Page {
	pm.cacheLock.Lock()
	defer pm.cacheLock.Unlock()
	
	pageID := pm.nextPageID
	pm.nextPageID++
	
	page := NewPage(pageType, pageID)
	pm.cache[pageID] = page
	pm.isDirty[pageID] = true
	
	return page
}

// ReadPage reads a page from disk or cache
func (pm *PageManager) ReadPage(pageID uint64) (*Page, error) {
	pm.cacheLock.RLock()
	if page, ok := pm.cache[pageID]; ok {
		pm.cacheLock.RUnlock()
		return page, nil
	}
	pm.cacheLock.RUnlock()
	
	// Check if page ID is beyond our allocated pages
	if pageID >= pm.nextPageID {
		return nil, fmt.Errorf("page %d does not exist (max page ID: %d)", pageID, pm.nextPageID-1)
	}
	
	// Read from disk
	offset := int64(pageID * PageSize)
	buf := make([]byte, PageSize)
	
	n, err := pm.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != PageSize {
		// If we can't read a full page, it might be because the file is truncated
		// or we're trying to read beyond the file size
		if n == 0 {
			return nil, fmt.Errorf("page %d is beyond file size", pageID)
		}
		return nil, fmt.Errorf("incomplete page read: expected %d, got %d", PageSize, n)
	}
	
	page := &Page{}
	if err := page.Unmarshal(buf); err != nil {
		return nil, err
	}
	
	// Verify checksum
	calculatedChecksum := page.CalculateChecksum()
	if uint64(calculatedChecksum) != page.Header.Checksum {
		return nil, fmt.Errorf("checksum mismatch for page %d", pageID)
	}
	
	// Decompress if needed
	if err := pm.decompressPage(page); err != nil {
		return nil, fmt.Errorf("failed to decompress page %d: %w", pageID, err)
	}
	
	// Cache the page
	pm.cacheLock.Lock()
	pm.cache[pageID] = page
	pm.cacheLock.Unlock()
	
	return page, nil
}

// WritePage writes a page to disk
func (pm *PageManager) WritePage(page *Page) error {
	// Create a copy for compression so we don't modify the cached version
	compressedPage := &Page{}
	*compressedPage = *page
	
	// Compress if enabled
	if err := pm.compressPage(compressedPage); err != nil {
		return fmt.Errorf("failed to compress page %d: %w", page.Header.PageID, err)
	}
	
	// Update checksum on compressed page
	compressedPage.Header.Checksum = uint64(compressedPage.CalculateChecksum())
	
	// Write to disk
	offset := int64(page.Header.PageID * PageSize)
	buf := compressedPage.Marshal()
	
	n, err := pm.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != PageSize {
		return fmt.Errorf("incomplete page write: expected %d, got %d", PageSize, n)
	}
	
	// Update cache
	pm.cacheLock.Lock()
	pm.cache[page.Header.PageID] = page
	pm.isDirty[page.Header.PageID] = false
	pm.cacheLock.Unlock()
	
	return nil
}

// Flush writes all dirty pages to disk
func (pm *PageManager) Flush() error {
	pm.cacheLock.RLock()
	dirtyPages := make([]*Page, 0)
	for pageID, isDirty := range pm.isDirty {
		if isDirty {
			if page, ok := pm.cache[pageID]; ok {
				dirtyPages = append(dirtyPages, page)
			}
		}
	}
	pm.cacheLock.RUnlock()
	
	for _, page := range dirtyPages {
		if err := pm.WritePage(page); err != nil {
			return err
		}
	}
	
	return pm.file.Sync()
}

// Close flushes all pages and closes the file
func (pm *PageManager) Close() error {
	if err := pm.Flush(); err != nil {
		return err
	}
	// Ensure all data is written to disk
	if err := pm.file.Sync(); err != nil {
		return err
	}
	return pm.file.Close()
}

// GetPageCount returns the total number of pages
func (pm *PageManager) GetPageCount() uint64 {
	return pm.nextPageID
}

// initializeCompressors creates compressor instances based on options
func (pm *PageManager) initializeCompressors() error {
	// Create compressors for each unique compression type
	compressionTypes := []CompressionType{
		pm.compressionOptions.DefaultPageCompression,
		pm.compressionOptions.LeafPageCompression,
		pm.compressionOptions.InternalPageCompression,
		pm.compressionOptions.StringPageCompression,
		pm.compressionOptions.BitmapPageCompression,
	}
	
	for _, compType := range compressionTypes {
		if compType != CompressionNone {
			if _, exists := pm.compressors[compType]; !exists {
				compressor, err := CreateCompressor(compType, pm.compressionOptions.DefaultCompressionLevel)
				if err != nil {
					return err
				}
				pm.compressors[compType] = compressor
			}
		}
	}
	
	return nil
}

// getCompressionType returns the appropriate compression type for a page
func (pm *PageManager) getCompressionType(pageType PageType) CompressionType {
	switch pageType {
	case PageTypeBTreeLeaf:
		return pm.compressionOptions.LeafPageCompression
	case PageTypeBTreeInternal:
		return pm.compressionOptions.InternalPageCompression
	case PageTypeStringSegment:
		return pm.compressionOptions.StringPageCompression
	case PageTypeRoaringBitmap:
		return pm.compressionOptions.BitmapPageCompression
	default:
		return pm.compressionOptions.DefaultPageCompression
	}
}

// compressPage compresses a page's data section
func (pm *PageManager) compressPage(page *Page) error {
	compressionType := pm.getCompressionType(page.Header.PageType)
	
	// Skip if no compression or data is too small
	if compressionType == CompressionNone || 
	   int(page.Header.DataSize) < pm.compressionOptions.MinPageSizeToCompress {
		return nil
	}
	
	compressor, exists := pm.compressors[compressionType]
	if !exists {
		return fmt.Errorf("compressor not found for type %d", compressionType)
	}
	
	// Compress only the used portion of data
	// Ensure DataSize doesn't exceed actual data length
	if page.Header.DataSize > uint32(len(page.Data)) {
		page.Header.DataSize = uint32(len(page.Data))
	}
	
	originalData := page.Data[:page.Header.DataSize]
	compressedData, err := compressor.Compress(originalData)
	if err != nil {
		return err
	}
	
	// Only use compression if it actually reduces size
	if len(compressedData) < len(originalData) {
		// Store original size in reserved field
		ByteOrder.PutUint32(page.Header.Reserved[:4], page.Header.DataSize)
		
		// Update page with compressed data
		copy(page.Data[:], compressedData)
		page.Header.DataSize = uint32(len(compressedData))
		page.Header.CompressionType = uint8(compressionType)
		
		// Update stats
		pm.compressionStats.UncompressedBytes += int64(len(originalData))
		pm.compressionStats.CompressedBytes += int64(len(compressedData))
		pm.compressionStats.PageCompressions[page.Header.PageType]++
	}
	
	return nil
}

// decompressPage decompresses a page's data section
func (pm *PageManager) decompressPage(page *Page) error {
	if page.Header.CompressionType == 0 {
		return nil // Not compressed
	}
	
	compressor, exists := pm.compressors[CompressionType(page.Header.CompressionType)]
	if !exists {
		return fmt.Errorf("compressor not found for type %d", page.Header.CompressionType)
	}
	
	// Get original size from reserved field
	originalSize := ByteOrder.Uint32(page.Header.Reserved[:4])
	
	// Decompress data
	compressedData := page.Data[:page.Header.DataSize]
	decompressedData, err := compressor.Decompress(compressedData)
	if err != nil {
		return err
	}
	
	// Verify size
	if uint32(len(decompressedData)) != originalSize {
		return fmt.Errorf("decompression size mismatch: expected %d, got %d", 
			originalSize, len(decompressedData))
	}
	
	// Update page with decompressed data
	copy(page.Data[:], decompressedData)
	page.Header.DataSize = originalSize
	page.Header.CompressionType = 0
	
	return nil
}

// GetCompressionStats returns compression statistics
func (pm *PageManager) GetCompressionStats() *CompressionStats {
	if pm.compressionStats.UncompressedBytes > 0 {
		pm.compressionStats.CompressionRatio = float64(pm.compressionStats.CompressedBytes) / 
			float64(pm.compressionStats.UncompressedBytes)
	}
	return pm.compressionStats
}

// EstimateCompressedSize estimates the compressed size of data for a given page type
func (pm *PageManager) EstimateCompressedSize(pageType PageType, data []byte) (int, error) {
	compressionType := pm.getCompressionType(pageType)
	
	// No compression
	if compressionType == CompressionNone {
		return len(data), nil
	}
	
	// Too small to compress
	if len(data) < pm.compressionOptions.MinPageSizeToCompress {
		return len(data), nil
	}
	
	compressor, exists := pm.compressors[compressionType]
	if !exists {
		return len(data), nil // Assume no compression if compressor not found
	}
	
	// Compress to get actual size
	compressed, err := compressor.Compress(data)
	if err != nil {
		return len(data), err
	}
	
	// Return smaller of compressed or original
	if len(compressed) < len(data) {
		return len(compressed), nil
	}
	return len(data), nil
}