package columnar

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Page represents a single page with dynamic size
type Page struct {
	Header PageHeader
	Data   []byte
}

// NewPage creates a new page with the given type and size
func NewPage(pageType PageType, pageID uint64, pageSize int) *Page {
	return &Page{
		Header: PageHeader{
			PageType:  pageType,
			PageID:    pageID,
			FreeSpace: uint32(pageSize - PageHeaderSize),
		},
		Data: make([]byte, pageSize-PageHeaderSize),
	}
}

// CalculateChecksum computes the CRC32 checksum of the page
// The pageSize parameter is needed to ensure consistent checksum calculation
func (p *Page) CalculateChecksumWithSize(pageSize int) uint32 {
	// Create a buffer of the exact page size
	buf := make([]byte, pageSize)
	
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
	ByteOrder.PutUint64(buf[49:57], 0) // Zero checksum for calculation
	copy(buf[57:PageHeaderSize], p.Header.Reserved[:])
	
	// Copy data (only what we have)
	if len(p.Data) > 0 {
		copy(buf[PageHeaderSize:], p.Data)
	}
	
	// Calculate checksum over entire page buffer
	return crc32.ChecksumIEEE(buf)
}

// CalculateChecksum computes the CRC32 checksum of the page
// Deprecated: Use CalculateChecksumWithSize for dynamic page sizes
func (p *Page) CalculateChecksum() uint32 {
	// This assumes the page data is already the correct size
	return p.CalculateChecksumWithSize(PageHeaderSize + len(p.Data))
}

// Marshal serializes the page to bytes
func (p *Page) Marshal() []byte {
	pageSize := PageHeaderSize + len(p.Data)
	buf := make([]byte, pageSize)
	
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
	if len(buf) < PageHeaderSize {
		return fmt.Errorf("buffer too small for page header: got %d bytes, need at least %d", len(buf), PageHeaderSize)
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
	dataSize := len(buf) - PageHeaderSize
	p.Data = make([]byte, dataSize)
	copy(p.Data, buf[PageHeaderSize:])
	
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
	pageSize           int  // Dynamic page size
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
	
	// For read-only, we need to determine page size from the file
	// The challenge is that we need to read the page to get the page size,
	// but we need the page size to read the page correctly.
	// Solution: Try common page sizes until we find one that works
	options := NewCompressionOptions()
	
	// Try to determine page size by reading the file header
	pageSizesToTry := []int{4096, 8192, 16384, 32768, 65536}
	var detectedPageSize int
	
	for _, tryPageSize := range pageSizesToTry {
		// Read a full page of this size
		pageBuf := make([]byte, tryPageSize)
		n, err := file.ReadAt(pageBuf, 0)
		if err != nil && err != io.EOF {
			continue
		}
		if n < tryPageSize && n < PageHeaderSize + 100 { // Need at least header + some file header
			continue
		}
		
		// Try to parse as a page
		testPage := &Page{}
		if err := testPage.Unmarshal(pageBuf[:n]); err != nil {
			continue
		}
		
		// Check if it's a file header page
		if testPage.Header.PageType == PageTypeFileHeader {
			// Extract page size from the file header data
			if len(testPage.Data) >= 20 {
				// Skip magic number (4), major version (2), minor version (2), file size (8)
				filePageSize := ByteOrder.Uint32(testPage.Data[8:12])
				if filePageSize == uint32(tryPageSize) {
					detectedPageSize = tryPageSize
					break
				}
			}
		}
		
		// Also check by calculating checksum to see if page is valid
		if uint64(testPage.CalculateChecksumWithSize(tryPageSize)) == testPage.Header.Checksum {
			detectedPageSize = tryPageSize
			break
		}
	}
	
	if detectedPageSize == 0 {
		file.Close()
		return nil, fmt.Errorf("could not determine page size from file")
	}
	
	options.PageSize = detectedPageSize
	
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
		pageSize:           detectedPageSize,
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
	pm.nextPageID = uint64(info.Size()) / uint64(pm.pageSize)
	
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
		pageSize:           options.PageSize,
	}
	
	// Initialize compressors based on options
	if err := pm.initializeCompressors(); err != nil {
		file.Close()
		return nil, err
	}
	
	if create {
		// Initialize with file header
		header := NewPage(PageTypeFileHeader, 0, pm.pageSize)
		if err := pm.WritePage(header); err != nil {
			file.Close()
			return nil, err
		}
		pm.nextPageID = 1
	} else {
		// For existing files, we need to determine the page size
		// Try common page sizes to read the header
		pageSizesToTry := []int{4096, 8192, 16384, 32768, 65536}
		var detectedPageSize int
		
		// fmt.Printf("Trying to detect page size from file...\n")
		for _, tryPageSize := range pageSizesToTry {
			// Read a full page of this size
			pageBuf := make([]byte, tryPageSize)
			n, err := file.ReadAt(pageBuf, 0)
			// fmt.Printf("  Try page size %d: read %d bytes, err=%v\n", tryPageSize, n, err)
			if err != nil && err != io.EOF {
				continue
			}
			// For smaller page sizes, we should have read exactly that amount
			// For larger page sizes, we might read less if the file is small
			if n < tryPageSize && n < PageHeaderSize + 100 {
				// fmt.Printf("    Not enough data\n")
				continue
			}
			
			// Try to parse as a page of exactly this size
			if n >= tryPageSize {
				// We have a full page
				testPage := &Page{}
				if err := testPage.Unmarshal(pageBuf[:tryPageSize]); err != nil {
					// fmt.Printf("    Failed to unmarshal: %v\n", err)
					continue
				}
				
				// fmt.Printf("    Page type: %v, PageID: %d\n", testPage.Header.PageType, testPage.Header.PageID)
				
				// Check if it's a file header page with valid checksum
				calcChecksum := testPage.CalculateChecksumWithSize(tryPageSize)
				// fmt.Printf("    Checksum: calculated=%x, stored=%x\n", calcChecksum, testPage.Header.Checksum)
				
				if testPage.Header.PageType == PageTypeFileHeader &&
				   uint64(calcChecksum) == testPage.Header.Checksum {
					// Extract page size from the file header data
					if len(testPage.Data) >= 20 {
						// Skip magic number (4), major version (2), minor version (2), file size (8)
						filePageSize := ByteOrder.Uint32(testPage.Data[16:20])
						// fmt.Printf("    File header page size: %d\n", filePageSize)
						if filePageSize == uint32(tryPageSize) {
							detectedPageSize = tryPageSize
							// fmt.Printf("    MATCH! Detected page size: %d\n", detectedPageSize)
							break
						}
					}
				}
			}
		}
		
		if detectedPageSize > 0 {
			pm.pageSize = detectedPageSize
			pm.compressionOptions.PageSize = detectedPageSize
		} else {
			// Could not detect page size
			file.Close()
			return nil, fmt.Errorf("could not determine page size from file")
		}
		
		// Calculate next page ID based on file size
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		pm.nextPageID = uint64(info.Size()) / uint64(pm.pageSize)
		
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
	
	page := NewPage(pageType, pageID, pm.pageSize)
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
	offset := int64(pageID) * int64(pm.pageSize)
	buf := make([]byte, pm.pageSize)
	
	n, err := pm.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != pm.pageSize {
		// If we can't read a full page, it might be because the file is truncated
		// or we're trying to read beyond the file size
		if n == 0 {
			return nil, fmt.Errorf("page %d is beyond file size", pageID)
		}
		return nil, fmt.Errorf("incomplete page read: expected %d, got %d", pm.pageSize, n)
	}
	
	page := &Page{}
	if err := page.Unmarshal(buf); err != nil {
		return nil, err
	}
	
	// Verify checksum using the actual page size
	calculatedChecksum := page.CalculateChecksumWithSize(pm.pageSize)
	if uint64(calculatedChecksum) != page.Header.Checksum {
		return nil, fmt.Errorf("checksum mismatch for page %d: calculated %x, expected %x", pageID, calculatedChecksum, page.Header.Checksum)
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
	compressedPage := &Page{
		Header: page.Header,
		Data:   make([]byte, pm.pageSize-PageHeaderSize),
	}
	// Copy only the used data
	copy(compressedPage.Data, page.Data)
	
	// Compress if enabled
	if err := pm.compressPage(compressedPage); err != nil {
		return fmt.Errorf("failed to compress page %d: %w", page.Header.PageID, err)
	}
	
	// Update checksum on compressed page
	compressedPage.Header.Checksum = uint64(compressedPage.CalculateChecksumWithSize(pm.pageSize))
	
	// Create buffer of exact page size
	buf := make([]byte, pm.pageSize)
	
	// Marshal header
	ByteOrder.PutUint16(buf[0:2], uint16(compressedPage.Header.PageType))
	buf[2] = compressedPage.Header.CompressionType
	ByteOrder.PutUint16(buf[3:5], compressedPage.Header.Flags)
	ByteOrder.PutUint32(buf[5:9], compressedPage.Header.DataSize)
	ByteOrder.PutUint64(buf[9:17], compressedPage.Header.PageID)
	ByteOrder.PutUint64(buf[17:25], compressedPage.Header.ParentPageID)
	ByteOrder.PutUint64(buf[25:33], compressedPage.Header.NextPageID)
	ByteOrder.PutUint64(buf[33:41], compressedPage.Header.PrevPageID)
	ByteOrder.PutUint32(buf[41:45], compressedPage.Header.EntryCount)
	ByteOrder.PutUint32(buf[45:49], compressedPage.Header.FreeSpace)
	ByteOrder.PutUint64(buf[49:57], compressedPage.Header.Checksum)
	copy(buf[57:PageHeaderSize], compressedPage.Header.Reserved[:])
	
	// Copy data
	copy(buf[PageHeaderSize:], compressedPage.Data)
	
	// Write to disk
	offset := int64(page.Header.PageID) * int64(pm.pageSize)
	n, err := pm.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != pm.pageSize {
		return fmt.Errorf("incomplete page write: expected %d, got %d", pm.pageSize, n)
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

// GetPageSize returns the page size used by this manager
func (pm *PageManager) GetPageSize() int {
	return pm.pageSize
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