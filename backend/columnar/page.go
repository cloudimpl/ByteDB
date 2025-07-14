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
	file      *os.File
	cache     map[uint64]*Page
	cacheLock sync.RWMutex
	nextPageID uint64
	isDirty    map[uint64]bool
}

// NewPageManager creates a new page manager
func NewPageManager(filename string, create bool) (*PageManager, error) {
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
	
	pm := &PageManager{
		file:    file,
		cache:   make(map[uint64]*Page),
		isDirty: make(map[uint64]bool),
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
		// Read file header to determine next page ID
		header, err := pm.ReadPage(0)
		if err != nil {
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
	
	// Read from disk
	offset := int64(pageID * PageSize)
	buf := make([]byte, PageSize)
	
	n, err := pm.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n != PageSize {
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
	
	// Cache the page
	pm.cacheLock.Lock()
	pm.cache[pageID] = page
	pm.cacheLock.Unlock()
	
	return page, nil
}

// WritePage writes a page to disk
func (pm *PageManager) WritePage(page *Page) error {
	// Update checksum
	page.Header.Checksum = uint64(page.CalculateChecksum())
	
	// Write to disk
	offset := int64(page.Header.PageID * PageSize)
	buf := page.Marshal()
	
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
	return pm.file.Close()
}

// GetPageCount returns the total number of pages
func (pm *PageManager) GetPageCount() uint64 {
	return pm.nextPageID
}