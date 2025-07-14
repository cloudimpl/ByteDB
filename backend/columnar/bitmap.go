package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
)

// BitmapManager manages RoaringBitmap storage and retrieval
type BitmapManager struct {
	pageManager *PageManager
	cache       map[uint64]*roaring.Bitmap
}

// NewBitmapManager creates a new bitmap manager
func NewBitmapManager(pm *PageManager) *BitmapManager {
	return &BitmapManager{
		pageManager: pm,
		cache:       make(map[uint64]*roaring.Bitmap),
	}
}

// StoreBitmap stores a bitmap and returns the page offset
func (bm *BitmapManager) StoreBitmap(bitmap *roaring.Bitmap) (uint64, error) {
	// Serialize bitmap
	buf := new(bytes.Buffer)
	_, err := bitmap.WriteTo(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize bitmap: %w", err)
	}

	data := buf.Bytes()

	// Check if it fits in a single page
	availableSpace := PageSize - PageHeaderSize - 16 // 16 bytes for metadata
	if len(data) <= availableSpace {
		// Store in single page
		page := bm.pageManager.AllocatePage(PageTypeRoaringBitmap)

		// Write metadata
		pageBuf := bytes.NewBuffer(page.Data[:0])
		binary.Write(pageBuf, ByteOrder, uint32(bitmap.GetCardinality()))
		binary.Write(pageBuf, ByteOrder, uint32(len(data)))
		binary.Write(pageBuf, ByteOrder, uint64(0)) // Next page (0 = no continuation)

		// Write bitmap data
		pageBuf.Write(data)

		page.Header.DataSize = uint32(pageBuf.Len())
		page.Header.EntryCount = 1

		if err := bm.pageManager.WritePage(page); err != nil {
			return 0, err
		}

		// Cache the bitmap
		bm.cache[page.Header.PageID] = bitmap

		return page.Header.PageID, nil
	} else {
		// Multi-page storage
		return bm.storeMultiPageBitmap(bitmap, data)
	}
}

// storeMultiPageBitmap handles bitmaps that span multiple pages
func (bm *BitmapManager) storeMultiPageBitmap(bitmap *roaring.Bitmap, data []byte) (uint64, error) {
	firstPage := bm.pageManager.AllocatePage(PageTypeRoaringBitmap)
	currentPage := firstPage

	offset := 0
	availableSpace := PageSize - PageHeaderSize - 16
	firstPageDataSpace := availableSpace

	// Check if we need additional pages
	if len(data) > firstPageDataSpace {
		// We need multiple pages, allocate the second page now
		secondPage := bm.pageManager.AllocatePage(PageTypeRoaringBitmap)
		
		// Write metadata to first page with correct NextPageID
		pageBuf := bytes.NewBuffer(currentPage.Data[:0])
		binary.Write(pageBuf, ByteOrder, uint32(bitmap.GetCardinality()))
		binary.Write(pageBuf, ByteOrder, uint32(len(data)))
		binary.Write(pageBuf, ByteOrder, secondPage.Header.PageID) // Correct NextPageID

		// Write data to first page
		copySize := firstPageDataSpace
		pageBuf.Write(data[offset : offset+copySize])
		offset += copySize

		currentPage.Header.DataSize = uint32(pageBuf.Len())
		currentPage.Header.NextPageID = secondPage.Header.PageID

		if err := bm.pageManager.WritePage(currentPage); err != nil {
			return 0, err
		}

		currentPage = secondPage
	} else {
		// Single page is enough
		pageBuf := bytes.NewBuffer(currentPage.Data[:0])
		binary.Write(pageBuf, ByteOrder, uint32(bitmap.GetCardinality()))
		binary.Write(pageBuf, ByteOrder, uint32(len(data)))
		binary.Write(pageBuf, ByteOrder, uint64(0)) // No next page

		pageBuf.Write(data)
		currentPage.Header.DataSize = uint32(pageBuf.Len())
	}

	// Continue with additional pages if needed
	for offset < len(data) {
		// Write data to current page
		copySize := availableSpace
		if offset+copySize > len(data) {
			copySize = len(data) - offset
		}

		copy(currentPage.Data[:copySize], data[offset:offset+copySize])
		currentPage.Header.DataSize = uint32(copySize)
		offset += copySize

		// Check if we need another page
		if offset < len(data) {
			nextPage := bm.pageManager.AllocatePage(PageTypeRoaringBitmap)
			currentPage.Header.NextPageID = nextPage.Header.PageID

			if err := bm.pageManager.WritePage(currentPage); err != nil {
				return 0, err
			}

			currentPage = nextPage
		}
	}

	// Write final page
	if err := bm.pageManager.WritePage(currentPage); err != nil {
		return 0, err
	}

	// Cache the bitmap
	bm.cache[firstPage.Header.PageID] = bitmap

	return firstPage.Header.PageID, nil
}

// LoadBitmap loads a bitmap from the given page offset
func (bm *BitmapManager) LoadBitmap(pageID uint64) (*roaring.Bitmap, error) {
	// Check cache first
	if bitmap, exists := bm.cache[pageID]; exists {
		return bitmap, nil
	}

	// Load from disk
	firstPage, err := bm.pageManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	if firstPage.Header.PageType != PageTypeRoaringBitmap {
		return nil, fmt.Errorf("expected bitmap page, got %v", firstPage.Header.PageType)
	}

	// Read metadata
	buf := bytes.NewReader(firstPage.Data[:16])
	var cardinality uint32
	var dataSize uint32
	var nextPageID uint64

	binary.Read(buf, ByteOrder, &cardinality)
	binary.Read(buf, ByteOrder, &dataSize)
	binary.Read(buf, ByteOrder, &nextPageID)

	// Collect all data
	data := make([]byte, 0, dataSize)

	// Read from first page
	firstPageDataSize := firstPage.Header.DataSize - 16
	data = append(data, firstPage.Data[16:16+firstPageDataSize]...)

	// Read from continuation pages if needed
	currentPageID := nextPageID
	for currentPageID != 0 {
		page, err := bm.pageManager.ReadPage(currentPageID)
		if err != nil {
			return nil, err
		}

		data = append(data, page.Data[:page.Header.DataSize]...)
		currentPageID = page.Header.NextPageID
	}

	// Deserialize bitmap
	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to deserialize bitmap: %w", err)
	}

	// Verify cardinality
	if bitmap.GetCardinality() != uint64(cardinality) {
		return nil, fmt.Errorf("bitmap cardinality mismatch: expected %d, got %d",
			cardinality, bitmap.GetCardinality())
	}

	// Cache the bitmap
	bm.cache[pageID] = bitmap

	return bitmap, nil
}

// CreateBitmapFromRows creates a bitmap from a slice of row numbers
func (bm *BitmapManager) CreateBitmapFromRows(rows []uint64) (*roaring.Bitmap, uint64, error) {
	bitmap := roaring.New()
	for _, row := range rows {
		bitmap.Add(uint32(row))
	}

	pageID, err := bm.StoreBitmap(bitmap)
	if err != nil {
		return nil, 0, err
	}

	return bitmap, pageID, nil
}

// IntersectBitmaps performs AND operation on multiple bitmaps
func (bm *BitmapManager) IntersectBitmaps(pageIDs []uint64) (*roaring.Bitmap, error) {
	if len(pageIDs) == 0 {
		return roaring.New(), nil
	}

	// Load first bitmap
	result, err := bm.LoadBitmap(pageIDs[0])
	if err != nil {
		return nil, err
	}

	// Clone it to avoid modifying cached version
	result = result.Clone()

	// Intersect with remaining bitmaps
	for i := 1; i < len(pageIDs); i++ {
		bitmap, err := bm.LoadBitmap(pageIDs[i])
		if err != nil {
			return nil, err
		}

		result.And(bitmap)
	}

	return result, nil
}

// UnionBitmaps performs OR operation on multiple bitmaps
func (bm *BitmapManager) UnionBitmaps(pageIDs []uint64) (*roaring.Bitmap, error) {
	result := roaring.New()

	for _, pageID := range pageIDs {
		bitmap, err := bm.LoadBitmap(pageID)
		if err != nil {
			return nil, err
		}

		result.Or(bitmap)
	}

	return result, nil
}

// ExtractRowNumbers converts a bitmap to a slice of row numbers
func (bm *BitmapManager) ExtractRowNumbers(bitmap *roaring.Bitmap) []uint64 {
	rows := make([]uint64, 0, bitmap.GetCardinality())

	iter := bitmap.Iterator()
	for iter.HasNext() {
		rows = append(rows, uint64(iter.Next()))
	}

	return rows
}

// GetCardinality returns the number of set bits in a bitmap
func (bm *BitmapManager) GetCardinality(pageID uint64) (uint64, error) {
	// Try to read just from metadata without loading full bitmap
	page, err := bm.pageManager.ReadPage(pageID)
	if err != nil {
		return 0, err
	}

	if page.Header.PageType != PageTypeRoaringBitmap {
		return 0, fmt.Errorf("expected bitmap page, got %v", page.Header.PageType)
	}

	// Read cardinality from metadata
	cardinality := ByteOrder.Uint32(page.Data[0:4])
	return uint64(cardinality), nil
}
