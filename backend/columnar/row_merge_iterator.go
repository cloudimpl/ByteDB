package columnar

import (
	"container/heap"
	"sort"
)

// RowIterator provides iteration over row numbers for a column
type RowIterator interface {
	Next() bool
	RowNum() uint64
	KeyValue() interface{} // The key value for this row
	Close() error
}

// ColumnRowIterator implements RowIterator for a single column
type ColumnRowIterator struct {
	col           *Column
	dataType      DataType
	allRows       []uint64 // All row numbers in sorted order
	rowValues     map[uint64]interface{} // Map of row -> key value
	currentIndex  int
	currentRow    uint64
	currentValue  interface{}
	valid         bool
}

// NewColumnRowIterator creates a new row iterator for a column
func NewColumnRowIterator(col *Column, dataType DataType) (*ColumnRowIterator, error) {
	// Collect all rows from the column
	rowMap := make(map[uint64]interface{})
	
	// Iterate through main B-tree to collect all rows
	iter := NewBTreeIterator(col.btree, dataType, col.stringSegment)
	defer iter.Close()
	
	for iter.Next() {
		key := iter.Key()
		rows := iter.Rows()
		
		// For each row, store its key value
		rowIter := rows.Iterator()
		for rowIter.HasNext() {
			rowNum := uint64(rowIter.Next())
			rowMap[rowNum] = key
		}
	}
	
	// Also add null rows if any
	if col.nullBitmap != nil {
		nullIter := col.nullBitmap.Iterator()
		for nullIter.HasNext() {
			rowNum := uint64(nullIter.Next())
			rowMap[rowNum] = nil // NULL value
		}
	}
	
	// Sort row numbers
	allRows := make([]uint64, 0, len(rowMap))
	for row := range rowMap {
		allRows = append(allRows, row)
	}
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i] < allRows[j]
	})
	
	cri := &ColumnRowIterator{
		col:          col,
		dataType:     dataType,
		allRows:      allRows,
		rowValues:    rowMap,
		currentIndex: -1,
	}
	
	return cri, nil
}

// Next advances to the next row
func (cri *ColumnRowIterator) Next() bool {
	cri.currentIndex++
	if cri.currentIndex >= len(cri.allRows) {
		cri.valid = false
		return false
	}
	
	cri.currentRow = cri.allRows[cri.currentIndex]
	cri.currentValue = cri.rowValues[cri.currentRow]
	cri.valid = true
	return true
}

// RowNum returns the current row number
func (cri *ColumnRowIterator) RowNum() uint64 {
	return cri.currentRow
}

// KeyValue returns the key value for the current row
func (cri *ColumnRowIterator) KeyValue() interface{} {
	return cri.currentValue
}

// Close closes the iterator
func (cri *ColumnRowIterator) Close() error {
	return nil
}

// RowMergeIteratorEntry represents an entry from one of the source iterators
type RowMergeIteratorEntry struct {
	Iterator  RowIterator
	RowNum    uint64
	KeyValue  interface{}
}

// RowMergeIteratorHeap is a min-heap for merge sort by row number
type RowMergeIteratorHeap []RowMergeIteratorEntry

func (h RowMergeIteratorHeap) Len() int { return len(h) }

func (h RowMergeIteratorHeap) Less(i, j int) bool {
	// Sort by row number
	return h[i].RowNum < h[j].RowNum
}

func (h RowMergeIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *RowMergeIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(RowMergeIteratorEntry))
}

func (h *RowMergeIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// RowMergeIterator provides sorted iteration over rows from multiple columns
type RowMergeIterator struct {
	iterators    []RowIterator
	heap         *RowMergeIteratorHeap
	currentRow   uint64
	currentValue interface{}
	valid        bool
}

// NewRowMergeIterator creates a new row merge iterator
func NewRowMergeIterator(iterators []RowIterator) *RowMergeIterator {
	rmi := &RowMergeIterator{
		iterators: iterators,
		heap:      &RowMergeIteratorHeap{},
	}
	
	// Initialize heap with first entry from each iterator
	heap.Init(rmi.heap)
	
	for _, iter := range iterators {
		if iter.Next() {
			heap.Push(rmi.heap, RowMergeIteratorEntry{
				Iterator: iter,
				RowNum:   iter.RowNum(),
				KeyValue: iter.KeyValue(),
			})
		}
	}
	
	return rmi
}

// Next advances to the next row
func (rmi *RowMergeIterator) Next() bool {
	if rmi.heap.Len() == 0 {
		rmi.valid = false
		return false
	}
	
	// Get the minimum row number
	entry := heap.Pop(rmi.heap).(RowMergeIteratorEntry)
	rmi.currentRow = entry.RowNum
	rmi.currentValue = entry.KeyValue
	
	// Advance that iterator
	if entry.Iterator.Next() {
		heap.Push(rmi.heap, RowMergeIteratorEntry{
			Iterator: entry.Iterator,
			RowNum:   entry.Iterator.RowNum(),
			KeyValue: entry.Iterator.KeyValue(),
		})
	}
	
	rmi.valid = true
	return true
}

// RowNum returns the current row number
func (rmi *RowMergeIterator) RowNum() uint64 {
	return rmi.currentRow
}

// KeyValue returns the key value for the current row
func (rmi *RowMergeIterator) KeyValue() interface{} {
	return rmi.currentValue
}

// Valid returns whether the iterator is at a valid position
func (rmi *RowMergeIterator) Valid() bool {
	return rmi.valid
}

// Close closes all underlying iterators
func (rmi *RowMergeIterator) Close() error {
	for _, iter := range rmi.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}