package columnar

import (
	"container/heap"
	
	roaring "github.com/RoaringBitmap/roaring/v2"
)

// MergeIteratorEntry represents an entry from one of the source iterators
type MergeIteratorEntry struct {
	Iterator  ColumnIterator
	Key       interface{}
	Rows      *roaring.Bitmap
	FileIdx   int
}

// MergeIteratorHeap is a min-heap for merge sort
type MergeIteratorHeap []MergeIteratorEntry

func (h MergeIteratorHeap) Len() int { return len(h) }

func (h MergeIteratorHeap) Less(i, j int) bool {
	// Compare keys based on type
	switch k1 := h[i].Key.(type) {
	case bool:
		k2 := h[j].Key.(bool)
		if k1 != k2 {
			return !k1 && k2 // false < true
		}
	case int8:
		k2 := h[j].Key.(int8)
		if k1 != k2 {
			return k1 < k2
		}
	case int16:
		k2 := h[j].Key.(int16)
		if k1 != k2 {
			return k1 < k2
		}
	case int32:
		k2 := h[j].Key.(int32)
		if k1 != k2 {
			return k1 < k2
		}
	case int64:
		k2 := h[j].Key.(int64)
		if k1 != k2 {
			return k1 < k2
		}
	case uint8:
		k2 := h[j].Key.(uint8)
		if k1 != k2 {
			return k1 < k2
		}
	case uint16:
		k2 := h[j].Key.(uint16)
		if k1 != k2 {
			return k1 < k2
		}
	case uint32:
		k2 := h[j].Key.(uint32)
		if k1 != k2 {
			return k1 < k2
		}
	case uint64:
		k2 := h[j].Key.(uint64)
		if k1 != k2 {
			return k1 < k2
		}
	case string:
		k2 := h[j].Key.(string)
		if k1 != k2 {
			return k1 < k2
		}
	}
	// If keys are equal, prefer earlier file
	return h[i].FileIdx < h[j].FileIdx
}

func (h MergeIteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MergeIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(MergeIteratorEntry))
}

func (h *MergeIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeIterator provides sorted iteration over multiple column iterators
type MergeIterator struct {
	iterators []ColumnIterator
	heap      *MergeIteratorHeap
	
	// Current merged entry
	currentKey   interface{}
	currentRows  *roaring.Bitmap
	valid        bool
	
	// Data type for comparison
	dataType DataType
}

// NewMergeIterator creates a new merge iterator
func NewMergeIterator(iterators []ColumnIterator, dataType DataType) *MergeIterator {
	mi := &MergeIterator{
		iterators: iterators,
		heap:      &MergeIteratorHeap{},
		dataType:  dataType,
	}
	
	// Initialize heap with first entry from each iterator
	heap.Init(mi.heap)
	
	for idx, iter := range iterators {
		if iter.Next() {
			heap.Push(mi.heap, MergeIteratorEntry{
				Iterator: iter,
				Key:      iter.Key(),
				Rows:     iter.Rows(),
				FileIdx:  idx,
			})
		}
	}
	
	return mi
}

// Next advances to the next key
func (mi *MergeIterator) Next() bool {
	if mi.heap.Len() == 0 {
		mi.valid = false
		return false
	}
	
	// Get the minimum key
	entry := heap.Pop(mi.heap).(MergeIteratorEntry)
	mi.currentKey = entry.Key
	mi.currentRows = roaring.New()
	mi.currentRows.Or(entry.Rows)
	
	// Always collect all entries with the same key (this is how columnar storage works)
	// Use peek to check if other iterators have the same key
	for mi.heap.Len() > 0 {
		nextEntry := (*mi.heap)[0] // Peek at top
		if !mi.keysEqual(mi.currentKey, nextEntry.Key) {
			break
		}
		
		// Pop and merge
		sameKeyEntry := heap.Pop(mi.heap).(MergeIteratorEntry)
		mi.currentRows.Or(sameKeyEntry.Rows)
		
		// Advance that iterator
		if sameKeyEntry.Iterator.Next() {
			heap.Push(mi.heap, MergeIteratorEntry{
				Iterator: sameKeyEntry.Iterator,
				Key:      sameKeyEntry.Iterator.Key(),
				Rows:     sameKeyEntry.Iterator.Rows(),
				FileIdx:  sameKeyEntry.FileIdx,
			})
		}
	}
	
	// Advance the original iterator
	if entry.Iterator.Next() {
		heap.Push(mi.heap, MergeIteratorEntry{
			Iterator: entry.Iterator,
			Key:      entry.Iterator.Key(),
			Rows:     entry.Iterator.Rows(),
			FileIdx:  entry.FileIdx,
		})
	}
	
	mi.valid = true
	return true
}

// Key returns the current key
func (mi *MergeIterator) Key() interface{} {
	return mi.currentKey
}

// Rows returns the current merged bitmap
func (mi *MergeIterator) Rows() *roaring.Bitmap {
	return mi.currentRows
}

// Valid returns whether the iterator is at a valid position
func (mi *MergeIterator) Valid() bool {
	return mi.valid
}

// Close closes all underlying iterators
func (mi *MergeIterator) Close() error {
	for _, iter := range mi.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// keysEqual compares two keys for equality
func (mi *MergeIterator) keysEqual(a, b interface{}) bool {
	switch k1 := a.(type) {
	case uint64:
		k2, ok := b.(uint64)
		return ok && k1 == k2
	case int64:
		k2, ok := b.(int64)
		return ok && k1 == k2
	case string:
		k2, ok := b.(string)
		return ok && k1 == k2
	default:
		return false
	}
}