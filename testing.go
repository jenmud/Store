package store

import (
	"errors"

	h "github.com/mitchellh/hashstructure"
)

var (
	// ErrStopIteration error indicates that you have reached the
	// end of the iterator.
	ErrStopIteration = errors.New("no more items in the iterator")
)

// TestHashItem is a testing struct which implements the hasher interfaces
// for testing.
type TestHashItem struct {
	name    string
	surname string
	age     uint
}

// Hash returns a unique hash number used for identification.
func (i *TestHashItem) Hash() (uint64, error) {
	return h.Hash(i, nil)
}

// NewBucketSorter returns a container compatible with the sort.Interface.
func NewBucketSorter(items []*Bucket) *BucketSorter {
	return &BucketSorter{items: items, next: 0}
}

// BucketSorter is a container which is compatible with the sort.Interface.
type BucketSorter struct {
	items []*Bucket
	next  int
}

func (s *BucketSorter) Len() int {
	return len(s.items)
}

func (s *BucketSorter) Less(i, j int) bool {
	return s.items[i].Len() < s.items[j].Len()
}

func (s *BucketSorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

// Result returns the item in the current iteration.
func (s *BucketSorter) Result() *Bucket {
	item := s.items[s.next]
	s.next++
	return item
}

// Next indicates that there are still items to iterate over.
func (s *BucketSorter) Next() bool {
	return s.Len() == 0 || s.next > s.Len()
}
