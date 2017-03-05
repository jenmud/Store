// Package store contains structs for storing stuff.
package store

import (
	"errors"
	"fmt"
)

var (
	// ErrNoKey error if no keys found.
	ErrNoKey = errors.New("No such key")

	// ErrNoItem error if items found.
	ErrNoItem = errors.New("No such item found")

	// ErrZeroItems error if there are zero items.
	ErrZeroItems = errors.New("Zero items")
)

// Hasher is a interface that wraps methods for generating a hash.
type Hasher interface {
	// Hash returns a unique reproducible hash of the struct.
	Hash() (string, error)
}

// NewBucket returns a new Bucket.
func NewBucket() (*Bucket, error) {
	bucket := Bucket{
		count: 0,
		items: make(map[string]interface{}),
	}
	return &bucket, nil
}

// Bucket is a dumping ground for storing items.
type Bucket struct {
	count uint64
	items map[string]interface{}
}

// Has checks if the item it in the bucket.
func (b Bucket) Has(item Hasher) bool {
	hash, _ := item.Hash()
	v, ok := b.items[hash]
	return ok == true && v == item
}

// Add adds an items to the bucket.
func (b *Bucket) Add(item Hasher) error {
	if !b.Has(item) {
		hash, err := item.Hash()
		if err != nil {
			return err
		}
		b.items[hash] = item
		b.count++
	}
	return nil
}

// Remove removes an items from the bucket.
func (b *Bucket) Remove(item Hasher) error {
	if b.count <= 0 {
		return ErrZeroItems
	}

	if !b.Has(item) {
		return ErrNoItem
	}

	hash, err := item.Hash()
	if err != nil {
		return err
	}

	delete(b.items, hash)
	b.count--
	return nil
}

// Len returns the unique item count in the bucket.
func (b Bucket) Len() uint64 {
	return b.count
}

// NewStore creates and returns a new store.
func NewStore() *Store {
	return &Store{
		buckets:   make(map[string]*Bucket),
		itemCount: 0,
	}
}

// Store is a structure for storing items for later use.
type Store struct {
	buckets   map[string]*Bucket
	itemCount uint64
}

// Buckets returns all the registered buckets in the store.
func (s Store) Buckets() []*Bucket {
	buckets := make([]*Bucket, len(s.buckets))
	count := 0
	for _, bucket := range s.buckets {
		buckets[count] = bucket
		count++
	}
	return buckets
}

// BucketsWhichContain returns all the buckets containing items.
func (s Store) BucketsWhichContain(items ...Hasher) []*Bucket {
	contains := []*Bucket{}
	buckets := s.Buckets()
	for _, item := range items {
		for _, bucket := range buckets {
			if bucket.Has(item) {
				contains = append(contains, bucket)
			}
		}
	}
	return buckets
}

// Add adds an item to the store.
func (s *Store) Add(bucketName string, item Hasher) error {
	bucket, err := s.GetBucket(bucketName)
	if err != nil {
		return err
	}
	return bucket.Add(item)
}

// Remove removes an item from the store.
func (s *Store) Remove(bucketName string, item Hasher) error {
	bucket, err := s.GetBucket(bucketName)
	if err != nil {
		return err
	}
	return bucket.Remove(item)
}

// HasBucket checks there is a bucket with ``name`` in the store.
func (s Store) HasBucket(name string) bool {
	_, ok := s.buckets[name]
	return ok
}

// GetBucket returns the bucket with ``name`` from the store.
func (s Store) GetBucket(name string) (*Bucket, error) {
	if !s.HasBucket(name) {
		return nil, fmt.Errorf("No such bucket %q", name)
	}
	return s.buckets[name], nil
}

// AddBucket adds a new bucket to the store.
func (s *Store) AddBucket(name string, bucket *Bucket) error {
	if s.HasBucket(name) {
		return fmt.Errorf(
			"bucket %q already exists",
			name,
		)
	}
	s.buckets[name] = bucket
	return nil
}

// RemoveBucket unsafely removes a bucket from the store.
func (s *Store) RemoveBucket(name string) {
	delete(s.buckets, name)
}
