package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBucket(t *testing.T) {
	bucket, err := NewBucket()
	assert.NoError(t, err)
	assert.NotNil(t, bucket)
}

func TestAddToBucket(t *testing.T) {
	bucket, err := NewBucket()
	assert.NoError(t, err)

	something := &TestHashItem{name: "Foo"}
	err = bucket.Add(something)
	assert.NoError(t, err)
	assert.Equal(t, 1, int(bucket.Len()))

	// Add the same thing to make sure the count does not inc
	err = bucket.Add(something)
	assert.NoError(t, err)
	assert.Equal(t, 1, int(bucket.Len()))

	// Change ``something`` and re-add it to check the count
	// does not inc
	something.surname = "Bar"
	err = bucket.Add(something)
	assert.NoError(t, err)
	assert.Equal(t, 1, int(bucket.Len()))
}

func TestRemoveFromBucket(t *testing.T) {
	bucket, err := NewBucket()
	assert.NoError(t, err)

	missing := &TestHashItem{name: "Bar"}
	something := &TestHashItem{name: "Foo"}

	err = bucket.Add(something)
	assert.NoError(t, err)

	// First check that removing something that does not exist
	// in the bucket with items raises the correct error
	err = bucket.Remove(missing)
	assert.EqualError(t, err, "No such item found")

	// Add the same thing to make sure the count does not inc
	err = bucket.Remove(something)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(bucket.Len()))

	// Try and remove something that is not in the bucket
	err = bucket.Remove(missing)
	assert.EqualError(t, err, "Zero items")
}

func TestHasBucket(t *testing.T) {
	bucket, err := NewBucket()
	assert.NoError(t, err)

	somethingElse := &TestHashItem{name: "Bar"}
	something := &TestHashItem{name: "Foo"}

	err = bucket.Add(something)
	assert.NoError(t, err)

	assert.Equal(t, true, bucket.Has(something))
	assert.Equal(t, false, bucket.Has(somethingElse))
}

func TestStoreAddBucket(t *testing.T) {
	bucket, err := NewBucket()
	assert.NoError(t, err)

	store := NewStore()
	err = store.AddBucket("nodes", bucket)
	assert.NoError(t, err)

	err = store.AddBucket("nodes", bucket)
	assert.Error(t, err)
}

func TestStoreRemoveBucket(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	edges, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	err = store.AddBucket("edges", edges)
	assert.NoError(t, err)

	store.RemoveBucket("nodes")
	assert.Equal(t, false, store.HasBucket("nodes"))
	assert.Equal(t, true, store.HasBucket("edges"))
}

func TestStoreHasBucket(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	edges, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	err = store.AddBucket("edges", edges)
	assert.NoError(t, err)

	assert.Equal(t, true, store.HasBucket("nodes"))
	assert.Equal(t, true, store.HasBucket("edges"))
}

func TestStoreGetBucket(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	edges, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	err = store.AddBucket("edges", edges)
	assert.NoError(t, err)

	bucket, err := store.GetBucket("nodes")
	assert.NoError(t, err)
	assert.NotNil(t, bucket)
	assert.Equal(t, nodes, bucket)
}

func TestStoreAddItem(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	foo := &TestHashItem{name: "Foo"}
	bar := &TestHashItem{name: "Bar"}

	err = store.Add("nodes", foo)
	assert.NoError(t, err)

	err = store.Add("nodes", bar)
	assert.NoError(t, err)

	err = store.Add("missing-bucket", bar)
	assert.Error(t, err)
}

func TestStoreRemoveItem(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	foo := &TestHashItem{name: "Foo"}
	bar := &TestHashItem{name: "Bar"}

	err = store.Add("nodes", foo)
	assert.NoError(t, err)

	err = store.Add("nodes", bar)
	assert.NoError(t, err)

	err = store.Remove("nodes", bar)
	assert.NoError(t, err)
	assert.Equal(t, false, nodes.Has(bar))

	err = store.Remove("missing-bucket", foo)
	assert.Error(t, err)
}

func TestStoreBuckets(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	edges, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	err = store.AddBucket("edges", edges)
	assert.NoError(t, err)

	//Todo rather use this when working - AssertItemsEqual

	buckets := NewBucketSorter(store.Buckets())
	expected := NewBucketSorter([]*Bucket{nodes, edges})

	for buckets.Next() {
		for expected.Next() {
			bkt := buckets.Result()
			ex := expected.Result()
			assert.Equal(t, ex, bkt)
		}
	}
}

func TestStoreContains(t *testing.T) {
	store := NewStore()

	nodes, err := NewBucket()
	assert.NoError(t, err)

	edges, err := NewBucket()
	assert.NoError(t, err)

	err = store.AddBucket("nodes", nodes)
	assert.NoError(t, err)

	err = store.AddBucket("edges", edges)
	assert.NoError(t, err)

	foo := &TestHashItem{name: "Foo"}
	bar := &TestHashItem{name: "Bar"}
	cat := &TestHashItem{name: "Cat"}

	err = store.Add("nodes", foo)
	assert.NoError(t, err)

	err = store.Add("nodes", bar)
	assert.NoError(t, err)

	err = store.Add("edges", cat)
	assert.NoError(t, err)

	//Todo rather use this when working - AssertItemsEqual
	buckets := NewBucketSorter(store.BucketsWhichContain(foo, cat))

	for buckets.Next() {
		bkt := buckets.Result()
		assert.Equal(t, nodes, bkt)
		assert.Equal(t, edges, bkt)
	}
}
