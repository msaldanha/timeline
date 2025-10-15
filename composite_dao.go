package timeline

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Dao defines the minimal persistence operations required by CompositeTimeline.
// Implementations must be safe for concurrent use by multiple goroutines.
type Dao interface {
	// Init prepares the underlying storage (e.g., creates buckets).
	Init() error
	// Get returns the Item by key, a boolean indicating whether it was found, and an error.
	Get(_ context.Context, key string) (Item, bool, error)
	// Put stores or updates an Item in the timeline store.
	Put(item Item) error
	// GetLastKeyForAddress returns the last processed key for a given timeline address.
	GetLastKeyForAddress(address string) string
	// SetLastKeyForAddress sets the last processed key for a given timeline address.
	SetLastKeyForAddress(address, key string) error
	// DeleteLastKeyForAddress removes the stored last key for the given address.
	DeleteLastKeyForAddress(address string) error
	// DeleteAll removes all data related to the composite timeline namespace.
	DeleteAll() error
	// GetIterator returns an iterator starting from the provided key (exclusive).
	GetIterator(keyFrom string) (DaoIterator, error)
}

// CompositeDao is a BoltDB-backed implementation of the Dao interface
// used by CompositeTimeline to persist aggregated items and state.
type CompositeDao struct {
	db        *bolt.DB
	nameSpace string
}

// NewCompositeDao creates a new CompositeDao bound to the given BoltDB and namespace.
// The namespace isolates data so multiple composite timelines can share the same DB.
func NewCompositeDao(db *bolt.DB, nameSpace string) *CompositeDao {
	return &CompositeDao{db: db, nameSpace: nameSpace}
}

// Init ensures all required buckets exist in the BoltDB database for this namespace.
// It is safe to call multiple times.
func (c *CompositeDao) Init() error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.createBuckets(tx)
	})
}

// Get returns an Item by its logical key.
// It looks up the index to resolve the stored key, then loads and unmarshals the Item.
// The boolean indicates whether the Item was found.
func (c *CompositeDao) Get(_ context.Context, key string) (Item, bool, error) {
	var item Item
	found := false
	er := c.db.View(func(tx *bolt.Tx) error {
		tl, tlIndex, _ := c.getBuckets(tx)

		itemKey := tlIndex.Get([]byte(key))
		if itemKey == nil {
			return nil
		}

		v := tl.Get(itemKey)
		if v == nil {
			return nil
		}

		er := json.Unmarshal(v, &item)
		if er != nil {
			return er
		}
		found = true
		return nil
	})
	if er != nil {
		return item, found, er
	}
	return item, found, nil
}

// Put stores the given Item in the timeline buckets and updates the index and last-key map.
func (c *CompositeDao) Put(item Item) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		tl, tlIndex, lastKey := c.getBuckets(tx)

		value, err := json.Marshal(item)
		if err != nil {
			return err
		}

		seq, _ := tl.NextSequence()
		indexKey := fmt.Sprintf("%s|%09d", item.Timestamp, seq)

		err = tlIndex.Put([]byte(item.Key), []byte(indexKey))
		if err != nil {
			return err
		}

		err = tl.Put([]byte(indexKey), value)
		if err != nil {
			return err
		}

		err = lastKey.Put([]byte(item.Address), []byte(item.Key))
		if err != nil {
			return err
		}

		return nil
	})
}

// GetLastKeyForAddress reads the last stored logical key processed for the given timeline address.
// If no key is stored, it returns an empty string.
func (c *CompositeDao) GetLastKeyForAddress(address string) string {
	lastKey := ""
	_ = c.db.View(func(tx *bolt.Tx) error {
		_, _, addresses := c.getBuckets(tx)
		if addresses == nil {
			return fmt.Errorf("bucket %s not found", lastAddressKeyBucketName)
		}
		value := addresses.Get([]byte(address))
		if value == nil {
			return nil
		}
		lastKey = string(value)
		return nil
	})

	return lastKey
}

// SetLastKeyForAddress saves the last processed logical key for the given address.
func (c *CompositeDao) SetLastKeyForAddress(address, key string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, _, lastKey := c.getBuckets(tx)
		er := lastKey.Put([]byte(address), []byte(key))
		if er != nil {
			return er
		}
		return nil
	})
}

// DeleteLastKeyForAddress removes the last stored key entry for the given address.
func (c *CompositeDao) DeleteLastKeyForAddress(address string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, _, lastKey := c.getBuckets(tx)
		err := lastKey.Delete([]byte(address))
		if err != nil {
			return err
		}
		return nil
	})
}

// DeleteAll removes all data associated with this namespace and recreates the required buckets.
func (c *CompositeDao) DeleteAll() error {
	return c.db.Update(func(tx *bolt.Tx) error {
		comp := tx.Bucket([]byte(compositeBucketName))
		err := comp.DeleteBucket([]byte(c.nameSpace))
		if err != nil {
			return err
		}

		return c.createBuckets(tx)
	})
}

// GetIterator creates a new DaoIterator positioned to start reading items older than keyFrom.
func (c *CompositeDao) GetIterator(keyFrom string) (DaoIterator, error) {
	return NewDaoIterator(c, keyFrom)
}

func (c *CompositeDao) getBuckets(tx *bolt.Tx) (tl *bolt.Bucket, tlIndex *bolt.Bucket, lastKey *bolt.Bucket) {
	comp := tx.Bucket([]byte(compositeBucketName))
	own := comp.Bucket([]byte(c.nameSpace))
	tl = own.Bucket([]byte(timelineBucketName))
	tlIndex = own.Bucket([]byte(timelineIndexBucketName))
	lastKey = own.Bucket([]byte(lastAddressKeyBucketName))
	return
}

func (c *CompositeDao) createBuckets(tx *bolt.Tx) error {
	comp, er := tx.CreateBucketIfNotExists([]byte(compositeBucketName))
	if er != nil {
		return er
	}

	own, er := comp.CreateBucketIfNotExists([]byte(c.nameSpace))
	if er != nil {
		return er
	}

	_, er = own.CreateBucketIfNotExists([]byte(lastAddressKeyBucketName))
	if er != nil {
		return er
	}
	_, er = own.CreateBucketIfNotExists([]byte(timelineBucketName))
	if er != nil {
		return er
	}
	_, er = own.CreateBucketIfNotExists([]byte(timelineIndexBucketName))
	if er != nil {
		return er
	}
	return nil
}
