package boltutils

import (
	"bytes"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

var (
	// ErrNotFound not found error
	ErrNotFound = fmt.Errorf("not found")
	// ErrBreak break's iterator
	ErrBreak = fmt.Errorf("break")
)

func makePutFunc(bucketName, key, value []byte) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bucket.Put(key, value)
	}
}

// Put value into database
func Put(db *bolt.DB, bucketName, key, value []byte) error {
	return db.Update(makePutFunc(bucketName, key, value))
}

func makeGetFunc(bucketName, key []byte, target *[]byte) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bolt.ErrBucketNotFound
		}
		bts := bucket.Get(key)
		if bts == nil {
			return ErrNotFound
		}
		*target = bts
		return nil
	}
}

// Get value from database
func Get(db *bolt.DB, bucketName, key []byte) ([]byte, error) {
	var res []byte
	err := db.View(makeGetFunc(bucketName, key, &res))
	return res, err
}

func makeIterateFunc(bucketName []byte, fn func(k, v []byte) error) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bolt.ErrBucketNotFound
		}

		c := bucket.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := fn(k, v)
			if err != nil {
				if err == ErrBreak {
					return nil
				}
				return err
			}
		}

		return nil
	}
}

// Iterate over database
func Iterate(db *bolt.DB, bucketName []byte, fn func(k, v []byte) error) error {
	return db.View(makeIterateFunc(bucketName, fn))
}

func makeIteratePrefixFunc(bucketName, prefix []byte, fn func(k, v []byte) error) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return bolt.ErrBucketNotFound
		}

		c := bucket.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			err := fn(k, v)
			if err != nil {
				if err == ErrBreak {
					return nil
				}
				return err
			}
		}

		return nil
	}
}

// IteratePrefix over database
func IteratePrefix(db *bolt.DB, bucketName, prefix []byte, fn func(k, v []byte) error) error {
	return db.View(makeIteratePrefixFunc(bucketName, prefix, fn))
}

func makeCreateBucketFunc(bucketName []byte) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return nil
	}
}

// CreateBucket create bucket if not exists
func CreateBucket(db *bolt.DB, bucketName []byte) error {
	return db.Update(makeCreateBucketFunc(bucketName))
}

// DB wrapper for bolt.DB
type DB struct {
	*bolt.DB
}

// New return pointer of DB
func New(db *bolt.DB) *DB {
	return &DB{DB: db}
}

// Open open database file and return pointer of DB
func Open(path string, mode os.FileMode, options *bolt.Options) (*DB, error) {
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

// Put value into db
func (db *DB) Put(bucketName, key, value []byte) error {
	return Put(db.DB, bucketName, key, value)
}

// Get value from db
func (db *DB) Get(bucketName, key []byte) ([]byte, error) {
	return Get(db.DB, bucketName, key)
}

// Iterate over db
func (db *DB) Iterate(bucketName []byte, fn func(k, v []byte) error) error {
	return Iterate(db.DB, bucketName, fn)
}

// IteratePrefix over db
func (db *DB) IteratePrefix(bucketName, prefix []byte, fn func(k, v []byte) error) error {
	return IteratePrefix(db.DB, bucketName, prefix, fn)
}

// CreateBucket create buckt if it not exists
func (db *DB) CreateBucket(bucketName []byte) error {
	return CreateBucket(db.DB, bucketName)
}
