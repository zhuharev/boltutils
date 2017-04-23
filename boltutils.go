package boltutils

import (
	"fmt"

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

// DB wrapper for bolt.DB
type DB struct {
	*bolt.DB
}

// New return pointer of DB
func New(db *bolt.DB) *DB {
	return &DB{DB: db}
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
