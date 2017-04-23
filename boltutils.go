package boltutils

import (
	"fmt"
	"github.com/boltdb/bolt"
)

var (
	ErrNotFound = fmt.Errorf("not found")
	ErrBreak    = fmt.Errorf("break")
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

func Iterate(db *bolt.DB, bucketName []byte, fn func(k, v []byte) error) error {
	return db.View(makeIterateFunc(bucketName, fn))
}

type DB struct {
	*bolt.DB
}

func New(db *bolt.DB) *DB {
	return &DB{DB: db}
}

func (db *DB) Put(bucketName, key, value []byte) error {
	return Put(db.DB, bucketName, key, value)
}

func (db *DB) Get(bucketName, key []byte) ([]byte, error) {
	return Get(db.DB, bucketName, key)
}

func (db *DB) Iterate(bucketName []byte, fn func(k, v []byte) error) error {
	return Iterate(db.DB, bucketName, fn)
}
