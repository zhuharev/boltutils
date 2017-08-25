package boltutils

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// GetGzipped get value, ungzip and return
func (db *DB) GetGzipped(bucketName, key []byte) ([]byte, error) {
	data, err := db.Get(bucketName, key)
	if err != nil {
		return nil, err
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	data, err = ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	err = r.Close()
	if err != nil {
		return nil, err
	}
	return data, nil
}

// PutGzip gzip value and put to db
func (db *DB) PutGzip(bucketName, key, value []byte) error {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write(value)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return db.Put(bucketName, key, b.Bytes())
}
