package boltutils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
)

func ungzipData(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err = ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func gzipData(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// GetGzipped get value, ungzip and return
func (db *DB) GetGzipped(bucketName interface{}, key []byte) ([]byte, error) {
	switch bucketN := bucketName.(type) {
	case []byte:
		data, err := db.Get(bucketN, key)
		if err != nil {
			return nil, err
		}
		return ungzipData(data)
	case [][]byte:
		data, err := db.GetPath(bucketN, key)
		if err != nil {
			return nil, err
		}
		return ungzipData(data)
	}

	return nil, fmt.Errorf("unknown bcuket name")
}

// PutGzip gzip value and put to db
func (db *DB) PutGzip(bucketName interface{}, key, value []byte) error {
	data, err := gzipData(value)
	if err != nil {
		return err
	}
	return db.Put(bucketName, key, data)
}

func (db *DB) IterateGzipped(bucketName []byte, fn func(k, v []byte) error) error {
	err := db.Iterate(bucketName, func(k, v []byte) error {
		data, err := ungzipData(v)
		if err != nil {
			return err
		}
		return fn(k, data)
	})
	return err
}
