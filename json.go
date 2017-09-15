package boltutils

import "encoding/json"

func (db *DB) GetJSON(bucketName, key []byte, v interface{}) (err error) {
	var data []byte
	if db.EnableGzip {
		data, err = db.GetGzipped(bucketName, key)
	} else {
		data, err = db.Get(bucketName, key)
	}
	if err != nil {
		return
	}
	return json.Unmarshal(data, v)
}

func (db *DB) PutJSON(bucketName, key []byte, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if db.EnableGzip {
		return db.PutGzip(bucketName, key, data)
	} else {
		return db.Put(bucketName, key, data)
	}
}
