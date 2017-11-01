package boltutils

import (
	"bytes"
	"log"
	"testing"
)

func TestGzip(t *testing.T) {
	defer cleanDb()
	db, err := New(OpenPath(_testDbPath), Compression(GzipCompressor))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put(_testBucket, _testKey, _testValue)
	if err != nil {
		t.Fatal(err)
	}

	value, err := db.Get(_testBucket, _testKey)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(value, _testValue) {
		log.Fatalf("Value (%s) must be equal (%s)", value, _testValue)
	}
}
