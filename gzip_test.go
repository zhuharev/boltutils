package boltutils

import (
	"bytes"
	"log"
	"testing"
)

func TestGzip(t *testing.T) {
	defer cleanDb()
	db, err := Open(_testDbPath, 0777, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutGzip(_testBucket, _testKey, _testValue)
	if err != nil {
		t.Fatal(err)
	}

	value, err := db.GetGzipped(_testBucket, _testKey)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(value, _testValue) {
		log.Fatalf("Value (%s) must be equal (%s)", value, _testValue)
	}
}
