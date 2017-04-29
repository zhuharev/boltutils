package boltutils

import (
	"bytes"
	"log"
	"os"
	"testing"
)

var (
	_testDbPath = "testdb"
	_testBucket = []byte("tb")
	_testKey    = []byte("byte")
	_testValue  = []byte("value")
)

func cleanDb() {
	os.RemoveAll(_testDbPath)
}

func TestPutGet(t *testing.T) {
	defer cleanDb()
	db, err := Open(_testDbPath, 0777, nil)
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
