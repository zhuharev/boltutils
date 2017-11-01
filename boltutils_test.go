package boltutils

import (
	"bytes"
	"log"
	"os"
	"testing"

	"github.com/boltdb/bolt"
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

func dumpStructure(db *DB) {
	defer cleanDb()

	db.View(func(tx *bolt.Tx) error {
		tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			log.Printf("%s %v]n", name, b)
			return nil
		})
		return nil
	})
}

func TestPutGet(t *testing.T) {
	defer cleanDb()
	db, err := New(OpenPath(_testDbPath))
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

func TestCreateBucketPath(t *testing.T) {
	defer cleanDb()
	db, err := New(OpenPath(_testDbPath))
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateBucketPath([][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutPath([][]byte{[]byte("a"), []byte("c")}, []byte("allo"), []byte("dada"))
	if err != nil {
		t.Fatalf("%s", err)
	}
	data, err := db.GetPath([][]byte{[]byte("a"), []byte("c")}, []byte("allo"))
	if err != nil {
		t.Fatalf("%s", err)
	}
	if bytes.Compare(data, []byte("dada")) != 0 {
		t.Fatalf("data != %s, %d", data, bytes.Compare(data, []byte("dada")))
	}

	dumpStructure(db)
}
