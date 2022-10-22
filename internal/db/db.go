package db

import (
	"github.com/boltdb/bolt"
)

var bucketName = []byte("Hashes")

func Connect(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0o600, nil)
}

func Init(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
}
