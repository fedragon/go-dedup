package db

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/fedragon/go-dedup/internal"
	log "github.com/sirupsen/logrus"
)

var bucketName = []byte("Hashes")

func Connect(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0o600, nil)
}

func Store(id int, db *bolt.DB, media <-chan internal.Media) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				log.Fatalf(m.Err.Error())
			}

			if err := store(db, m); err != nil {
				log.Fatalf(err.Error())
			}

			updated <- 1
		}
	}()

	return updated
}

func store(db *bolt.DB, m internal.Media) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}

		var entries map[string]bool
		bytes := bucket.Get(m.Hash)

		if bytes != nil {
			if err = json.Unmarshal(bytes, &entries); err != nil {
				return err
			}
		}

		exists := false
		for path := range entries {
			if path == m.Path {
				entries[path] = true
				exists = true
				break
			}
		}
		if !exists {
			if entries == nil {
				entries = make(map[string]bool)
			}
			entries[m.Path] = true
		}

		marshalled, err := json.Marshal(&entries)
		if err != nil {
			return err
		}

		return bucket.Put(m.Hash, marshalled)
	})
}

func List(db *bolt.DB) <-chan internal.AggregatedMedia {
	media := make(chan internal.AggregatedMedia)

	go func() {
		defer close(media)

		if err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)

			if b == nil {
				return fmt.Errorf("bucket %s doesn't exist", string(bucketName))
			}

			return b.ForEach(func(k, v []byte) error {
				var entries map[string]bool
				if err := json.Unmarshal(v, &entries); err != nil {
					return err
				}

				var paths []string
				for k := range entries {
					paths = append(paths, k)
				}

				media <- internal.AggregatedMedia{Hash: k, Paths: paths}
				return nil
			})
		}); err != nil {
			log.Fatalf("error while reading from bucket: %v\n", err)
		}
	}()

	return media
}

func Prune(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.New("bucket doesn't exist")
		}

		c := bucket.Cursor()
		for hash, v := c.First(); hash != nil; hash, v = c.Next() {
			var entries map[string]bool
			if v != nil {
				if err := json.Unmarshal(v, &entries); err != nil {
					return err
				}

				if entries != nil {
					var missing []string
					for path, marked := range entries {
						if marked {
							entries[path] = false
						} else {
							missing = append(missing, path)
						}
					}
					for _, path := range missing {
						log.Printf("Pruned non-existing path: %s\n", path)
						delete(entries, path)
					}
				}

				marshalled, err := json.Marshal(&entries)
				if err != nil {
					return err
				}

				if err = bucket.Put(hash, marshalled); err != nil {
					return err
				}
			}
		}

		return nil
	})
}
