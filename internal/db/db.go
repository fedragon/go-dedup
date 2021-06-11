package db

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/fedragon/go-dedup/internal"
	"github.com/fedragon/go-dedup/internal/metrics"
	"log"
)

func Connect(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0600, nil)
}

func Store(metrics *metrics.Metrics, db *bolt.DB, media <-chan internal.Media) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				log.Fatalf(m.Err.Error())
			}

			stop := metrics.Record("store")
			err := store(db, m)
			if err != nil {
				log.Fatalf(err.Error())
			}
			_ = stop()

			updated <- 1
		}
	}()

	return updated
}

func store(db *bolt.DB, m internal.Media) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("Hashes"))
		if err != nil {
			return err
		}

		var ms []internal.Media
		bytes := bucket.Get(m.Hash)

		if bytes != nil {
			if err = json.Unmarshal(bytes, &ms); err != nil {
				return err
			}
		}

		for _, x := range ms {
			if x.Path == m.Path {
				// path already exists
				return nil
			}
		}
		ms = append(ms, m)

		marshalled, err := json.Marshal(&ms)
		if err != nil {
			return err
		}

		if err = bucket.Put(m.Hash, marshalled); err != nil {
			return err
		}

		return nil
	})
}
