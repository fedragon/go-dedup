package db

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fedragon/go-dedup/internal"
	"github.com/fedragon/go-dedup/internal/metrics"
	log "github.com/sirupsen/logrus"
)

var bucketName = []byte("Hashes")

func Connect(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0600, nil)
}

func Store(metrics *metrics.Metrics, id int, db *bolt.DB, media <-chan internal.Media) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				log.Fatalf(m.Err.Error())
			}

			stop := metrics.Record(fmt.Sprintf("worker-%d.store", id))
			stored, err := store(db, m)
			if err != nil {
				log.Fatalf(err.Error())
			}
			_ = stop()

			if stored {
				updated <- 1
			}
		}
	}()

	return updated
}

func store(db *bolt.DB, m internal.Media) (bool, error) {
	stored := true

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
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
			if x.Path == m.Path { // path already exists
				stored = false
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

	if err != nil {
		return false, err
	}

	return stored, nil
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

			err := b.ForEach(func(k, v []byte) error {
				var stored []internal.Media
				err := json.Unmarshal(v, &stored)
				if err != nil {
					return err
				}

				media <- internal.AggregatedMedia{Hash: k, Medias: stored}
				return nil
			})

			return err
		}); err != nil {
			log.Fatalf("error while reading from bucket: %v\n", err)
		}
	}()

	return media
}
