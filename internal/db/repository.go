package db

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fedragon/go-dedup/internal/models"

	"github.com/boltdb/bolt"
	"go.uber.org/zap"
)

type Repository interface {
	List() <-chan models.AggregatedMedia
	Store(media <-chan models.Media, mark func(map[string]bool, string)) <-chan int64
	Sweep(doSweep func(map[string]bool)) error
}

type BoltRepository struct {
	db     *bolt.DB
	logger *zap.Logger
}

func NewRepository(db *bolt.DB, logger *zap.Logger) (Repository, error) {
	return &BoltRepository{
		db:     db,
		logger: logger,
	}, nil
}

func (r *BoltRepository) Store(media <-chan models.Media, mark func(map[string]bool, string)) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				r.logger.Fatal(m.Err.Error())
			}

			if err := store(r.db, m, mark); err != nil {
				r.logger.Fatal(err.Error())
			}

			updated <- 1
		}
	}()

	return updated
}

func store(db *bolt.DB, m models.Media, mark func(map[string]bool, string)) error {
	return db.Update(func(tx *bolt.Tx) error {
		var entries map[string]bool
		bucket := tx.Bucket(bucketName)
		bytes := bucket.Get(m.Hash)

		if bytes != nil {
			if err := json.Unmarshal(bytes, &entries); err != nil {
				return err
			}
		}

		if entries == nil {
			entries = make(map[string]bool)
		}

		mark(entries, m.Path)

		marshalled, err := json.Marshal(&entries)
		if err != nil {
			return err
		}

		return bucket.Put(m.Hash, marshalled)
	})
}

func (r *BoltRepository) List() <-chan models.AggregatedMedia {
	media := make(chan models.AggregatedMedia)

	go func() {
		defer close(media)

		if err := r.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)

			if b == nil {
				return fmt.Errorf("bucket %s doesn't exist", string(bucketName))
			}

			return b.ForEach(func(k, v []byte) error {
				var entries map[string]bool
				if err := json.Unmarshal(v, &entries); err != nil {
					return err
				}

				paths := make([]string, 0, len(entries))
				for k := range entries {
					paths = append(paths, k)
				}

				media <- models.AggregatedMedia{Hash: k, Paths: paths}
				return nil
			})
		}); err != nil {
			r.logger.Fatal("error while reading from bucket", zap.Error(err))
		}
	}()

	return media
}

func (r *BoltRepository) Sweep(doSweep func(map[string]bool)) error {
	return r.db.Update(func(tx *bolt.Tx) error {
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

				doSweep(entries)

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
