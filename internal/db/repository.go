package db

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fedragon/go-dedup/internal/models"

	"github.com/boltdb/bolt"
	"go.uber.org/zap"
)

var bucketName = []byte("Hashes")

type Repository interface {
	List() <-chan models.AggregatedMedia
	Store(media <-chan models.Media) <-chan int64
	Sweep() error
}

type BoltRepository struct {
	db     *bolt.DB
	logger *zap.Logger
}

func NewBoltRepository(path string, logger *zap.Logger) (*BoltRepository, error) {
	db, err := connect(path)
	if err != nil {
		return nil, err
	}

	return &BoltRepository{
		db:     db,
		logger: logger,
	}, nil
}

func connect(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		return nil, err
	}

	return db, nil
}

func (r *BoltRepository) Close() {
	if err := r.db.Close(); err != nil {
		r.logger.Error("Unable to close database", zap.Error(err))
	}
}

func (r *BoltRepository) Store(media <-chan models.Media) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				r.logger.Fatal(m.Err.Error())
			}

			if err := r.store(m); err != nil {
				r.logger.Fatal(err.Error())
			}

			updated <- 1
		}
	}()

	return updated
}

func (r *BoltRepository) store(m models.Media) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		var entries map[string]bool
		bucket := tx.Bucket(bucketName)
		bytes := bucket.Get(m.Hash)

		if bytes != nil {
			if err := json.Unmarshal(bytes, &entries); err != nil {
				return err
			}
		}

		entries = mark(entries, m.Path)

		marshalled, err := json.Marshal(&entries)
		if err != nil {
			return err
		}

		return bucket.Put(m.Hash, marshalled)
	})
}

func mark(entries map[string]bool, path string) map[string]bool {
	exists := false
	for p := range entries {
		if p == path {
			entries[path] = true
			exists = true
			break
		}
	}
	if !exists {
		if entries == nil {
			entries = make(map[string]bool)
		}
		entries[path] = true
	}

	return entries
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
				for e := range entries {
					paths = append(paths, e)
				}

				media <- models.AggregatedMedia{Hash: k, Paths: paths}
				return nil
			})
		}); err != nil {
			r.logger.Fatal("Error while reading from bucket", zap.Error(err))
		}
	}()

	return media
}

func (r *BoltRepository) Sweep() error {
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

				entries = r.sweep(entries)

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

func (r *BoltRepository) sweep(entries map[string]bool) map[string]bool {
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
			r.logger.Info("Swept non-existing path", zap.String("path", path))
			delete(entries, path)
		}
	}

	return entries
}
