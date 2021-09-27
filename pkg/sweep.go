package pkg

import (
	"github.com/boltdb/bolt"
	dedb "github.com/fedragon/go-dedup/internal/db"
	log "github.com/sirupsen/logrus"
)

func Sweep(db *bolt.DB) error {
	log.Println("Sweeping stale entries...")

	return dedb.Prune(db)
}
