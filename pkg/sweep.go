package pkg

import (
	"github.com/boltdb/bolt"
	dedb "github.com/fedragon/go-dedup/internal/db"
	log "github.com/sirupsen/logrus"
)

func Sweep(db *bolt.DB) error {
	log.Println("Sweeping stale entries...")

	doSweep := func(entries map[string]bool) {
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
				log.Printf("Swept non-existing path: %s\n", path)
				delete(entries, path)
			}
		}
	}

	return dedb.Sweep(db, doSweep)
}
