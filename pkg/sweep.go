package pkg

import (
	"github.com/boltdb/bolt"
	dedb "github.com/fedragon/go-dedup/internal/db"
	"go.uber.org/zap"
)

func Sweep(db *bolt.DB, logger *zap.Logger) error {
	logger.Info("Sweeping stale entries...")

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
				logger.Info("Swept non-existing path", zap.String("path", path))
				delete(entries, path)
			}
		}
	}

	return dedb.Sweep(db, doSweep)
}
