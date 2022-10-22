package pkg

import (
	"github.com/fedragon/go-dedup/internal/db"

	"go.uber.org/zap"
)

func Sweep(repo db.Repository, logger *zap.Logger) error {
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

	return repo.Sweep(doSweep)
}
