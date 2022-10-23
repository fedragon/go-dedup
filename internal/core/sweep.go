package core

import (
	"github.com/fedragon/go-dedup/internal/db"

	"go.uber.org/zap"
)

func Sweep(repo db.Repository, logger *zap.Logger) error {
	logger.Info("Sweeping stale entries...")
	return repo.Sweep()
}
