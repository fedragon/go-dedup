package pkg

import (
	"bufio"
	"context"
	"os"
	"path/filepath"

	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/models"
	"github.com/natefinch/atomic"

	"go.uber.org/zap"
)

func Dedup(repo db.Repository, logger *zap.Logger, dryRun bool, numWorkers int, target string) {
	logger.Info("Deduplicating files to directory", zap.String("path", target))

	if !dryRun {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			if err := os.MkdirAll(target, os.ModePerm); err != nil {
				logger.Fatal("Unable to create target directory", zap.String("path", target), zap.Error(err))
			}
		}
	}

	media := repo.List()

	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = dedup(logger, i, dryRun, target, media)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var deduped int64
	for i := range Merge(ctx, workers...) {
		if deduped > 0 && deduped%1000 == 0 {
			logger.Info("Deduplicated a(nother) batch of files", zap.Int64("count", deduped))
		}
		deduped += i
	}
	logger.Info("Total deduplicated files", zap.Int64("total", deduped))
}

func dedup(logger *zap.Logger, id int, dryRun bool, targetDir string, media <-chan models.AggregatedMedia) <-chan int64 {
	moved := make(chan int64)
	log := logger.With(zap.Int("worker_id", id))

	go func() {
		defer close(moved)

		for m := range media {
			if len(m.Paths) > 1 {
				for _, path := range m.Paths[1:] {
					target := filepath.Join(targetDir, filepath.Base(path))

					if dryRun {
						log.Info("Would have moved file", zap.String("source", path), zap.String("dest", target))
						moved <- 1

						continue
					}

					buf, err := os.Open(path)
					if err != nil {
						log.Fatal(err.Error())
					}

					log.Info("Atomically moving file", zap.String("source", path), zap.String("dest", target))
					err = atomic.WriteFile(target, bufio.NewReader(buf))

					if err != nil {
						log.Error("Cannot atomically move file", zap.String("source", path), zap.String("dest", target), zap.Error(err))
						continue
					}

					if err = os.Remove(path); err != nil {
						log.Error("Cannot remove file", zap.String("path", path), zap.Error(err))
						continue
					}

					moved <- 1
				}
			}
		}
	}()

	return moved
}
