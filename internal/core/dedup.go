package core

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/models"

	"github.com/natefinch/atomic"
	"go.uber.org/zap"
)

type Deduper interface {
	Dedup(target string)
}

type ConcurrentDeduper struct {
	Repo       db.Repository
	NumWorkers int
	DryRun     bool
	Logger     *zap.Logger
}

func (cd *ConcurrentDeduper) Dedup(parentCtx context.Context, target string) error {
	cd.Logger.Info("Deduplicating files", zap.String("target_directory", target))

	if !cd.DryRun {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			if err := os.MkdirAll(target, os.ModePerm); err != nil {
				return fmt.Errorf("unable to create target directory %v: %w", target, err)
			}
		}
	}

	media := cd.Repo.List()

	workers := make([]<-chan int64, cd.NumWorkers)
	for i := 0; i < cd.NumWorkers; i++ {
		workers[i] = dedup(cd.Logger, i, cd.DryRun, target, media)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	var total int64
	for i := range merge(ctx, workers...) {
		if total > 0 && total%1000 == 0 {
			cd.Logger.Info("Deduplicated a(nother) batch of files", zap.Int64("count", total))
		}
		total += i
	}
	cd.Logger.Info("Total deduplicated files", zap.Int64("total", total))

	return nil
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
					if err := atomic.WriteFile(target, bufio.NewReader(buf)); err != nil {
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
