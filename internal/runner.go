package internal

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/fedragon/go-dedup/internal/core"
	dedb "github.com/fedragon/go-dedup/internal/db"

	"go.uber.org/zap"
)

type Runner struct {
	logger    *zap.Logger
	dbPath    string
	source    string
	dest      string
	fileTypes []string
	dryRun    bool
}

func NewRunner(logger *zap.Logger, dbPath string, source string, dest string, fileTypes []string, dryRun bool) *Runner {
	return &Runner{
		logger:    logger,
		dbPath:    dbPath,
		source:    source,
		dest:      dest,
		fileTypes: fileTypes,
		dryRun:    dryRun,
	}
}

func (r *Runner) Run() error {
	start := time.Now()
	defer func() {
		r.logger.Info("Elapsed time", zap.Duration("elapsed", time.Since(start)))
		_ = r.logger.Sync()
	}()

	if r.dryRun {
		r.logger.Info("Running in DRY-RUN mode: duplicate files will not be moved")
	}

	repo, err := dedb.NewBoltRepository(r.dbPath, r.logger)
	if err != nil {
		return err
	}
	defer repo.Close()

	numWorkers := runtime.NumCPU()
	r.logger.Info("Determined number of workers", zap.Int("num_workers", numWorkers))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	indexer := core.ConcurrentIndexer{
		Repo:       repo,
		NumWorkers: numWorkers,
		FileTypes:  r.fileTypes,
		Logger:     r.logger,
	}
	indexer.Index(ctx, r.source)

	if err := core.Sweep(repo, r.logger); err != nil {
		return err
	}

	deduper := core.ConcurrentDeduper{
		Repo:       repo,
		NumWorkers: numWorkers,
		DryRun:     r.dryRun,
		Logger:     r.logger,
	}
	return deduper.Dedup(ctx, r.dest)
}
