package internal

import (
	"runtime"
	"time"

	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/pkg"

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
	}()

	if r.dryRun {
		r.logger.Info("Running in DRY-RUN mode: duplicate files will not be moved")
	}

	db, err := dedb.Connect(r.dbPath)
	if err != nil {
		r.logger.Fatal(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			r.logger.Info(err.Error())
		}
	}()
	if err := dedb.Init(db); err != nil {
		r.logger.Fatal(err.Error())
	}

	repo, err := dedb.NewRepository(db, r.logger)
	if err != nil {
		r.logger.Fatal(err.Error())
	}

	numWorkers := runtime.NumCPU()
	r.logger.Info("Determined number of workers", zap.Int("num_workers", numWorkers))

	pkg.Index(repo, r.logger, r.fileTypes, numWorkers, r.source)
	if err := pkg.Sweep(repo, r.logger); err != nil {
		r.logger.Fatal(err.Error())
	}
	pkg.Dedup(repo, r.logger, r.dryRun, numWorkers, r.dest)

	return nil
}
