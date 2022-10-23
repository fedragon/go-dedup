package core

import (
	"context"
	"fmt"

	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/fs"
	"go.uber.org/zap"
)

type Indexer interface {
	Index(source string)
}

type ConcurrentIndexer struct {
	Repo       db.Repository
	FileTypes  []string
	NumWorkers int
	Logger     *zap.Logger
}

func (ci *ConcurrentIndexer) Index(source string) {
	ci.Logger.Info(fmt.Sprintf("Indexing %v ...\n", source))

	media := fs.Walk(source, ci.FileTypes)

	workers := make([]<-chan int64, ci.NumWorkers)
	for i := 0; i < ci.NumWorkers; i++ {
		workers[i] = ci.Repo.Store(media)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var upserted int64
	for i := range merge(ctx, workers...) {
		if upserted > 0 && upserted%1000 == 0 {
			ci.Logger.Info(fmt.Sprintf("Indexed %v files so far\n", upserted))
		}
		upserted += i
	}
	ci.Logger.Info(fmt.Sprintf("Indexed %v files in total\n", upserted))
}
