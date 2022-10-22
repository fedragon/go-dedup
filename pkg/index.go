package pkg

import (
	"context"
	"fmt"

	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/fs"

	"go.uber.org/zap"
)

func Index(repo db.Repository, logger *zap.Logger, fileTypes []string, numWorkers int, source string) {
	logger.Info(fmt.Sprintf("Indexing %v ...\n", source))

	media := fs.Walk(source, fileTypes)

	mark := func(entries map[string]bool, path string) {
		exists := false
		for p := range entries {
			if p == path {
				entries[path] = true
				exists = true
				break
			}
		}
		if !exists {
			if entries == nil {
				entries = make(map[string]bool)
			}
			entries[path] = true
		}
	}

	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = repo.Store(media, mark)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var upserted int64
	for i := range Merge(ctx, workers...) {
		if upserted > 0 && upserted%1000 == 0 {
			logger.Info(fmt.Sprintf("Indexed %v files so far\n", upserted))
		}
		upserted += i
	}
	logger.Info(fmt.Sprintf("Indexed %v files in total\n", upserted))
}
