package pkg

import (
	"context"
	"github.com/boltdb/bolt"
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/fs"
	log "github.com/sirupsen/logrus"
)

func Index(db *bolt.DB, fileTypes []string, numWorkers int, source string) {
	log.Printf("Indexing %v ...\n", source)

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
		workers[i] = dedb.Store(db, media, mark)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var upserted int64
	for i := range Merge(ctx, workers...) {
		if upserted > 0 && upserted%1000 == 0 {
			log.Printf("Indexed %v files so far\n", upserted)
		}
		upserted += i
	}
	log.Printf("Indexed %v files in total\n", upserted)
}
