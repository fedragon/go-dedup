package pkg

import (
	"github.com/boltdb/bolt"
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/fs"
	"github.com/fedragon/go-dedup/internal/metrics"
	log "github.com/sirupsen/logrus"
)

func Index(mx *metrics.Metrics, db *bolt.DB, fileTypes []string, numWorkers int, source string) {
	log.Printf("Starting to index %v\n", source)

	media := fs.Walk(mx, source, fileTypes)

	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = dedb.Store(mx, i, db, media)
	}

	done := make(chan struct{})
	defer close(done)

	var upserted int64
	for i := range Merge(done, workers...) {
		if upserted > 0 && upserted%1000 == 0 {
			log.Printf("Indexed %v files so far\n", upserted)
		}
		upserted += i
	}
	log.Printf("Indexed %v files in total\n", upserted)
}
