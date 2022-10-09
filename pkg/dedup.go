package pkg

import (
	"bufio"
	"context"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/fedragon/go-dedup/internal"
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/natefinch/atomic"
	log "github.com/sirupsen/logrus"
)

func Dedup(db *bolt.DB, dryRun bool, numWorkers int, target string) {
	log.Printf("Moving unique files to %v ...\n", target)

	if !dryRun {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			if err := os.MkdirAll(target, os.ModePerm); err != nil {
				log.Fatalf("unable to create target directory %v\n", target)
			}
		}
	}

	media := dedb.List(db)

	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = dedup(i, dryRun, target, media)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var deduped int64
	for i := range Merge(ctx, workers...) {
		if deduped > 0 && deduped%1000 == 0 {
			log.Printf("Deduplicated %v files so far\n", deduped)
		}
		deduped += i
	}
	log.Printf("Deduplicated %v files in total\n", deduped)
}

func dedup(id int, dryRun bool, targetDir string, media <-chan internal.AggregatedMedia) <-chan int64 {
	moved := make(chan int64)

	go func() {
		defer close(moved)

		for m := range media {
			if len(m.Paths) > 1 {
				for _, path := range m.Paths[1:] {
					target := filepath.Join(targetDir, filepath.Base(path))

					if dryRun {
						log.Printf("would have executed: mv %v %v\n", path, target)
						moved <- 1

						continue
					}

					buf, err := os.Open(path)
					if err != nil {
						log.Fatal(err)
					}

					log.Printf("[worker-%d] moving file %v to %v\n", id, path, target)
					err = atomic.WriteFile(target, bufio.NewReader(buf))

					if err != nil {
						log.Printf("[worker-%d] cannot atomically move file %v to %v: %v\n", id, path, target, err)
						continue
					}

					if err = os.Remove(path); err != nil {
						log.Printf("[worker-%d] cannot remove file %v: %v\n", id, path, err)
						continue
					}

					moved <- 1
				}
			}
		}
	}()

	return moved
}
