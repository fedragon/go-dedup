package dedup

import (
	"bufio"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fedragon/go-dedup/internal"
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/metrics"
	"github.com/fedragon/go-dedup/pkg"
	"github.com/natefinch/atomic"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"runtime"
)

func Dedup(mx *metrics.Metrics, db *bolt.DB, targetDir string) {
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		if err := os.MkdirAll(targetDir, os.ModePerm); err != nil {
			log.Fatalf("unable to create target directory %v\n", targetDir)
		}

	}

	media := dedb.List(db)

	numWorkers := runtime.NumCPU()
	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = dedup(mx, i, targetDir, media)
	}

	done := make(chan struct{})
	defer close(done)

	var deduped int64
	for i := range pkg.Merge(done, workers...) {
		if deduped > 0 && deduped%1000 == 0 {
			log.Printf("Deduplicated %v files so far\n", deduped)
		}
		deduped += i
	}
	log.Printf("Deduplicated %v files in total\n", deduped)
}

// dedup moves duplicated files into target directory
func dedup(mx *metrics.Metrics, id int, targetDir string, media <-chan internal.AggregatedMedia) <-chan int64 {
	moved := make(chan int64)

	go func() {
		defer close(moved)

		for m := range media {
			if len(m.Medias) > 1 {
				for _, x := range m.Medias[1:] {
					buf, err := os.Open(x.Path)
					if err != nil {
						log.Fatal(err)
					}

					target := filepath.Join(targetDir, filepath.Base(x.Path))
					stop := mx.Record(fmt.Sprintf("worker-%d.dedup", id))

					log.Printf("[worker-%d] moving file %v to %v\n", id, x.Path, target)
					err = atomic.WriteFile(target, bufio.NewReader(buf))
					_ = stop()

					if err != nil {
						log.Printf("[worker-%d] cannot atomically move file %v to %v: %v\n", id, x.Path, target, err)
						continue
					}

					if err = os.Remove(x.Path); err != nil {
						log.Printf("[worker-%d] cannot remove file %v: %v\n", id, x.Path, err)
						continue
					}

					moved <- 1
				}
			}
		}
	}()

	return moved
}
