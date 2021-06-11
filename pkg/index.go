package pkg

import (
	"database/sql"
	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/fs"
	"github.com/fedragon/go-dedup/internal/metrics"
	"log"
	"runtime"
	"sync"
)

func Index(mx *metrics.Metrics, dbase *sql.DB, root string) {
	media := fs.Walk(mx, root)

	numWorkers := runtime.NumCPU()
	workers := make([]<-chan int64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = db.Store(mx, dbase, media)
	}

	done := make(chan struct{})
	defer close(done)

	var upserted int64
	for i := range merge(done, workers...) {
		if upserted > 0 && upserted%1000 == 0 {
			log.Printf("upserted %v rows so far\n", upserted)
		}
		upserted += i
	}
}

func merge(done <-chan struct{}, channels ...<-chan int64) <-chan int64 {
	var wg sync.WaitGroup

	wg.Add(len(channels))
	media := make(chan int64)
	multiplex := func(c <-chan int64) {
		defer wg.Done()
		for i := range c {
			select {
			case <-done:
				return
			case media <- i:
			}
		}
	}
	for _, c := range channels {
		go multiplex(c)
	}
	go func() {
		wg.Wait()
		close(media)
	}()
	return media
}
