package main

import (
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/metrics"
	"github.com/fedragon/go-dedup/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/vrischmann/envconfig"
	"runtime"
)

func main() {
	var cfg struct {
		DbPath     string
		From       string
		To         string
		NumWorkers int  `envconfig:"optional"`
		DryRun     bool `envconfig:"default=false"`
	}

	if err := envconfig.Init(&cfg); err != nil {
		log.Fatalf(err.Error())
	}

	log.Printf("Using configuration: %+v\n", cfg)

	db, err := dedb.Connect(cfg.DbPath)
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	mx := metrics.NewMetrics()
	defer func() {
		if err := mx.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	numWorkers := cfg.NumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}
	log.Printf("Using %v concurrent goroutines\n", numWorkers)

	pkg.Index(mx, db, numWorkers, cfg.From)

	if cfg.DryRun {
		log.Println("Dry run: not going to move duplicates.")
		return
	}

	pkg.Dedup(mx, db, numWorkers, cfg.To)
}
