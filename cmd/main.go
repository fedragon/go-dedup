package main

import (
	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/metrics"
	"github.com/fedragon/go-dedup/pkg"
	"log"
	"os"
)

func main() {
	dbase, err := db.Connect(os.Getenv("DB"))
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer func() {
		if err := dbase.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	if err := db.Migrate(dbase, os.Getenv("DB_MIGRATIONS")); err != nil {
		log.Fatalf(err.Error())
	}

	mx := metrics.NewMetrics()
	defer func() {
		if err := mx.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	pkg.Index(mx, dbase, os.Getenv("ROOT"))
}
