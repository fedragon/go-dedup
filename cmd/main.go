package main

import (
	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/metrics"
	"github.com/fedragon/go-dedup/pkg"
	"log"
	"os"
)

func main() {
	db, err := dedb.Connect(os.Getenv("DB"))
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

	pkg.Index(mx, db, os.Getenv("ROOT"))
}
