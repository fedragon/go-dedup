package main

import (
	"github.com/fedragon/go-dedup/internal/app"
	"github.com/fedragon/go-dedup/internal/db"
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

	media := app.Walk(os.Getenv("ROOT"))
	var found int
	var upserted int64

	for m := range media {
		if found > 0 && found%1000 == 0 {
			log.Printf("Found %v media so far\n", found)
		}

		if m.Err != nil {
			log.Fatalf(m.Err.Error())
		}

		n, err := db.Store(dbase, m)
		if err != nil {
			log.Fatalf(err.Error())
		}

		upserted += n
		found++
	}

	log.Printf("Found %v media, upserted %v\n", found, upserted)
}
