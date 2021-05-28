package main

import (
	"github.com/fedragon/go-dedup/internal/app"
	"github.com/fedragon/go-dedup/internal/db"
	"log"
	"os"
)

func main() {
	dbase, err := db.Connect(os.Getenv("DB_PATH"))
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer func() {
		if err := dbase.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	if err := db.Migrate(dbase, os.Getenv("DB_MIGRATIONS_PATH")); err != nil {
		log.Fatalf(err.Error())
	}

	media := app.Walk(os.Getenv("ROOT"))
	var count int

	for m := range media {
		if count%1000 == 0 {
			log.Printf("Found %v media so far\n", count)
		}

		if m.Err != nil {
			log.Fatalf(m.Err.Error())
		}

		if err := db.Store(dbase, m); err != nil {
			log.Fatalf(err.Error())
		}

		count++
	}

	log.Printf("Found %v media\n", count)
}
