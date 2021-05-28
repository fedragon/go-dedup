package db

import (
	"database/sql"
	"fmt"
	"github.com/fedragon/go-dedup/internal"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"log"
)

func Connect(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", path)
}

func Migrate(db *sql.DB, path string) error {
	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(path, "sqlite3", driver)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil {
		if err != migrate.ErrNoChange {
			return err
		}
	}

	return nil
}

func Store(db *sql.DB, m internal.Media) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert into media(path, hash, unix_time) values(?, ?, ?)")
	if err != nil {
		return err
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			log.Printf(err.Error())
		}
	}()

	if _, err = stmt.Exec(m.Path, fmt.Sprintf("%x", m.Hash), m.Timestamp.Unix()); err != nil {
		return err
	}

	return tx.Commit()
}
