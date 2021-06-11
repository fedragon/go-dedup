package db

import (
	"database/sql"
	"fmt"
	"github.com/fedragon/go-dedup/internal"
	"github.com/fedragon/go-dedup/internal/metrics"
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

func Store(metrics *metrics.Metrics, db *sql.DB, media <-chan internal.Media) <-chan int64 {
	updated := make(chan int64)

	go func() {
		defer close(updated)

		for m := range media {
			if m.Err != nil {
				log.Fatalf(m.Err.Error())
			}

			stop := metrics.Record("store")
			n, err := store(db, m)
			if err != nil {
				log.Fatalf(err.Error())
			}
			_ = stop()

			updated <- n
		}
	}()

	return updated
}

func store(db *sql.DB, m internal.Media) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO media(path, hash, unix_time) VALUES(?, ?, ?) 
		ON CONFLICT (path) DO UPDATE SET hash = excluded.hash, unix_time = excluded.unix_time 
		WHERE excluded.hash <> media.hash
	`)
	if err != nil {
		return 0, err
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			log.Printf(err.Error())
		}
	}()

	res, err := stmt.Exec(m.Path, fmt.Sprintf("%x", m.Hash), m.Timestamp.Unix())
	if err != nil {
		return 0, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, tx.Commit()
}

func List(db *sql.DB, offset int, limit int) (map[string][]MediaRow, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	stmt, err := tx.Prepare(`
		select m.ROWID, a.hash, m.path
			from (
				select hash, count(*) as total
					from media
				group by hash
			) a
			join media m using (hash)
			where a.total > 1
			order by 2, 3
			limit ? offset ?`)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(limit, offset)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]MediaRow)
	for rows.Next() {
		var rowID int64
		var hash, path string

		if err := rows.Scan(&rowID, &hash, &path); err != nil {
			return nil, err
		}

		result[hash] = append(result[hash], MediaRow{RowId: rowID, Path: path})
	}

	return result, nil
}
