package pkg

import (
	"database/sql"
	"github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/internal/metrics"
)

// Need a different database model:
// one table that stores all existing hashes
// one table that stores associations between hashes and paths
// To distribute the load:
// Count the total number of hashes in the table
// Divide it among the goroutines, each getting a given offset+limit and operating on those without conflicting with each other

// How to efficiently index files with this db model, without having different goroutines competing when trying to store the hash?

func Move(mx *metrics.Metrics, dbase *sql.DB) error {
	// 1. Select all hashes that have more than one corresponding file, with offset and limit
	//	select a.hash, a.total, m.path
	//		from (
	//			select hash, count(*) as total
	//			from media
	//			group by hash
	//		) a
	//		join media m using (hash)
	//		where a.total > 1
	//		order by 2 desc, 1, 3;
	// 2. Group duplicates together (e.g. map[Hash][]Path)
	// 3. Send each group to a channel
	// 4. (on the consumer side)
	//	  For each group
	//      for each duplicate
	//        move it to another directory
	//        delete its row from the `media` table (only if the above succeeds) by ROWID
	offset, limit := 0, 50

	for {
		rows, err := db.List(dbase, offset, limit)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			break
		}
	}

	return nil
}
