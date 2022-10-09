package main

import (
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	dedb "github.com/fedragon/go-dedup/internal/db"
	"github.com/fedragon/go-dedup/pkg"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

const (
	dbPathFlag     = "db-path"
	sourceFlag     = "source-dir"
	destFlag       = "dest-dir"
	fileTypesFlag  = "file-types"
	dryRunFlag     = "dry-run"
	cpuProfileFlag = "cpu-profile"
	memProfileFlag = "mem-profile"
)

func main() {
	app := &cli.App{
		Usage:           "a cli to deduplicate media files",
		UsageText:       "dedup [global options]",
		Version:         "0.1.0",
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:     sourceFlag,
				Aliases:  []string{"src"},
				Required: true,
				EnvVars:  []string{"DEDUP_SRC_PATH"},
				Usage:    "Absolute path of the directory to scan",
			},
			&cli.PathFlag{
				Name:     destFlag,
				Aliases:  []string{"dst"},
				Required: true,
				EnvVars:  []string{"DEDUP_DST_PATH"},
				Usage:    "Absolute path of the directory to move duplicates to",
			},
			&cli.PathFlag{
				Name:    dbPathFlag,
				Aliases: []string{"db"},
				Value:   "./my.db",
				EnvVars: []string{"DEDUP_DB_PATH"},
				Usage:   "Path to the BoltDB file",
			},
			&cli.StringSliceFlag{
				Name:    fileTypesFlag,
				Value:   cli.NewStringSlice(".cr2", ".jpg", ".jpeg", ".mov", ".mp4", ".orf"),
				EnvVars: []string{"DEDUP_FILE_TYPES"},
				Usage:   "Media file types to be indexed",
			},
			&cli.BoolFlag{
				Name:    dryRunFlag,
				Value:   false,
				EnvVars: []string{"DEDUP_DRY_RUN"},
				Usage:   "Only print all `mv` operations that would be performed, without actually executing them",
			},
			&cli.PathFlag{
				Name:  cpuProfileFlag,
				Value: "./cpuprofile",
				Usage: "Enable profiler and write CPU profiler output to this file",
			},
			&cli.PathFlag{
				Name:  memProfileFlag,
				Value: "./memprofile",
				Usage: "Enable profiler and write memory profiler output to this file",
			},
		},
	}

	app.Action = func(c *cli.Context) error {
		dryRun := c.Bool(dryRunFlag)
		dbPath, err := homedir.Expand(c.String(dbPathFlag))
		if err != nil {
			log.Fatalf(err.Error())
		}
		source, err := homedir.Expand(c.String(sourceFlag))
		if err != nil {
			log.Fatal(err.Error())
		}
		dest, err := homedir.Expand(c.String(destFlag))
		if err != nil {
			log.Fatal(err.Error())
		}
		fileTypes := c.StringSlice(fileTypesFlag)

		cpuprofile, err := homedir.Expand(c.String(cpuProfileFlag))
		if err != nil {
			log.Fatal(err.Error())
		}
		if cpuprofile != "" {
			f, err := os.Create(cpuprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		memprofile, err := homedir.Expand(c.String(memProfileFlag))
		if err != nil {
			log.Fatal(err.Error())
		}
		if memprofile != "" {
			f, err := os.Create(memprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(f)
			defer f.Close()
		}

		return RunAction(dbPath, source, dest, fileTypes, dryRun)
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err.Error())
	}
}

func RunAction(dbPath string, source string, dest string, fileTypes []string, dryRun bool) error {
	start := time.Now()
	defer func() {
		log.Printf("Elapsed time: %v\n", time.Now().Sub(start))
	}()

	if dryRun {
		log.Println("Running in DRY-RUN mode: duplicate files will not be moved")
	}

	db, err := dedb.Connect(dbPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println(err.Error())
		}
	}()
	if err := dedb.Init(db); err != nil {
		log.Fatal(err.Error())
	}

	numWorkers := runtime.NumCPU()
	log.Printf("Using %v goroutines\n", numWorkers)

	pkg.Index(db, fileTypes, numWorkers, source)
	if err := pkg.Sweep(db); err != nil {
		log.Fatal(err.Error())
	}
	pkg.Dedup(db, dryRun, numWorkers, dest)

	return nil
}
