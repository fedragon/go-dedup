package main

import (
	"os"
	"runtime/pprof"

	"github.com/fedragon/go-dedup/internal"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"

	"go.uber.org/zap"
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
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

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
				Usage:    "Absolute path of the directory to scan",
			},
			&cli.PathFlag{
				Name:     destFlag,
				Aliases:  []string{"dst"},
				Required: true,
				Usage:    "Absolute path of the directory to move duplicates to",
			},
			&cli.PathFlag{
				Name:    dbPathFlag,
				Aliases: []string{"db"},
				Value:   "./my.db",
				Usage:   "Path to the BoltDB file",
			},
			&cli.StringSliceFlag{
				Name:    fileTypesFlag,
				Value:   cli.NewStringSlice(".cr2", ".jpg", ".jpeg", ".mov", ".mp4", ".orf"),
				EnvVars: []string{"DEDUP_FILE_TYPES"},
				Usage:   "Media file types to be indexed",
			},
			&cli.BoolFlag{
				Name:  dryRunFlag,
				Value: false,
				Usage: "Only print all `mv` operations that would be performed, without actually executing them",
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
			logger.Fatal(err.Error())
		}
		source, err := homedir.Expand(c.String(sourceFlag))
		if err != nil {
			logger.Fatal(err.Error())
		}
		dest, err := homedir.Expand(c.String(destFlag))
		if err != nil {
			logger.Fatal(err.Error())
		}
		fileTypes := c.StringSlice(fileTypesFlag)

		cpuprofile, err := homedir.Expand(c.String(cpuProfileFlag))
		if err != nil {
			logger.Fatal(err.Error())
		}
		if cpuprofile != "" {
			f, err := os.Create(cpuprofile)
			if err != nil {
				logger.Fatal(err.Error())
			}
			_ = pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}

		memprofile, err := homedir.Expand(c.String(memProfileFlag))
		if err != nil {
			logger.Fatal(err.Error())
		}
		if memprofile != "" {
			f, err := os.Create(memprofile)
			if err != nil {
				logger.Fatal(err.Error())
			}
			_ = pprof.WriteHeapProfile(f)
			defer f.Close()
		}

		return internal.NewRunner(logger.With(zap.Bool("dry_run", dryRun)), dbPath, source, dest, fileTypes, dryRun).Run()
	}

	if err := app.Run(os.Args); err != nil {
		logger.Fatal(err.Error())
	}
}
