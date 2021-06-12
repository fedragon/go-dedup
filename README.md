# go-dedup

Recursively scans a directory for _media_ files and moves duplicates (if any) to a user-defined directory.

## Usage

    # scans ~/Pictures for media files and moves any duplicate to $TMPDIR/duplicates
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates

    # scans ~/Pictures for media files and prints all `mv` operations that would be performed, without executing them
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates --dry-run

## All available options

    NAME:
    dedup - a cli to deduplicate media files

    USAGE:
    dedup [global options]

    VERSION:
    0.1.0

    GLOBAL OPTIONS:
    --source-dir value, --src value  Absolute path of the directory to scan [$DEDUP_SRC_PATH]
    --dest-dir value, --dst value    Absolute path of the directory to move duplicates to [$DEDUP_DST_PATH]
    --db-path value, --db value      Path to the BoltDB file (default: "./my.db") [$DEDUP_DB_PATH]
    --file-types value               Media file types to be indexed (default: ".cr2", ".jpg", ".jpeg", ".mov", ".mp4", ".orf") [$DEDUP_FILE_TYPES]
    --dry-run mv                     Only print all mv operations that would be performed, without actually executing them (default: false) [$DEDUP_DRY_RUN]
    --help, -h                       show help (default: false)
    --version, -v                    print the version (default: false)

## How to build

    make
