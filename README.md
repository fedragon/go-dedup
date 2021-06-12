# go-dedup

Recursively scans a directory for _media_ files and (optionally) moves duplicates to a different directory. Two files are considered duplicates if their contents produce the same SHA256 hash, regardless of their names.

## Usage

    # scans ~/Pictures for media files and moves any duplicate to $TMPDIR/duplicates
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates

    # scans ~/Pictures for media files and prints all `mv` operations that would be performed, without executing them
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates --dry-run

    # See all available options
    dedup --help

## How it works

It works in two stages:

- index: computes the SHA256 hash of each _media_ file found in the `SOURCE` directory and stores it to a BoltDB bucket, along with the original file path
- deduplicate: iterates over all the keys in the bucket and moves duplicate files (if any) to the `TARGET` directory