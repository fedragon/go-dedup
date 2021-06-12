# go-dedup

Recursively scans a directory for _media_ files and (optionally) moves duplicates to a different directory. Two files are considered duplicates if their contents produce the same SHA256 hash, regardless of their names.

## How to build

    make

## Usage

    # scans ~/Pictures for media files and moves any duplicate to $TMPDIR/duplicates
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates

    # scans ~/Pictures for media files and prints all `mv` operations that would be performed, without executing them
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates --dry-run

    # See all available options
    dedup --help

## How it works

An execution runs two sequential stages:

- index files
- deduplicate files

### Index files

This stage recursively scans the _source_ directory for any media file: whenever it encounters a _media_ file, it computes a SHA256 hash based on the file contents and stores a key-value pair in a BoltDB bucket, which looks like this:

```
"00129cccac26ad1b0a1285f1a92049f6df572c9bc0a32d64914e486f572bfb0a": [ { Path: "~/somewhere/1.jpg", ... }, { Path: "~/somewhere-else/2.orc", ... } ]
```

where the `key` is the computed hash and the value is a slice of all files that have that hash.

### Deduplicate files

This stage iterates over all the keys (= hashes) that have been created by the previous stage and, for each of them, moves any duplicate files to the _target_ directory.