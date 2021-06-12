# go-dedup

Recursively scans a directory for _media_ files and moves duplicates (if any) to a user-defined directory.

## Usage

    # scans ~/Pictures for media files and moves any duplicate to $TMPDIR/duplicates
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates

    # scans ~/Pictures for media files and prints all `mv` operations that would be performed, without executing them
    dedup --src=~/Pictures --dst=$TMPDIR/duplicates --dry-run

It's also possible to configure the application using environment variables: run `dedup --help` to see all available options.

## How it works

An execution runs two sequential stages:

- index files
- deduplicate files

### Index files

This stage recursively scans the _source_ directory for any media file: whenever it encounters a _media_ file, it computes a SHA256 hash based on the file contents and stores a key-value pair in a BoltDB bucket, which looks like this:

```
"00129cccac26ad1b0a1285f1a92049f6df572c9bc0a32d64914e486f572bfb0a": [ "~/somewhere/1.jpg", "~/somewhere-else/2.orc", ... ]
```

where the `key` is the computed hash and the value is a slice of paths pointing to all files that have that same hash (= duplicates).

**Note:** if the BoltDB database already exists, it will be deleted at the beginning of this stage, before doing anything else; this is to ensure that the application starts from a clean state.

### Deduplicate files

This stage iterates over all the keys (= hashes) that have been created by the previous stage and, for each of them, moves any duplicate files to the _target_ directory.

## How to build

    make
