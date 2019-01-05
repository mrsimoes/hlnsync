# lnsync

## Introduction

_lnsync_ provides unidirectional local file tree sync with rename detection and support for hardlinks.

This package started as a Python learning project. I've found it useful enough to polish for publication, but it underwent only limited testing and so should be used with adequate caution.

### Purpose and Operation

When files are renamed/moved in the source file tree, _lnsync_  syncs the target by renaming, without deleting and recopying. Since _lnsync_ never creates or deletes data on the target, it may be used as a preprocessing step for other sync tools, such as _rsync_.

File content is compared by size and content hash, which _lnsync_ stores in single-file databases at the top-level directory of each file tree. Using those hashes, _lnsync_ can also find duplicate files (like _fdupes_), compare file trees, and check for changes/bitrot.

On certain file systems (e.g. ext3/4 and btrfs), a file may have multiple file paths, which function as equivalent aliases. A new path created for an existing file is a _hardlink_, but all such aliases are on an equal footing, so each may be called a hardlink.

### Alternative Solutions

Many tools exist for syncing with rename detection:

- Patches for _rsync_ have been published (see --detect-renamed), relying on file size and modification time, or matching files in nearby directories. Unlike _rsync_, _lnsync_ does not offer a network protocol.

- [bsync](https://github.com/dooblem/bsync) provides network syncing with rename detection, but no support for hardlinks.

- _git_ itself is based on syncing by content, and has been adapted for syncing.

- Support in modern file systems (e.g. btrfs) for snapshots may be adapted for syncing.

### Hash Databases and Offline File Trees

The hash database is a single file at the top directory of any tree processed, with basename matching `lnsync-[0-9]+.db`. (Only one such file should exist at that location.) These files are ignored by _lnsync_ operations, and should not be copied from source to target when syncing with other tools.

_lnsync_ also allows incorporating the file tree structure into the hash database, turning into an _offline file tree_, which may be used place of the original directory for certain purposes: a target directory may be synced from a source offline file tree and _lnsync_ may search for files common to an offline file tree and a given local directory.

## Installation and Quickstart

### Installing

Install from the test PyPI repository with `pip install lnsync`.

Or, clone the repo with `git clone https://github.com/mrsimoes/lnsync.git` and run `python setup.py install`.

### Example Usage

If you reorganize your photo collection, renaming and moving around files and directories, _lnsync_ will mirror those changes to your backup. Also, if you use hardlinks to organize your photo collection, _lnsync_ will replicate that structure to the backup. It will also delete directories which have become empty on the target and no longer exist on the source.

If you have your photo archive at `/home/you/Photos` and your backup is at `/mnt/disk/Photos`, run `lnsync sync /home/you/Photos /mnt/disk/Photos` to sync. For a dry run: `lnsync sync -n /home/you/Photos /mnt/disk/Photos`.

To quickly obtain an _rsync_ command that will complete syncing, skipping the hash database files, run `lnsync rsync /home/you/Photos /mnt/disk/Photos`. To also run this command, use the `-x` switch. Make sure the `rsync` options provided by this command are suitable for you.

Finally, to check target is in-sync, recursively compare source and backup with `lnsync cmp /home/you/Photos /mnt/disk/Photos`.

To find duplicate files on the Photos directory, run `lnsync fdupes /home/you/Photos`. Use `-H` to count different hardlinks to the same file as duplicates. Use `-z` to compare by size only.

To find all files in Photos which are not in the backup (under any name), run `lnsync onfirstonly /home/you/Photos /mnt/disk/Photos`.


## Command Reference
All _lnsync_ commands are `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

### Syncing

- `lnsync sync [-d] [-M <size>] <source> <target>` syncs a target dir from a source dir (or offline tree).

First target files are matched to source files. Each matched target file is associated to a single source file. If either file system supports hardlinks, a file may have multiple pathnames. _lnsync_ will not complain if the match is not unique or some files are not matched on either source and/or target.

For each matched target file, its pathnames are set to match those of the corresponding source file, by renaming, deleting, or creating hardlinks. New intermediate subdirectories are created as needed on the target; directories which become empty on the target are not removed. Also, if distinct source files A, B, C are renamed B, C, A on the target, undoing this cycle is currently not supported.

With the `-z` switch, files are matched by size only and hash databases are not created or updated.

With the `-M` option, only files of size at most `<size>` are considered for matching. Size may be given in human form: `10k`, `2.1M`, `3G`, etc.

Use `-n` for a dry-run, showing which operations would be executed.

_lnsync_ does not copy file data from the source or delete file data on the target.

- `lnsync rsync <tree> <dir>` prints an _rsync_ command that will sync target dir from source dir or offline tree, skipping _lnsync_ database files. Make sure the _rsync_ options given are suitable for your purpose. To also run the _rsync_ command, use the `-x` switch.


### Creating and Updating Hash Databases

- `lnsync update <dir>` updates the hash database, creating a new database if none exists, and rehashing all new files and those with a changed modification time (mtime).

- `lnsync rehash <dir> [<relpath>]+` forces rehashing the files specified by paths relative to the root `dir`.

- `lnsync subdir <dir> <relsubdir>` updates the hash database at `relsubdir` using any hash value already present in the hash database for `dir`.

- `lnsync mkoffline <dir>` incorporates file tree structure into the file hash database at the root of `dir`.

- `lnsync rmoffline <databasefile>` removes file tree structure from a hash database.

- `lnsync cleandb <dir>` removes outdated entries and re-compact the database.

### Obtaining Information

- `lnsync lookup <tree> [<relpath>+]` returns (either from db or by recomputing) the hash value for the files, where `tree` may be a a directory or an offline tree.

- `lnsync cmp <tree1> <tree2>` recursively compares two file trees.

- `lnsync fdupes [-h] [<tree>]+` finds files duplicated anywhere on the given trees.

- `lnsync onall [<tree>]+` finds files with at least one copy on each tree.

- `lnsync onfirstonly [<tree>]+` finds files on the first tree which are not duplicated anywhere on any other tree.

- `lnsync check [<tree>] [<path>]*` recomputes hashes for given files and compare to the hash stored in the database, to check for changes/bitrot.

## Limitations and Future Developments

### Caveats and Limitations

- Tested only on Linux

- Works only on locally mounted directories.

- Considers only readable files and readable+accessible directories. All other objects, including pipes, softlinks, special devices are ignored.

- Filenames are not required to be valid UTF8, to accommodate older archives.

- Minimum support for case-insensitive, case-preserving file systems like vfat: if a target file name differs from source match in case only, target is not updated.

### Possible Improvements

- Take advantage of multiple CPUs to hash multiple files in parallel.

- Match algorithm works well in simple cases, but could be further developed and optimized.

- Support for excluding/including files by regexp.

- Support checking for duplicates by actual content, not just hash.

- Update target mtimes from source.

- Sort duplicates, e.g. by name or mtime.

- Port on Python 3.

- Detect renamed directories to obtain a more compact sync schedule.

- Store database and config files in a .lnsync directory.

