# lnsync

## Introduction

_lnsync_ provides unidirectional local file tree sync with rename detection and support for hardlinks.

This package started as a Python learning project. I've found it useful enough to polish for publication, but it underwent limited testing and therefore should be used with caution. All comments, corrections, and suggestions are welcome.

### Purpose and Operation

When files are renamed/moved in the source file tree, _lnsync_  makes a best-effort to sync target from source by only renaming files and generally creating/removing hard links, but without deleting or copying file data. (It also removes directories which have become extraneous.) It may be used as a preprocessing step for other sync tools, such as _rsync_.

File content is compared by size and content hash, which _lnsync_ stores in single-file SQLite3 database at the top-level directory of each file tree. Using those hashes, _lnsync_ can also find duplicate files (like _fdupes_), compare file trees, and check for changes/bitrot.

File modification times are used to detect stale hash values and are not synced. File ownership and permissions are ignored: files which cannot be read are skipped.

On many file systems (e.g. ext3/4, NTFS, btrfs), a file may be reached via multiple file paths, which function as aliases. A new path created for an existing file is a _hard link_, but all such aliases are on an equal footing, so each may be called a hard link. Removing the last hardlink to a file means deleting that file. If both source directories are in file systems supporting multiple hard links and hard links are created or removed in the source (without deleting files), then _lnsync_ can sync the target by creating and removing hardlinks there.

Again, in no case should _lnsync_ copy data from source to target or delete data from the target.

### Alternative Solutions

Many tools exist for syncing with rename detection:

- Patches for _rsync_ have been published (see --detect-renamed), relying on file size and modification time, or matching files in nearby directories. Unlike _rsync_, _lnsync_ does not offer a network protocol.

- unison and [bsync](https://github.com/dooblem/bsync) provide network syncing with rename detection, but do not support hardlinks.

- _git_ itself is based on syncing by content, and has been adapted for syncing.

- Support in modern file systems (e.g. btrfs) for snapshots may be adapted for syncing.

### Hash Databases and Offline File Trees

The hashing function is xxHash, a very fast, non-cryptographic hasher.

The hash value database is a single file at the top directory of processed trees, with basename matching `lnsync-[0-9]+.db`. (Only one such file should exist there.) These files are ignored by all _lnsync_ operations, and care should be taken not to sync them with other tools.

_lnsync_ also allows for _offline file trees_, which incorporate a static image of the file tree structure. These may be used place of the original directory for certain purposes: a target directory may be synced from a source offline file tree and _lnsync_ can search for duplicates common to an offline file tree and a mounted directory.

## Installation and Quickstart

### Installing

Install from the test PyPI repository with `pip install lnsync`.

Or clone the repo with `git clone https://github.com/mrsimoes/lnsync.git` and run `python setup.py install`.

### Example Usage

If you reorganize your photo collection, renaming and moving around files and directories, _lnsync_ will mirror those changes to your backup. Also, if you use hardlinks to organize your photo collection, _lnsync_ will replicate that structure to the backup. It will also delete directories which have become empty on the target and no longer exist on the source.

If you have your photo archive at `/home/you/Photos` and your backup is at `/mnt/disk/Photos`, run `lnsync sync /home/you/Photos /mnt/disk/Photos` to sync. For a dry run: `lnsync sync -n /home/you/Photos /mnt/disk/Photos`.

To quickly obtain an _rsync_ command that will complete syncing, skipping `lnsync` database files, run `lnsync rsync /home/you/Photos /mnt/disk/Photos`. To also run this command, use the `-x` switch. Make sure the `rsync` options provided by this command are suitable for you.

Finally, to check the target is in-sync by recursively comparing it to source, run `lnsync cmp /home/you/Photos /mnt/disk/Photos`.

To find duplicate files on the Photos directory, run `lnsync fdupes /home/you/Photos`. Use `-H` to count different hardlinks to the same file as duplicates. Use `-z` to compare by size only.

To find all files in Photos which are not in the backup (under any name), run `lnsync onfirstonly /home/you/Photos /mnt/disk/Photos`.


## Command Reference
All _lnsync_ commands are `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

### Syncing

- `lnsync sync [options] <source> <target>` syncs a target dir from a source dir (or offline tree).

First target files are matched to source files. Each matched target file is associated to a single source file. If either file system supports hardlinks, a file may have multiple pathnames. _lnsync_ will not complain if the match is not unique or some files are not matched on either source and/or target.

For each matched target file, its pathnames are made to match those of the corresponding source file, by renaming, deleting, or creating hardlinks. New intermediate subdirectories are created as needed on the target and directories which become empty on the target are removed.

Options:

`-z` Match files by size only. In this case, hash databases are not created or updated.

`-M=<size>` Excludes all files larger than <size>, which may be given in human form, e.g. `10k`, `2.1M`, `3G`.

`-n` Dry-run, just show which operations would be performed.

`--exclude=<glob_pattern>` Exclude source files and directories by glob pattern. Patterns are interpreted as in `rsync --exclude=<glob_pattern> source/ target`. May be repeated, and each `--exclude` option affects all locations. A trailing slash anchors the patter to the file tree root. There is no corresponding `--include` as in `rsync`. Some commands accept `--exclude-once=<pattern>` applying only to the next location following the switch.

- `lnsync rsync [options] <tree> <dir> [rsync-options]` Prints an _rsync_ command that would sync target dir from source, skipping _lnsync_ database files. Source may be a dir or an offline tree. Check the default _rsync_ options provided are what you want. To also run the _rsync_ command, use the `-x` switch.

### Creating and Updating Hash Databases

- `lnsync update <dir>` Update the hash database, creating a new database if none exists, and rehashing all new files and those with a changed modification time (mtime). Accepts `--exclude=<pattern>` options.

- `lnsync rehash <dir> [<relpath>]+` Force rehashing files specified by paths relative to the root `dir`.

- `lnsync subdir <dir> <relsubdir>` Update the database at `relsubdir` using any hash value already present in the hash database for `dir`.

- `lnsync mkoffline <dir> <outputfile>` Update database at `dir` and create corresponding offline database at `outputfile`.

- `lnsync cleandb <dir>` Remove outdated entries and re-compact the database.

### Obtaining Information

- `lnsync lookup <tree> [<relpath>+]` Returns (either from db or by recomputing) the hash value for the files, where `tree` may be a a directory or an offline tree.

- `lnsync cmp <tree1> <tree2>` Recursively compares two file trees. Accepts `--exclude=<pattern>` options.

- `lnsync fdupes [-h] [<tree>]+` Find files duplicated anywhere on the given trees.

- `lnsync onall [<tree>]+`, `lnsync onfirstonly [<tree>]+`, `lnsync onlastonly [<tree>]+` Find files as advertised. Some options: `-M` prunes by maximum size; `-0` prunes empty files; `-1` prints each group of files in a single line, separated by spaces and with escaped backslashes and spaces, like `fdupes`.

- `lnsync check [<tree>] [<path>]*` Recompute hashes for given files and compare to the hash stored in the database, to check for changes/bitrot.

## Release Notes

## Version 0.3.0

- Exclude files by glob pattern in sync and other commands.

- Better terminal output.

- Many minor improvements.

- Major code overhaul.

## Version 0.1.9

- Improved sync algorithm.

- Remove directories left empty after sync.

## Version 0.1

- Initial version.

## Limitations and Future Developments

### Caveats and Limitations

- Tested only on Linux

- Works only on locally mounted directories.

- If source files A, B, C (with pairwise distinct contents) are renamed C, A, B on the target, undoing this cycle is currently not supported.

- Considers only readable files and readable+accessible directories. Also, symlinks, pipes, special devices are ignored.

- Filenames are not required to be valid UTF8, to accommodate older archives.

- Minimal support for case-insensitive, case-preserving file systems like vfat: if a target file name differs from source match in case only, target is not updated.

### Possible Improvements

- Port to Python 3.

- Detect renamed directories to obtain a more compact sync schedule.

- Take advantage of multiple CPUs to hash multiple files in parallel.

- Support partial hashes for quicker comparison of same-size files.

- Further optimizations to sync algorithm. Currently, it's straightforward, but has worked well in most cases.

- Support for checking for duplicates by actual content, not just hash.

- Update target mtimes from source.

- Sort fdupes output, e.g. by name or mtime.

- Allow config files and maybe store database along with config files in some .lnsync-DDDD directory at the root.
