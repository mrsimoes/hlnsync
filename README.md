## Overview

_lnsync_ provides sync-by-content of local file trees (including hard link syncing), plus other search and compare features.

###  Features

The main feature is partial, one-way sync of local directories by renaming, linking and delinking only, without copying or deleting file content data.

This alows quick target replication of arbitrary source renaming/linking/delinking (but not deleting), and may be used as a preprocessing step for full a sync tool such as _rsync_.

Other operations include finding duplicate files, checking for file content changes, and listing all hard links to a file.

Files may be included or excluded using glob pattersn, much like rsync.

Configuration is possible via very flexible config files.

### Offline Trees

There is also an option to save the file structure (not file contents, and not all attributes) under a directory in a combined hash/metadata database. These _offline trees_ can be used in most _lnsync_ commands in place of a local directory, e.g. as the source in a sync command, to reorganize a target tree according to a certain source pattern.

### Hashes

Files are identified by their content hash, using xxHash (a fast, non-cryptographic hash function). Both 32-bit and 64-bit hashes xxHash functions are available, with 32-bit as default. Custom hashing functions are supported.

To avoid recomputation, hash values are stored in local databases, one single file per tree, by default matching `lnsync-[0-9]+.db`. Only one such file should exist at each location. (A different naming pattern or an altogether different file location may be specified.)

File modification times are used to detect stale hash values. Modification times are not synced to the target.

Hash databases are ignored by _lnsync_ operations. Care should be taken not to overwrite them when syncing with other tools.

## Files, File Paths, and Hard Links

On most file systems, the same _file_ may be reached by multiple _file paths_, also called aliases, or _hard links_. If there is a single hard link to a file, removing it deletes the file.

Operations are on files, not file paths. E.g., if a file has two hard links, then by itself it does not count as a duplicate.

### Files Handled

Files which cannot be read are skipped. File ownership and permissions are otherwise ignored.

Symbolic links and any other file system objects are ignored.

### Directories

When syncing, directories are created as needed on the target, and also empty target directories not on the source are removed.

## Installing

Install the latest version from the PyPI repository with `pip install --user -U lnsync` or else clone the repo with `git clone https://github.com/mrsimoes/lnsync.git` and then run `python setup.py install`.

### Alternatives for Linux

Some of the many tools for syncing with rename detection:

- There are patches for _rsync_ (see --detect-renamed) that provide renaming on the target, relying on file size and modification time, for matching files in nearby directories. _rsync_ can preserve hard links and sync with remote rsync instances.

- [rclone](https://rclone.org/) provides sync-by-rename for local as well as an amazing array of remote clients. It allows caching file hashes, after some configuration. However, it does not preserve hard links.

- [unison](https://www.cis.upenn.edu/~bcpierce/unison/) and [bsync](https://github.com/dooblem/bsync) provide network syncing with rename detection, but do not preserve hard links.

- _git_ itself identifies and stores files by content, and has been adapted for syncing.

- Support in modern file systems (e.g. btrfs) for snapshots may be adapted for syncing.

In addition to syncing, _lnsync_ allows using the file hash database to search for files according to a variety of criteria.

## Usage Scenarios

### Syncing

If your photos are at `/home/you/Photos` and its backup is at `/mnt/disk/Photos`, then `lnsync sync /home/you/Photos /mnt/disk/Photos` will sync the target. For a dry run, use the `-n` switch.

After syncing, two database files are created, one at the source `/home/you/Photos` and another at the target `/mnt/disk/Photos`. File hashes are computed, as needed, and stored in those files. The database filenames include a random suffix, to help avoid accidental overwriting when syncing with a tool other than _lsync_.

To obtain an _rsync_ command that will complete syncing, skipping `lnsync` database files, run `lnsync rsync /home/you/Photos /mnt/disk/Photos`. If the `rsync` options provided are suitable, run the command again with the `-x` switch to execute. Alternatively, run `lnsync syncr /home/you/Photos /mnt/disk/Photos` to complete those two steps in one go.

Finally, to compare source and target, run `lnsync cmp /home/you/Photos /mnt/disk/Photos`.

### Other Operations

To find duplicate files, run `lnsync fdupes /home/you/Photos`. Use `-z` to compare by size only.

Use `-H` to treat hard links to the same file as distinct. If this option is not given, for each multiple-linked with other duplicates, a path is arbitrarily picked and printed.

To find all files in Photos which are not in the backup (under any name): `lnsync onfirstonly /home/you/Photos /mnt/disk/Photos`. To find all files with jpg extension, `lnsync search /home/you/Photos "*.jpg"`.

To have any operation on a subdir of `/home/you/Photos` use the hash database at `/home/you/Photos`, include `root=/home/you/Photos` under section `/home/you/Photos/**` of your config file. (See Configuration Files below.)

For example, to sync the subdir `/home/you/Photos/Best` to another target, using the hash database at `/home/you/Photos`: `lnsync sync /home/you/Photos/Best --root=/home/you/Photos /mnt/eframe/`.

## Custom Hash Functions

A custom hasher is any executable that takes a single argument and outputs an hash value as a 64-bit unsigned integer (in decimal). To set a custom hasher: `--hasher=<EXECUTABLE>`. It may be advisable to also change the database prefix or location.

As an example, if `hashmp3.sh` computes the hash of only the sound content of an mp3 (and not any included metadata), then the following `lnsync-mp3.cfg` config file may be used to find duplicate mp3 content:

```
[DEFAULT]
    dbprefix = lnsync-mp3
    hasher = ~/bin/hashmp3.sh

[**]
    only_include = *.mp3
```

The second section applies the `--only-include="*.mp3"` to ALL online tree locations.

Invoke this mode with `lnsync --config <PATH TO lnsync-mp3.cfg> <COMMAND> [<ARG> ...]`.

## Command Reference

All _lnsync_ commands are `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

### Specifying the Database Location

By default, the database file corresponding to an online file tree is the unique file located in that directory and with basename matching `<PREFIX>-[0-9]+.db`.

To specify another prefix for all following online file trees, `--dbprefix <PREFIX>`.

To specify a different database directory possibly for all online file trees where to look for the database file, `--dbrootdir DBDIR`. Each online file tree corresponding to a subdir of DBDIR will use the database file at DBDIR.

To specify the database file for the following online file tree, `--dblocation FILEPATH`.

### Syncing

- `sync [options] <source> <target>` syncs a target dir from a source dir (or offline tree).

First, target files are matched to source files. Each matched target file is associated to a single source file. If either file system supports hard links, a file may have multiple pathnames. _lnsync_ will not complain if the match is not unique or some files are not matched on either source and/or target.

For each matched target file, its pathnames are made to match those of the corresponding source file, by renaming, deleting, or creating hard links. New intermediate subdirectories are created as needed on the target and directories which become empty on the target are removed.

 - `-n` Dry-run, just show which operations would be performed.

 - `-z` Match files by size only. In this case, hash databases are not created or updated.

 - `-M=<size>` Excludes all files larger than <size>, which may be given in human form, e.g. `10k`, `2.1M`, `3G`.

 - `--exclude <glob_pattern> ... <glob_pattern>` Exclude source files and directories by glob patterns. There is a corresponding `--include` and these are interpreted as in `rsync --exclude <pattern> source/ target` (beware, compatability has not been fully tested).

  - A file or directory is excluded if it matches an `exclude` pattern before matching any `include` pattern.

  - An initial slash anchors a pattern to the corresponding file tree root and a trailing slash means the pattern applies only to directories. 

  - Each `--exclude` and `--include` option applies to all file trees in the command.

 - `--once-exclude=<pattern>` and `--once-include=<pattern>` apply only to the following file tree.

 - `--only-include <PATTERN> ...` is equivalent to `--include="*/" --include <PATTERNS> ... --exclude="*"`.

 - `--root <DIR>` For each online location that is a subdir of <DIR>, use the hash database at <DIR> to read and update. If several <DIR> in the command line are suitable for a location, use the last one given.
 
- `rsync [options] <tree> <dir> [rsync-options]` Prints an _rsync_ command that would sync target dir from source, skipping _lnsync_ database files. Source may be a dir or an offline tree. Check the default _rsync_ options provided are what you want. To also run the _rsync_ command, use the `-x` switch.

- `syncr` This convenience command is like `sync`, but follows it by executing the command created by `rsync` just above.

### Creating, Updating, and Accessing the Hash Database

- `update <dir>` Update the hash database, creating a new database if none exists, and rehashing all new files and those with a changed modification time (mtime). Accepts `--exclude=<pattern>` options.
- `rehash <dir> [<relpath>]+` Force rehashing specified files and subdirs.
- `subdir <dir> <relsubdir>` Update the database at `relsubdir` using any hash value already present in the hash database for `dir`.
- `mkoffline <dir> <outputfile>` Update database at `dir` and create corresponding offline database at `outputfile`. Use `-f` to force overwriting the output file.
- `cleandb <dir>` Remove outdated entries and re-compact the database.
- `lookup <tree> [<relpath>+]` Returns (either from db or by recomputing) the hash value for the files, where `tree` may be a a directory or an offline tree.

### Finding Files and Paths

These commands operate on files, as opposed to paths. To instead operate on paths, use the `--hard-links` switch on these commands. To operate on files, but print all hard links, instead a of picking one, use  `--all-links`.
 
- `cmp <tree1> <tree2>` Recursively compares two file trees. Accepts `--exclude=<pattern>` .
- `fdupes [-h] [<tree>]+` Find files duplicated anywhere on the given trees.
- `onall [<tree>]+`, `lnsync onfirstonly [<tree>]+`, `lnsync onlastonly [<tree>]+` Find files as advertised. Some options: `-M` prunes by maximum size; `-0` prunes empty files; `-1` prints each group of files in a single line, separated by spaces and with escaped backslashes and spaces, like `fdupes`; `-s` sorts output by size.
- `search <tree> [<globpat>]+` Find files one of whose relative paths matches one of the given glob patterns (which are as in `--exclude`).

### Other Commands
- `check [<tree>] [<path>]*` Recompute hashes for given files and compare to the hash stored in the database, to check for changes/bitrot.

### Configuration Files

Optional command-line arguments are read from an INI-style configuration file. (The format is not very suitable to store default options, at most one entry per key.)

Unless otherwise specified, the config file is searched at at `./lnsync.cfg`, `~/lnsync.cfg`, or `~/.lnsync.cfg` location may nbe specified. By default, it 

Entries are `key = value`, the `key` can match the short or long option name (`n` or `dry-run`).

For options taking taking multiple values (e.g. `exclude`), separate them by line breaks.

Options in the `DEFAULT` section apply as if given at the beginning of command line input.

For each location in the command line, options are read from all sections whose name glob-matches that location (directory or offline file). These options apply as if given just before that location.

For example, to have an option applied to all locations, include it in a section`[**]`.

To specify another configuration file altogether, `--config FILENAME`. To not load any config file: `--no-config`.

## Origin, Status, and Future Development

This package started as a learning project. I've found it useful enough to polish for publication, but as with any work in progress, it should be used with adequate caution.

Feedback, suggestions, comments, and corrections are very welcome.

You can support this project with bitcoin at [17HS828pkQMiXZGy7UNbA49TYCz7LAQ2ze](bitcoin:17HS828pkQMiXZGy7UNbA49TYCz7LAQ2ze?amount=.001).

This program comes with ABSOLUTELY NO WARRANTY. This is free software, and you are welcome to redistribute it under certain conditions. See the GNU General Public Licence v3 for details.

### Caveats and Limitations

- Linux only.

- Local directories only.

- Depends on mtime to detect file content changes.

- If source files A, B, C (with pairwise distinct contents) are renamed on target in a cycle to C, A, B, sync is currently not supported.

- Only readable files and readable+accessible directories are read. Other files and dirs, as well as symlinks, pipes, special devices are ignored.

- Minimal support for case-insensitive but case-preserving file systems like vfat: if a target file name differs from source match in case only, target is not updated.

### Release Notes

- v0.7.2: Support for 64-bit hashing functions. Option `--dbdir` renamed to `--dbrootdir`. Internal changes: use xxhash over pyhashxx, refactoring, bug fixes.
- v0.7.0: Custom hashing functions, better command line argument parsing, custom db location, bug fixes.
- v0.6.1: Thread improvements and bug fixes.
- v0.6.0: Threaded hashing and tree scanning for much better performance. Internal refactoring.
- v0.5.3: New `syncr`. Changed `mkoffline` syntax. More output options (`--all-links`). Hard link-aware `check` and `cmp`, improved `search`. 
- v0.5.2: Search files by file path glob pattern. Multiple patterns on --exclude. More powerful configuration files. `--root` now allowed in `mkoffline` and `rehash`. Major rewrite of the command line and config file parsers. Optimize onfirstonly and sync to do less hashing. Fix bugs in `--root`, `cmp`, `check`, and more. Wildcards in config section names.
- v0.4.0: Drop Python 2 compatibility. Add config files. Bug fixes.
- v0.3.8: Less hashing on `onfirstonly`. Sort file search output by size. Adjust user output levels.
- v0.3.7: Bug fix on reading offline trees. Change output levels and some messages.
- v0.3.6: New: --include and --include-once options. Bug fix: wrong exit code.
- v0.3.5: Bug fix: not excluding dirs in offline mode.
- v0.3.3: Python 3 support.
- v0.3.2: New --root option to allow reading and updating a root tree database when querying subtrees.
- v0.3.0: Exclude files by glob pattern in sync and other commands. Better terminal output. Major code overhaul.
- v0.1.9: Improved sync algorithm. Remove directories left empty after sync.
- v0.1.0: Initial version.

### Possible Improvements

- Support the newer xxhash3 hashes, or other, including 128-bit hashes.
- Better configuration file format.
- More parallel hashing, multiprocessing instead of threads.
- More output options, e.g. sort by name or mtime.
- Make `--include` and `--exclude` patterns more compatible with `rsync`.
- Store Unicode file names in offline database to support other operating systems. Currently stored as-is.
- Detect renamed directories for a compact sync schedule.
- Partial hashes for quicker comparison of same-size files.
- Check for duplicates by actual content, not just content hash.
- Update target mtimes.
- Support argparsecomplete.