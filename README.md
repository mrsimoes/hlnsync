## Overview
_lnsync_ provides sync-by-content of local directories (including hard link syncing), plus other features related to matching and finding.

###  Features
The main feature is (partial) one-way sync of local directories by only renaming/linking/delinking on the target directory, without copying or deleting any file content from source. This may be used as a preprocessing step for a full sync tool (such as rsync).

This is achieved by maintaining a simple on-file database of file hashes for each top directory.

Files match if they have the same size and hash value.

Using file hashes, other features are provided, including: finding duplicate files, checking for file content changes, and listing all hard links to a file.

Other hashing functions are provided, including dhash, an image invariant under scaling and recoloring. This allows finding duplicate images.

### File Trees: Online and Offline

The file structure under a directory (path names, file sizes, mod dates) can be saved to a one-file database, along with the file hashes. Such a database is termed here an _offline tree_, whereas local top directories with their hash database are _online trees_,

Most _lnsync_ commands accept offline trees as well as local directory. For example, an offline tree may be used as the source to reorganize a target directory according to a certain pattern.

Via a config file, specfic options may be applied to online trees matching a glob pattern.

To describe a tree: `lnsync info <LOCATION>`.

Note that the database file of an online tree is NOT an offline tree, since it is missing the file and directory names.

### File Hashes
Files are identified by their content hash, using either 32-bit or 64-bit xxHash (a fast, non-cryptographic hash function), or other functions (see Hashing Functions below).

Hash values are stored in local databases, on a single file per tree, by default with a name matching `lnsync-[0-9]+.db`. Only one such file should exist at each location. At the time of creation, the numeric part is chosen randomly, to avoid overwritting by accident when doing a full sync. Files matching `lnsync-[0-9]+.db` as well as the hash database in use are ignored by _lnsync_ operations.

File modification times are used to detect stale hash values. Modification times are not synced to the target.

### Hashing Functions

Using `--hasher=<HASHERNAME>` selects one of the a built-in hashing function, and also changes the default file hash database basename pattern to `lnsync-<HASHERNAME>-[0-9]+.db`.

The built-in hashing functions are:

- The 32-bit and 64-bit variants of xxHash.

- Image difference hash (dhash), (Gnome) thumbnail dhash, and a thumbnail dhash that is invariant under horizontal mirroring.

When xxHash is selected, files match only if they also have the same size.

Invoking `lnsync32` or `lnsync64` selects the 32-bit and the 64-bit version of the xxHash, respectively, while keeping `lnsync-[0-9]+.db` as the file hash database location. Otherwise the two commands work the same. `lnsync` is equivalent to `lnsync32`.

External hashing functions are supported: `--external-hasher=<EXECUTABLE>`. The should take as single argument a file path and print out (in decimal) a 64-bit unsigned integer hash value. The file hash location is set to `lnsync-external-[0-9]+.db`.

Finally, the `lnsync-nopreset` entry point requires explicitly selecting the hashing function.

## Files, File Paths, and Hard Links
On most current file systems, the same _file_ may be reached via multiple _file paths_, also called _aliases_, or _hard links_. If there is a single hard link to a file, removing that link deletes the file.

`lnsync` oprerates on files, not file paths. E.g., if a file has two hard links, it does not count as a duplicate.

### Ignored File System Objects
Files which cannot be read are skipped. File ownership and permissions are otherwise ignored.

Symbolic links and any other file system objects are ignored.

As with rsync, files and directories may be included/excluded using glob patterns.

### Directories
When syncing, directories are created as needed on the target, and target directories left empty and not on the source are removed.

## Installing
Install the latest version from the PyPI repository with `pip install --user -U lnsync` or else clone the repo with `git clone https://github.com/mrsimoes/lnsync.git` and then run `python setup.py install`.

### Alternative Tools for Linux
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

To find all files in Photos which are not in the backup (under any name): `lnsync onfirstonly /home/you/Photos /mnt/disk/Photos`. To find all files with jpg extension, `lnsync search "*.jpg" /home/you/Photos`.

To have any operation on a subdir of `/home/you/Photos` use the hash database at `/home/you/Photos`, include `root=/home/you/Photos` under section `/home/you/Photos/**` of your config file. (See Configuration Files below.)

For example, to sync the subdir `/home/you/Photos/Best` to another target, using the hash database at `/home/you/Photos`: `lnsync sync /home/you/Photos/Best --root=/home/you/Photos /mnt/eframe/`.

## External Hashers
As an example of an external hasher function, if `hashmp3.sh` computes the hash of only the sound content of an mp3 (and not any included metadata), then the following `lnsync-mp3.cfg` config file may be used to find duplicate mp3 content:

    [LNSYNC_MAIN]
        external_hasher = ~/bin/hashmp3.sh
    [**]
        dbprefix = lnsync-mp3
        only_include = *.mp3

The second section applies the `--only-include="*.mp3"` to ALL online tree locations.

Invoke this mode with `lnsync --config <PATH TO lnsync-mp3.cfg> <COMMAND> [<ARG> ...]`.

## Command Reference

All _lnsync_ commands are `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

### Tree information

- `info` <location>

### Specifying the Database Location

By default, the database file corresponding to an online file tree is the unique file located in that directory and with basename matching `<PREFIX>-[0-9]+.db`.

To specify another prefix for all following online file trees, `--dbprefix <PREFIX>`.

To specify a different database directory possibly for all online file trees where to look for the database file, `--dbrootdir DBDIR`. Each online file tree corresponding to a subdir of DBDIR will use the database file at DBDIR.

To specify a directory containing database root directories to be used for any contained onlien tree, `--dbrootmounr DBMOUNTSLOCATION`. This is useful e.g. for removable media, which are all mounted at `/mnt/user`. Then `--dbrootmounr /mnt/user` will use `/mnt/user/somedrive` for the online tree at `/mnt/user/somedrive/some/subdir/`.

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
 
The criteria for equality is matching hash value (plus matching size when using xxHash).

- `cmp <tree1> <tree2>` Recursively compares two file trees. Accepts `--exclude=<pattern>`.
- `fdupes [-h] [<tree>]+` Find files duplicated anywhere on the given trees.
- `onall [<tree>]+`, `onfirstonly [<tree>]+`, `onfirstnotonly [<tree>]+`, `onlastonly [<tree>]+`, `onlastnotonly [<tree>]+` Find files as advertised. Some options: `-M` prunes by maximum size; `-0` prunes empty files; `-1` prints each group of files in a single line, separated by spaces and with escaped backslashes and spaces, like `fdupes`; `-s` sorts output by size (average size if sizes in a resulting file group aren't all the same).
- `search [<globpat>] <tree>*` Find files one of whose relative paths matches one of the given glob patterns (which are as in `--exclude`).

### Other Commands
- `check [<tree>] [<path>]*` Recompute hashes for given files and compare to the hash stored in the database, to check for changes/bitrot.

### Configuration Files

Optional command-line arguments are read from an INI-style configuration file. (The format is not very suitable to store default options: at most one entry per key, no order on keys.)

Unless otherwise specified, the config file is searched at at `./lnsync.cfg`, `~/lnsync.cfg`, or `~/.lnsync.cfg` location may nbe specified.

Entries are `key = value`, the `key` can match the short or long option name (`n` or `dry-run`).

Multiple values for an option (e.g. `exclude`) are separated by line breaks.

A `LNSYNC_MAIN` section is mandatory. Entries in this section are interpreted as if given at the beginning of command line input.

For each tree location in the command line, options are read from all sections whose name glob-matches that location (directory or offline file). These options apply as if given just before that location. Do not include these options in the GENERAL_OPTIONS section.

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

- v0.8.0: Rename hasher options. Rename main config section. New built-in hash functions: dhash and thumbnail_dhash. Bug fixes.
- v0.7.6: New `onfirstnotonly` search command. Allow mkoffline to operate on non-writeable dirs. Fixed regressions: incomplete reading of config file options, mishandling of single quotes in offline location databases.
- v0.7.5: New lnsync64 and lnsync32 entry points. Non-zero return value to indicate failed searches. Small improvements and bug fixes.
- v0.7.4: New `--dbrootmount` option.
- v0.7.3: Ignore all files matching `lnsync-*.db`. Bug fixes, notably on handling 64-bit xxhash values.
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

- Support hash values larger than 64 bits, including the newer 128-bit xxhash3, and others.
- Better configuration file format.
- More parallel hashing, multiprocessing instead of threads.
- More output options, e.g. sort by name or mtime.
- Make `--include` and `--exclude` patterns more compatible with `rsync`.
- Store Unicode file names in offline database to support other operating systems. Currently stored as-is.
- Detect renamed directories for more compact sync schedules.
- Partial hashes for quicker comparison of same-size files.
- Check for duplicates by actual content, not just content hash.
- Update target mtime.
- Support argparsecomplete.