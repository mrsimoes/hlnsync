# Overview

_lnsync_ provides unidirectional sync-by-rename of local directories with support for hard links, but with no copying or deleting file data.

Files are identified by content, by file hash. Using those hashes, other features are provided, such as finding duplicate files.

File renaming, moving, and linking in the source can be quickly replicated in the target.

_lnsync_ may be used as a preprocessing step for other sync tools such as _rsync_, which do copy and delete file data.

##  Hard Link Support

On some file systems (e.g. ext3/4, NTFS, btrfs), the same file may be reached by multiple file paths, which function as aliases.

A new path created for an existing file is a _hard link_, but all such aliases are on an equal footing, so each may be called a hard link. Removing the last hard link to a file means deleting that file.

Arbitrary linking/delinking (but not deleting) in the source can also be easily easily replicated in the target, so long as the target supports linking.

##  Syncing and the Hash Database

_lnsync_  makes a best-effort to sync the target to the source by only renaming files and generally creating/removing hard links on the target.

Empty directories on the target which do not exist on the source are also removed.

File content is compared using xxHash (a fast, non-cryptographic hash function). Hash values are stored in a single-file database at the top-level directory of each file tree.

This hash value database is a single file at the top directory of processed trees, with basename matching `lnsync-[0-9]+.db`. (Only one such file should exist there.) These files are ignored by all _lnsync_ operations, and care should be taken not to overwrite them when syncing with other tools.

File modification times are used to detect stale hash values. Modification times are not synced. File ownership and permissions are ignored: files which cannot be read are skipped.

Symbolic links are skipped.

## Offline File Trees

_lnsync_ can save the file tree structure (with no file content) of a local directory to a single-file database. These offline trees can be used in most _lnsync_ commands in place of an local directory, e.g. as the source in a sync command.

## File Searching

Using file hashes, _lnsync_ can also find duplicate files (like _fdupes_), and more generally find files according to the which file trees they appear on, compare file trees, and check for file content changes/bitrot.

It is also possible to search files by pathname.

## Installing

Install the latest version from the PyPI repository with `pip install -U lnsync` or clone the repo with `git clone https://github.com/mrsimoes/lnsync.git` and run `python setup.py install`.

## Alternative Sync Solutions for Linux

Some of the many tools for syncing with rename detection:

- There are patches for _rsync_ (see --detect-renamed) that provide renaming on the target, relying on file size and modification time, for matching files in nearby directories. _rsync_ can preserve hard links and sync with remote rsync instances.

- [rclone](https://rclone.org/) provides sync-by-rename for local as well as an amazing array of remote clients. It allows caching file hashes, after some configuration. However, it does not preserve hard links.

- [unison](https://www.cis.upenn.edu/~bcpierce/unison/) and [bsync](https://github.com/dooblem/bsync) provide network syncing with rename detection, but do not preserve hard links.

- _git_ itself identifies and stores files by content, and has been adapted for syncing.

- Support in modern file systems (e.g. btrfs) for snapshots may be adapted for syncing.

In addition to syncing, _lnsync_ allows using the file hash database to search for files according to a variety of criteria.

# Example Usage

If you have your photo archive at `/home/you/Photos` and your backup is at `/mnt/disk/Photos`, run `lnsync sync -n /home/you/Photos /mnt/disk/Photos` for a dry-run, to see which sync operations would be performed. To sync, ommit the `-n` switch.

You will notice that two database files are created, one at the source and another at the backup directory. File hashes are cmoputed as needed and then stored in these files. The database filenames includes a random suffix to avoid accidental overwriting when syncing with a tool other than _lsync_.

To quickly obtain an _rsync_ command that will complete syncing, skipping `lnsync` database files, run `lnsync rsync /home/you/Photos /mnt/disk/Photos`. To also run this command, use the `-x` switch. Make sure the `rsync` options provided by this command are suitable for you.

Finally, to check the target is in-sync by recursively comparing it to source, run `lnsync cmp /home/you/Photos /mnt/disk/Photos`.

To find duplicate files on the Photos directory, run `lnsync fdupes /home/you/Photos`. Use `-z` to compare by size only. Use `-H` to count different hard links to the same file as distinct files. If this option is not given, for each multiple-linked with other duplicates, a path is arbitrarily picked and printed.

To find all files in Photos which are not in the backup (under any name): `lnsync onfirstonly /home/you/Photos /mnt/disk/Photos`. 

To find all files with jpg extension, `lnsync search /home/you/Photos "*.jpg"`.

To sync the subdir `/home/you/Photos/Best` to your digital picture frame, using the hash database at `/home/you/Photos`: `lnsync sync /home/you/Photos/Best --root=/home/you/Photos /mnt/eframe/`.

To have any operation on a subdir of `/home/you/Photos` use the hash database at `/home/you/Photos`, include the option `root=/home/you/Photos` under section `/home/you/Photos/**` of your config file. (See Configuration Files below.)

# Command Reference

All _lnsync_ commands are `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

## Syncing

- `lnsync sync [options] <source> <target>` syncs a target dir from a source dir (or offline tree).

 - First, target files are matched to source files. Each matched target file is associated to a single source file. If either file system supports hard links, a file may have multiple pathnames. _lnsync_ will not complain if the match is not unique or some files are not matched on either source and/or target.

 - For each matched target file, its pathnames are made to match those of the corresponding source file, by renaming, deleting, or creating hard links. New intermediate subdirectories are created as needed on the target and directories which become empty on the target are removed.

 - `-z` Match files by size only. In this case, hash databases are not created or updated.

 - `-M=<size>` Excludes all files larger than <size>, which may be given in human form, e.g. `10k`, `2.1M`, `3G`.

 - `-n` Dry-run, just show which operations would be performed.

 - `--exclude <glob_pattern> ... <glob_pattern>` Exclude source files and directories by glob patterns. There is a corresponding `--include` and these are interpreted as in `rsync --exclude <pattern> source/ target` (beware, compatability has not been fully tested).
 -- A file or directory is excluded if it matches an `exclude` pattern before matching any `include` pattern.
 -- Each `--exclude` and `--include` option applies to all file trees in the command.
 -- Some commands accept `--exclude-once=<pattern>` and `--include-once=<pattern>`, which apply only to the next file tree following the switch and gain precedence over global patterns.
 -- An initial slash anchors the pattern to the corresponding file tree root.
 -- A trailing slash means the pattern applies only to directories. 

- `lnsync rsync [options] <tree> <dir> [rsync-options]` Prints an _rsync_ command that would sync target dir from source, skipping _lnsync_ database files. Source may be a dir or an offline tree. Check the default _rsync_ options provided are what you want. To also run the _rsync_ command, use the `-x` switch.

## Creating, Updating, and Accessing the Hash Database
- `lnsync update <dir>` Update the hash database, creating a new database if none exists, and rehashing all new files and those with a changed modification time (mtime). Accepts `--exclude=<pattern>` options.
- `lnsync update <dir>` Update the hash database, creating a new database if none exists, and rehashing all new files and those with a changed modification time (mtime). Accepts `--exclude=<pattern>` options.
- `lnsync rehash <dir> [<relpath>]+` Force rehashing files specified by paths relative to the root `dir`.
- `lnsync subdir <dir> <relsubdir>` Update the database at `relsubdir` using any hash value already present in the hash database for `dir`.
- `lnsync mkoffline <dir> <outputfile>` Update database at `dir` and create corresponding offline database at `outputfile`.
- `lnsync cleandb <dir>` Remove outdated entries and re-compact the database.
- `lnsync lookup <tree> [<relpath>+]` Returns (either from db or by recomputing) the hash value for the files, where `tree` may be a a directory or an offline tree.

## Finding Files

- `lnsync cmp <tree1> <tree2>` Recursively compares two file trees. Compares files at each path, does not compare the hard link structure. Accepts `--exclude=<pattern>` options.
- `lnsync fdupes [-h] [<tree>]+` Find files duplicated anywhere on the given trees.
- `lnsync onall [<tree>]+`, `lnsync onfirstonly [<tree>]+`, `lnsync onlastonly [<tree>]+` Find files as advertised. Some options: `-M` prunes by maximum size; `-0` prunes empty files; `-1` prints each group of files in a single line, separated by spaces and with escaped backslashes and spaces, like `fdupes`; `-s` sorts output by size. Use `-H` to consider multiple links to the same file as distinct files; if this option is not used, print a single, arbitrarily picked path for each multiple-linked file found to satisfy the condition of the command.
- `lnsync search <tree> [<globpat>]+` Find files one of whose relative paths matches one of the given glob patterns (which are as in `--exclude`).

## Other Commands
- `lnsync check [<tree>] [<path>]*` Recompute hashes for given files and compare to the hash stored in the database, to check for changes/bitrot.

## Configuration Files

Optional command-line arguments are read from any INI-style configuration files at `./lnsync.cfg`, `~/lnsync.cfg`, and `~/.lnsync.cfg`. In its `DEFAULT` section, in entries of the form `key = value`, the `key` can match the short or long option name (with intermediate minus replaced by underscores, e.g. `n` or `dry_run`).

For options taking no values, the `value` can be either empty or be a count of how many times to apply that option. For options  taking more than one value (e.g. `exclude`), multiple values may be separated by line breaks. values may be set there for command-line options `sort`, `hardlinks`, `sameline`, `dbprefix`, `bysize`, `maxsize`, `skipempty`, `dry_run`.

The `DEFAULT` section may contain `exclude` and `include` options, applying to all trees. Sections whose name glob-matches a directory (or offline tree file) may contain `exclude` and `include` options that applied only to that location, before all other exclude/include patterns.

To specify another configuration file altogether, `--config FILENAME`. To avoid all config files, `--no-config`.

# Origin, Status, and Future Development

This package started as a Python learning project. I've found it useful enough to polish for publication, but as with any work in progress, it should be used with adequate caution.

Feedback, suggestions, comments, and corrections are very welcome.

You can support this project with bitcoin at [17HS828pkQMiXZGy7UNbA49TYCz7LAQ2ze](bitcoin:17HS828pkQMiXZGy7UNbA49TYCz7LAQ2ze?amount=.001).

This program comes with ABSOLUTELY NO WARRANTY. This is free software, and you are welcome to redistribute it under certain conditions. See the GNU General Public Licence v3 for details.

## Caveats and Limitations
- Works only on locally mounted directories, no support whatsoever for remote servers.
- Depends on mtime to detect file content changes.
- If source files A, B, C (with pairwise distinct contents) are renamed on target in a cycle to C, A, B, sync is currently not supported.
- Only readable files and readable+accessible directories are read. Other files and dirs, as well as symlinks, pipes, special devices are ignored.
- Minimal support for case-insensitive but case-preserving file systems like vfat: if a target file name differs from source match in case only, target is not updated.
- Supports Linux only.

## Possible Improvements

- argparsecomplete support
- Find all hard links (aliases) to a given file.
- Make `--include` and `--exclude` patterns more compatible with `rsync`.
- Extend `cmp` to take hard links into account and generally extend each command to either work on files or paths.
- Filenames are NOT converted to Unicode. To allow using offline database across systems, conversion is required.
- Detect renamed directories to obtain a more compact sync schedule.
- Use coroutines to scan trees and hash files in parallel.
- Support partial hashes for quicker comparison of same-size files.
- Further optimize the sync algorithm, though it has been working well in practice.
- Support for checking for duplicates by actual content, not just hash.
- Update target mtimes from source.
- Allow more output sorting options, e.g. by name or mtime.

## Release Notes

- Version 0.5.1
 - Search files by file path glob pattern.
 - Multiple patterns on --exclude.
 - More powerful configuration files.
 - `--root` now allowed in `mkoffline` and `rehash`.
 - Major rewrite of the command line and config file parsers.
 - Optimize onfirstonly and sync to do less hashing.
 - Fix bugs in `--root`, `cmp`, `check`, and more.
 - Wildcards in config section names.
- Version 0.4.0
 - Drop Python 2 compatibility.
 - Add config files.
 - Bug fixes.
- Version 0.3.8
 - Less hashing on `onfirstonly`.
 - Sort file search output by size.
 - Adjust user output levels.
- Version 0.3.7
 - Bug fix on reading offline trees.
 - Change output levels and some messages.
- Version 0.3.6
-- New: --include and --include-once options.
 - Bug fix: wrong exit code.
- Version 0.3.5
 - Bug fix: not excluding dirs in offline mode.
 - Version 0.3.3
 - Python 3 support.
- Version 0.3.2
 - New --root option to allow reading and updating a root tree database when querying subtrees.
- Version 0.3.0
 - Exclude files by glob pattern in sync and other commands.
 - Better terminal output.
 - Major code overhaul.
- Version 0.1.9
 - Improved sync algorithm.
 - Remove directories left empty after sync.
- Version 0.1
 - Initial version.
