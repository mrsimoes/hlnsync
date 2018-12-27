# lnsync

## Introduction

_lnsync_ provides unidirectional local file tree sync by content, with support for hardlinks (multiple pathnames per file).

### Purpose

Suppose file trees at local source and target directories are in sync. If files are renamed/moved in the source tree, _lnsync_ syncs by renaming/moving on the target, rather than deleting and recopying from the source. In general, the source hardlink structure is replicated on the target.

### Operation

_lnsync_ matches files by content (by using a file hash database kept at the root of the file tree) and adds/renames/removes links (pathnames) on the target to replicate the source hardlink structure. No new files are ever created on the target and the last link for a file is never removed on the target. Therefore, _lnsync_ may be used as a preprocessing step for other sync tools, such as _rsync_.

Using those file hashes, _lnsync_ can also quickly finding duplicate files (like _fdupes_), compare file trees, and check files for changes/bitrot.

### Alternative Solutions

Numerous tools already exist for syncing, and some are suitable for the purpose described above:

- Patches for _rsync_ have been published that provide some of this functionality (see --detect-renamed), relying mostly on file size and modification time, or matching files in nearby directories. Unlike _rsync_, _lnsync_ does not offer a network client/server protocol.

- [bsync](https://github.com/dooblem/bsync) provides network syncing with moved files detection, but does not seem to handle hardlinks.

- _git_ itself is based on syncing by content, and has been adapted for the above purpose.

- Some modern file systems with support for snapshots (e.g. btrfs) may provide a basis for this functionality, while _lnsync_ is meant to be used file systems in common use today, including ext2/3/4, vfat, and ntfs.

### Hash Databases and Offline File Trees

The hash database file is created at the root of the file tree, by default with a randomly chosen basename matching `lnsync-[0-9]+.db`. Only one such file may exist there. These files are ignored by all _lnsync_ operations, and they should not be copied from source to target when using other sync tools.

_lnsync_ also offers the ability to incorporate the file tree structure into the hash database, so that the database file by itself may used as an _offline file tree_, in place of the original directory. For example, a directory may be synced from an offline file tree.

## Using _lnsync_

All _lnsync_ commands are of the form `lnsync [<global-options>] <command> [<cmd-options>] [<cmd-parameters>]`.

Brief descriptions follow.

### Creating and Updating Hash Databases

To update the hash database, creating a new database if none exists, and rehashing all new and modified files:

- `lnsync update <dir>`

To force rehash of files specified by paths relative to the root `dir`:

- `lnsync rehash <dir> [<relpath>]+` 

To update the hash database at `relsubdir` using any hash value already present in the hash database for `dir`.

- `lnsync subdir <dir> <relsubdir>` 

To incorporate file tree structure into the file hash database at the root of `dir`:

- `lnsync mkoffline <dir>`

To remove file tree structure from a hash database:

- `lnsync rmoffline <databasefile>`

To remove outdated entries and re-compact the database.

- `lnsync cleandb <dir>` 

### Syncing

To sync a target dir from a source dir (or offline tree):

- `lnsync sync [-d] <source> <target>`.

First target files are matched to source files. Each matched target file is associated to a single source file. If either file system supports hardlinks, a file may have multiple pathnames (hardlinks).

Then for each matched target file, its pathnames are set to match those of the corresponding source file, by
renaming and deleting its hardlinks, or creating new hardlinks for it. New intermediate subdirectories are created as needed on the target; directories that become empty on the target are not removed. If distinct source files A, B, C are renamed B, C, A on the target, undoing this cycle is currently not supported.

To print an rsync command that will sync target dir from source dir or offline tree:

- `lnsync rsync <tree> <dir>` .


### Obtaining Information

To get the hash value for files:

- `lnsync lookup <tree> <relpath>` Print the hash value for the given file. `tree` may be a a directory or an offline database.

To compare two file trees:

- `lnsync cmp <tree1> <tree2>`

To check for duplicate files:

- `lnsync fdupes [<tree>]+` Find files files duplicated anywhere on the given trees.

- `lnsync onall [<tree>]+` Find files with at least one copy on each tree.

- `lnsync onfirstonly [<tree>]+` Find files on the first tree which are not duplicated anywhere on any other tree.

For the above commands, the `-h` flag will make _lnsync_ handle hardlinks to the same file as duplicates within each tree.

Finally, _lnsync_ provides for recomputing file hash signatures and comparing to the previously stored signature, to check for changes/bitrot.

- `lnsync check [<tree>] [<path>]*` Print files that exist on the first tree and on no other tree.

## Limitations and Future Developments

### Caveats and Limitations

- Tested only on Linux.

- Works only on locally mounted directories.

- Considers only readable files and readable+accessible directories. All other objects, including pipes, softlinks, special devices are ignored.

- Filenames are not required to be valid UTF8, to accomodate older archives.

- Bare minimum support for case-insensitive, case- preserving file systems like VFAT. E.g.: if a target file name differs from its source match in casing only, no change is applied.

- mtimes are ignored.

### Possible Improvements

- Take advantage of multiple CPUs to hash multiple files in parallel.

- Matching algorithm works well in simple cases, but could be further developed and optimized.

- Support for excluding/including files by regexp.

- No support for checking duplicates by actual content, not just hash.

- Rollback changes to target in case of failure.

- Sort duplicates, e.g. by name or mtime.

- Support for Python 3.

- Detect renamed directories to present a shorter schedule of target sync operations.
