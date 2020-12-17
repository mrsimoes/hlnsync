#!/usr/bin/env python

import lnsync_pkg.printutils as pr

class GroupedFileListPrinter:
    """
    Output filepaths in groups, either separated by empty lines or by empty
    spaces, sorted or not.
    """
    def __init__(self, hardlinks, alllinks, sameline, sort):
        """
         - sameline: if True, filenames in each group are printed in the same
        line, separated by spaces, with filename spaces and backslashes escaped;
        other print one file per line, with an empty line separating groups.
        - hardlinks: if True, print all aliases for each file as if they were
        different files; if False, print for each file a single path alias,
        arbitrarily chosen.
        - alllinks: if True (and assuming hardlinksis False), print all links
        to each file.
        """
        self.hardlinks = hardlinks
        self.alllinks = alllinks
        self.sameline = sameline
        self.sort = sort
        if self.sort:
            self.groups = []
        self._output_group_linebreak = False # Not before first group.
        if self.sameline:
            self._built_line = None

    def add_group(self, located_files):
        """
        located files is {tree_1: [files_1],... {tree_k, [files_k]}
        """
        if self.sort:
            self.groups.append(located_files)
        else:
            self._print_group(located_files)

    def flush(self):
        if self.sort:
            def get_size(located_files):
                for file_list in located_files.values(): # Use any.
                    return file_list[0].file_metadata.size
            self.groups.sort(key=get_size)
            for g in self.groups:
                self._print_group(g)

    def _print_group(self, located_files):
        if self.sameline:
            self._built_line = ""
        else:
            if self._output_group_linebreak:
                pr.print("")
            else:
                self._output_group_linebreak = True
        for tree, fobjs in located_files.items():
            for fobj in fobjs:
                self._print_file(tree, fobj)
        if self.sameline:
            pr.print(self._built_line)

    def _print_file(self, tree, fobj):
        if self.sameline:
            if self._built_line != "":
                self._built_line += " "
            for k, relpath in enumerate(fobj.relpaths):
                if k == 0:
                    include, prefix = (True, "")
                elif self.hardlinks or self.alllinks:
                    include, prefix = (True, " ")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    # Escape single backslashes.
                    pr_path = pr_path.replace("\\", "\\\\")
                    pr_path = pr_path.replace(r" ", r"\ ")
                    self._built_line += prefix + pr_path
        else:
            for k, relpath in enumerate(fobj.relpaths):
                if k == 0:
                    include, prefix = (True, "")
                elif self.hardlinks:
                    include, prefix = (True, "")
                elif self.alllinks:
                    include, prefix = (True, " ")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    pr.print(prefix, pr_path, end="\n")
