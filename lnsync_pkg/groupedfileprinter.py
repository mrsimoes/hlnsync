#!/usr/bin/python3

# Copyright (C) 2021 Miguel Simoes, miguelrsimoes[a]yahoo[.]com
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Accept several groups of files, each group a dict {tree1: [files11,...], ... },
where each tree_k is a FileTree and each file_ki is a FileItem.

The file groups are output with appropriate formatting, either as they are
presented, or sorted once all groups have been presented.

Files in each group are either presented one per line, separated by empty lines,
or concatenated on a single line.
"""

import lnsync_pkg.printutils as pr

class GroupedFileListPrinter:
    """
    Output filepaths in groups, either separated by empty lines or by empty
    spaces, sorted or not.
    """
    def __init__(self, hard_links, all_links, sameline, sort):
        """
         - sameline: if True, filenames in each group are printed in the same
        line, separated by spaces, with filename spaces and backslashes escaped;
        other print one file per line, with an empty line separating groups.
        - hard_links: if False, print all aliases for each file as if they were
        different files; if False, print for each file a single path alias,
        arbitrarily chosen.
        - all_links: if True (and assuming hard_links is False), print all links
        to each file.
        """
        self.hard_links = hard_links
        self.all_links = all_links
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
            def get_average_size(located_files):
                # Use the average size.
                tot = 0
                count = 0
                for file_list in located_files.values(): # Use any.
                    for file_obj in file_list:
                        tot += file_obj.file_metadata.size
                        count += 1
                return tot / count
            self.groups.sort(key=get_average_size)
            for group in self.groups:
                self._print_group(group)

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
                elif not self.hard_links or self.all_links:
                    include, prefix = (True, " ")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    # Escape a few choice characters.
                    for char in ("\\", " ", "'", '"', "(", ")"):
                        pr_path = pr_path.replace(char, "\\"+char)
                    self._built_line += prefix + pr_path
        else:
            for k, relpath in enumerate(fobj.relpaths):
                if k == 0:
                    include, prefix = (True, "")
                elif not self.hard_links:
                    include, prefix = (True, "")
                elif self.all_links:
                    include, prefix = (True, " ")
                else:
                    include = False
                if include:
                    pr_path = tree.printable_path(relpath)
                    pr.print(prefix + pr_path, end="\n")
