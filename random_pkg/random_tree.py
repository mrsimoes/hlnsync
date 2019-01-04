#!/usr/bin/env python

from __future__ import print_function

import random
import errno
import tempfile
import os
import sys
import shutil
from collections import defaultdict

from random_pkg.random_extras import dirichlet_vec_discrete_rv, randint_intv_avg, randint_avg

from lnsync_pkg.filetree import FileTree

DIR_PREFIX = "d-"
FILE_PREFIX = "f-"

class RandomTree(FileTree):
    """
    Randomly generate, modify, and pick elements from a file tree.
    Also, support for a new "cp" command.
    """
    def __init__(self, top_dir_path, content_maker=None):
        """
        Argument content_maker function maps filenumber -> content string.
        """
        if content_maker is None:
            content_maker = lambda n: ("content for file %d" % n)
        self._content_maker = content_maker
        self._all_dirs = []
        def dir_is_empty(absdir):
            "Check if a given directory has no entries."
            return len(os.listdir(absdir)) == 0
        if not dir_is_empty(top_dir_path):
            raise RuntimeError("RandomTree: top dir must be initially empty.")
        FileTree.__init__(self, top_dir_path, use_metadata=False)
        self._next_file_number = 1
        self._files_removed = {} # For undoing rm operations.

    def _make_dir(self, dir_id):
        d = FileTree._make_dir(self, dir_id)
        self._all_dirs.append(d)
        return d

    def pick_random_dir(self):
        return random.choice(self._all_dirs)

    def pick_random_file(self):
        rand_fid = random.choice(self._id_to_file.keys())
        return self._id_to_file[rand_fid]

    def pick_random_path(self, file_obj=None):
        if file_obj is None:
            file_obj = self.pick_random_file()
        rand_path = random.choice(file_obj.relpaths)
        return rand_path

    def pick_random_free_path(self):
        """
        Return a relapath (with basename "<FILE_PREFIX>xxx") that is not a file or dir,
        either in the source file system or the tree.
        """
        dir_obj = self.pick_random_dir()
        dir_relpath = dir_obj.get_relpath()
        while True:
            bname = "%s%03d" % (FILE_PREFIX, random.randint(0,999))
            if bname in dir_obj.entries:
                continue
            free_relpath = os.path.join(dir_relpath, bname)
            if not os.path.isfile(self.rel_to_abs(free_relpath)):
                return free_relpath

    def populate_from_description(self, desc):
        """
        Create a tmp file tree from a subtree description 
        [num_top_files, [subtree_1, ..., subtree_n]].
        """ 
        def populate_subdir_rec(desc, subd_obj):
            "Arg subdir is a rel path."
            for k in range(desc[0]):
                self._create_next_file(subd_obj)
            for subt in desc[1:]:
                new_subd_obj = self._create_next_dir(subd_obj)
                populate_subdir_rec(subt, new_subd_obj)
            subd_obj.mark_scanned()
        populate_subdir_rec(desc, self.rootdir_obj)

    def populate_rootdir(self, nr_files):
        self.populate_from_description([nr_files])

    def populate_randomly(self, nr_files, avg_branch, avg_depth):
        """
        Create a random file tree with a total of nr_files,
        organized so that the average branching is avg_br
        and the average depth is avg_depth.
        """
        def mk_rand_tree(nr_fs, avg_br, avg_d):
            """
            Create a description of a subtree containing
            with average branching factor avg_br and
            average depth avg_d and nr_f files.
            """
            if nr_fs == 0:
                return [0]
            if avg_d <= 1:
                return [nr_fs]
            if nr_fs > avg_br:
                num_dirs = randint_intv_avg(0, nr_fs, avg_br)
            else:
                num_dirs = random.randint(0, nr_fs)
            # Each dir must have at least one file.
            split = dirichlet_vec_discrete_rv(num_dirs+1, nr_fs-num_dirs)
            for k in range(1, num_dirs+1):
                split[k] = mk_rand_tree(split[k]+1, avg_br, randint_avg(avg_d-1))
            return split
        d = mk_rand_tree(nr_files, avg_branch, avg_depth)
        print(d)
        self.populate_from_description(d)

    def _create_next_dir(self, dir_obj):
        """
        Create a subdir at dir_obj, return the new subdir obj.
        """
        reldir = dir_obj.get_relpath()
        dname = "%s%d" % (DIR_PREFIX, len(self._all_dirs))
        new_rel_subdir  = os.path.join(reldir, dname)
        os.makedirs(self.rel_to_abs(new_rel_subdir))
        newd_obj = self._make_dir(None)
        dir_obj.add_entry(dname, newd_obj)
        self._all_dirs += [newd_obj]
        return newd_obj

    def _create_next_file(self, reldir_obj):
        """
        Create a file with unique basename at the given relative subdir
        in the source tree and insert it into the tree object.
        """
        bname = "%s%03d" % (FILE_PREFIX, self._next_file_number)
        contents = self._content_maker(self._next_file_number)
        self._next_file_number += 1
        self._create_new_file(reldir_obj, bname, contents)

    def _create_new_file(self, reldir_obj, bname, contents):
        """
        Create a file with given basename at the given relative subdir
        in the source tree and insert it into the tree object.
        """
        reldir = reldir_obj.get_relpath()
        absdir = self.rel_to_abs(reldir)
        abs_filepath = os.path.join(absdir, bname)
        relpath = os.path.join(reldir, bname)
        with open(abs_filepath, "w") as f:
            f.write(contents)
        st = os.stat(abs_filepath)
        fid = self._id_computer.get_id(relpath)
        f_obj = self.new_file_obj(fid, st)
        self._add_path(f_obj, reldir_obj, bname)

    
    def exec_cmds_random(self, cmds):
        """
        Execute a sequence of commands (mv, ln, rm, cp), each
        with random arguments, and return a list of commands with actual arguments.
        """
        out_cmds = []
        for ctype in cmds:
            if self._id_to_file == {}:
                break
            if ctype == "mv":
                res = (ctype, self.pick_random_path(), self.pick_random_free_path())
            elif ctype == "ln":
                res = (ctype, self.pick_random_path(), self.pick_random_free_path())
            elif ctype == "cp":
                res = (ctype, self.pick_random_path(), self.pick_random_free_path())
            elif ctype == "rm":
                rand_file = self.pick_random_file()
                if len(rand_file.relpaths) == 1:
                    rand_path, alias_path = rand_file.relpaths[0], None
                else:
                    rand_path, alias_path = random.sample(rand_file.relpaths, 2)
                res = (ctype, rand_path, alias_path)
            else:
                raise RuntimeError("Unknown command: %s", (ctype,))
            self.exec_cmd(res)
            out_cmds.append(res)
        return out_cmds

    def exec_cmd(self, cmd):
        """
        Execute a file command (cmdname, path_from, path_to).
        Add cp and rm final path to FileTree.
        """
        if cmd[0] == "cp":
            assert self.writeback, "cannot cp file without writeback"
            fn_from, fn_to = cmd[1:]
            fn_abs_from = self.rel_to_abs(fn_from)
            fn_abs_to = self.rel_to_abs(fn_to)
            shutil.copy2(fn_abs_from, fn_abs_to)
            st = os.stat(fn_abs_to)
            fid = self._id_computer.get_id(fn_to)
            new_f_obj = self.new_file_obj(fid, st)
            tr_obj = self._create_dir_if_needed_writeback(os.path.dirname(fn_to))
            self._add_path(new_f_obj, tr_obj, os.path.basename(fn_to))
        elif cmd[0] == "rm" and cmd[2] is None:
            assert self.writeback, "cannot rm file without writeback"
            dirname = os.path.dirname(cmd[1])
            bname = os.path.basename(cmd[1])
            d_obj = self.follow_path(dirname)
            assert d_obj is not None
            f_obj = d_obj.get_entry(bname)
            assert f_obj is not None
            assert f_obj.relpaths == [cmd[1]]
            abs_filepath = self.rel_to_abs(cmd[1])
            with open(abs_filepath, "r") as f:
                contents = f.read()
            self._files_removed[cmd[1]] = contents
            self._rm_path(f_obj, d_obj, bname)
            os.unlink(abs_filepath)
        else:
            FileTree.exec_cmd(self, cmd)

    def exec_cmd_reverse(self, cmd):
        """
        Undo a file command (cmdname, path_from, path_to).
        Undoes cp and rm final path implemented in this class.
        """
        cmd_name, fn_from, fn_to = cmd
        if cmd_name == "cp":
            assert self.writeback, "cannot undo cp file without writeback"
            self.exec_cmd(("rm", fn_to, None))
        elif cmd_name == "rm" and fn_to is None:
            assert self.writeback, "cannot undo rm file without writeback"
            dirname = os.path.dirname(fn_from)
            d_obj = self.follow_path(dirname)
            assert d_obj is not None
            bname = os.path.basename(fn_from)
            contents = self._files_removed[fn_from] 
            self._create_new_file(d_obj, bname, contents)
            del self._files_removed[fn_from]
        else:
            FileTree.exec_cmd_reverse(self, cmd)

    def exec_cmds_if_possible(self, cmds):
        """
        Return a list of the commands actually executed.
        """
        done_cmds = []
        for c in cmds:
            try:
                self.exec_cmd(c)
            except Exception:
                pass
            else:
                done_cmds.append(c)
        return done_cmds

class TmpRandomTree(RandomTree):
    def __init__(self, group_dir=None, **args):
        self._group_dir = group_dir
        self._temp_dir = None
        self._temp_dir = tempfile.mkdtemp(prefix="tr-", dir=group_dir)
        RandomTree.__init__(self, self._temp_dir, **args)

    def __enter__(self):
        RandomTree.__enter__(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        RandomTree.__exit__(self, exc_type, exc_val, exc_tb)
        print(self, exc_type, exc_val, exc_tb)
        if self._temp_dir is not None:
            shutil.rmtree(self._temp_dir)
        return False

    @staticmethod
    def clone(other_tmp_tree):
        """
        Populate a tree by copying content from another tree.
        """
        clone = TmpRandomTree(other_tmp_tree._group_dir)
        for en in os.listdir(other_tmp_tree.rootdir_path):
            other_path = os.path.join(other_tmp_tree.rootdir_path, en)
            new_path = os.path.join(clone.rootdir_path, en)
            if os.path.isfile(other_path):
                shutil.copy2(other_path, new_path)
            else: # Copytree needs to create the new target topdir.
                shutil.copytree(other_path, new_path)
        clone.scan_full_tree() # RandomTree root dir is left unscanned by default. 
        return clone



