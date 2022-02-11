# -*- coding: utf-8 -*-
"""
lnsync project metadata.
"""

# pylint: disable-all

# The package name, which is also the "UNIX name" for the project.
package = 'lnsync'
version = '0.8.0'
description = "Dir sync by content with rename detection, " \
              "hard link support, plus fast fdupes, and more."
summary = description
project = 'lnsync dir sync tool'
project_no_spaces = project.replace(' ', '')
url = 'https://github.com/mrsimoes/lnsync'
download_url = 'https://github.com/mrsimoes/lnsync/archive/v%s.tar.gz' % version
keywords = 'sync rsync fast rename backup link hardlink fdupes'
authors = ['Miguel Simoes']
authors_string = ', '.join(authors)
emails = ['miguelrsimoes@yahoo.com']
license = 'GNU General Public License v3'
copyright = '(C) 2018-2021 ' + authors_string
