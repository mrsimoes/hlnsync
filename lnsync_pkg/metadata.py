# -*- coding: utf-8 -*-
"""
hlnsync project metadata.
"""

# pylint: disable-all

# The package name, which is also the "UNIX name" for the project.
package = 'hlnsync'
version = '0.9.0rc4'
description = "Sync local dirs by content, with rename detection and " \
              "hard link support, plus fast fdupes, and more."
summary = description
project = 'hlnsync dir sync tool'
project_no_spaces = project.replace(' ', '')
url = 'https://github.com/mrsimoes/hlnsync'
download_url = 'https://github.com/mrsimoes/hlnsync/archive/v%s.tar.gz' % version
keywords = 'sync rsync fast rename backup link hardlink fdupes'
authors = ['Miguel Simoes']
authors_string = ', '.join(authors)
emails = ['miguelrsimoes@yahoo.com']
license = 'GNU General Public License v3'
copyright = '(C) 2018-2021 ' + authors_string
