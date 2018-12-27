#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from lnsync_pkg import metadata
cwd = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join("docs/README.md")) as readme_file:
    long_description = readme_file.read()

setup(
	name = 'lnsync',
	version = metadata.version,
	description = metadata.description,
	url = metadata.url,
	download_url = 'https://github.com/mrsimoes/lnsync/archive/v0.1.0.tar.gz',
	author = metadata.authors[0],
	author_email = metadata.emails[0],
	license = 'GNU General Public License v3',
	long_description = long_description,
	install_requires = ['pyhashxx', 'psutil'],
	packages = find_packages(exclude=['tests']),
	entry_points = {
		'console_scripts': [
		'lnsync=lnsync_pkg.lnsync:main',  # CLI command=package.module:function
		],
	},
	classifiers = [
       'Development Status :: 3 - Alpha',
       'Environment :: Console',
       'Operating System :: POSIX :: Linux',
       'Programming Language :: Python :: 2.7',
       'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
       'Topic :: System :: Archiving :: Backup',
       'Topic :: Utilities',
	],
)
