#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from lnsync_pkg import metadata

cwd = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join("README.md")) as readme_file:
    long_description = readme_file.read()

setup(
	name = 'lnsync',
	version = metadata.version,
	summary = "bug fixes",
	description = metadata.description,
	url = metadata.url,
	download_url = metadata.download_url,
	author = metadata.authors[0],
	author_email = metadata.emails[0],
	license = metadata.license,
	long_description = long_description,
	long_description_content_type = 'text/markdown',
	keywords = metadata.keywords,
	install_requires = ['xxhash', 'psutil'],
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
       'Programming Language :: Python :: 3.6',
       'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
       'Topic :: System :: Archiving :: Backup',
       'Topic :: System :: Archiving :: Mirroring',
       'Topic :: Utilities',
	],
)
