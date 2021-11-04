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
            # CLI command=package.module:function
            'lnsync=lnsync_pkg.lnsync:main32',
            'lnsync32=lnsync_pkg.lnsync:main32',
            'lnsync64=lnsync_pkg.lnsync:main64',
            'lnsync-nopreset=lnsync_pkg.lnsync:main_nopreset',
        ],
    },
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: System :: Archiving :: Mirroring',
        'Topic :: Utilities',
    ],
)
