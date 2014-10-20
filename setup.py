#!/usr/bin/env python

import os

from setuptools import setup, find_packages
import versioneer


versioneer.VCS = 'git'
versioneer.versionfile_source = 'clusterit/_version.py'
versioneer.versionfile_build = 'clusterit/_version.py'
versioneer.tag_prefix = 'v'
versioneer.parentdir_prefix = 'clusterit-'


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='ClusterIt',
    version=versioneer.get_version(),
    description='Configurable clustering feature proxy',
    long_description=read('README.md'),
    author='Christian Wygoda',
    author_email='christian.wygoda@wygoda.net',
    license="BSD",
    url='https://github.com/arsgeografica/clusterit',
    packages=find_packages(exclude=["tests.*", "tests"]),
    install_requires=[
        'flask>=0.10.1',
        'geoalchemy2>=0.2.1',
        'geojson>=1.0.1',
        'psycopg2>=2.5.1',
        'shapely>=1.2.17'
    ],
    extras_require={
        'Oracle': [
            'cx_Oracle>=5.1.3'
        ]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    cmdclass=versioneer.get_cmdclass(),
)
