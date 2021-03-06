#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import amqp

with open('README.md', 'r') as f:
    long_description = f.read()

with open('requirements.txt', 'r') as f:
    install_requires = list(f)

with open('test-requirements.txt', 'r') as f:
    test_requires = [x for x in list(f) if x[0:2] != '-r']

setup(
    name='tributary_amqp',
    description=amqp.__doc__,
    long_description=long_description,
    maintainer='Max Franks',
    maintainer_email='max.franks@synapse-wireless.com',
    url='http://www.synapse-wireless.com',
    packages=['amqp'],
    entry_points={
        'tributary.ext': '.amqp = amqp'
    },
    setup_requires=['vcversioner'],
    vcversioner={
        'version_module_paths': ['amqp/_version.py'],
        'vcs_args': ['git', '--git-dir', '%(root)s/.git', 'describe',
                  '--tags', '--long'],
    },
    install_requires=install_requires,
    tests_require=test_requires,
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Natural Language :: English',
    ],
)
