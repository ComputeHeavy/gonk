# Copyright 2023 - Compute Heavy Industries Incorporated
# This work is released, distributed, and licensed under AGPLv3.

from setuptools import setup

setup(
    name='gonk',
    version='0.1.4',
    packages=['gonk', 'gonk.core', 'gonk.api', 'gonk.impl'],
    package_dir={
        'gonk': 'src',
        'gonk.core': 'src/core',
        'gonk.api': 'src/api',
        'gonk.impl': 'src/impl',
    },
    entry_points={
        'console_scripts': [
            'gonk-api=gonk.api.server:cli',
        ],
    },
    install_requires=[
        "Flask",
        "jsonschema",
        "PyNaCl",
        "click",
        "requests",
    ],
)