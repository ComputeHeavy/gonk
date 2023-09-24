from setuptools import setup

setup(
    name='gonk',
    version='0.1',
    packages=['gonk', 'gonk.core', 'gonk.api'],
    package_dir={
        'gonk': 'src',
        'gonk.core': 'src/core',
        'gonk.api': 'src/api',
    },
    entry_points={
        'console_scripts': [
            'gonk-api=gonk.api.server:cli',
        ],
    },
)