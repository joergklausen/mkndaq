#!/usr/bin python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    author='Jörg Klausen',
    author_email='joerg.klausen@meteoswiss.ch',
    classifiers=['Development Status :: 3 - Alpha',
                 'Programming Language :: Python :: 3.5',
                 'Environment :: Console',
                 'Topic :: Utilities',],
    cmdclass={'doc': 'sphinx.BuildDoc'},
    description='Dispatch NRB Dobson files to PMOD/WRC',
    download_url='https://github.com/joergklausen/gawkenya.git',
    entry_points={
        'console_scripts': [
            'xfer2pmod = xfer2pmod.__main__:main',
        ],
    },
    include_package_data=True,
    install_requires=[
        'ftplib',
        'PyYaml',
        ],
    keywords='GAW Kenya',
    license='MIT',
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer='Jörg Klausen',
    maintainer_email='joerg.klausen@meteoswiss.ch',
    name='xfer2pmod',
    packages=find_packages(exclude=['bin', 'lib', 'lib64',
                                    '.git', '.idea']),
    platforms=['Ubuntu16', 'Ubuntu20'],
    tests_require=[],
    test_suite='gawkenya/tests/test_xfer2pmod.py',
    url='https://github.com/joergklausen/gawkenya.git',
    version='0.1.1',
)
