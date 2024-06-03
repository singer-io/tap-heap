#!/usr/bin/env python

from setuptools import setup

setup(name='tap-heap',
      version='1.1.4',
      description='Singer.io tap for extracting Heap data from Avro files in S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_heap'],
      install_requires=[
          'boto3==1.34.117',
          'singer-encodings==0.1.3',
          'singer-python==6.0.1',
          'python-snappy==0.7.1',
          'fastavro==1.9.4'
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint',
              'pytest',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-heap=tap_heap:main
      ''',
      packages=['tap_heap'])
