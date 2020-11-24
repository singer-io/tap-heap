#!/usr/bin/env python

from setuptools import setup

setup(name='tap-heap',
      version='1.0.2',
      description='Singer.io tap for extracting Heap data from Avro files in S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_heap'],
      install_requires=[
          'boto3==1.9.57',
          'singer-encodings==0.0.3',
          'singer-python==5.1.5',
          'python-snappy==0.5.3',
          'fastavro==0.21.8'
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-heap=tap_heap:main
      ''',
      packages=['tap_heap'])
