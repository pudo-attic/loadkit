import os
import logging

from sqlalchemy import create_engine
from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.connection import Location

logging.basicConfig(level=logging.INFO)

conn = S3Connection(os.environ.get('AWS_KEY_ID'),
                    os.environ.get('AWS_SECRET'))


bucket_name = 'data.mapthemoney.org'

try:
    bucket = conn.get_bucket(bucket_name)
except S3ResponseError, se:
    #if se.status == 404:
    bucket = conn.create_bucket(bucket_name, location=Location.EU)


from loadkit import PackageIndex
from loadkit.convert import convert_package
from loadkit.database import database_load

index = PackageIndex(bucket)

#for package in index:
#    #print package, package.manifest.items()
#    convert_package(package)
#    #print package


# package = Package(bucket)

#PATH = '/Users/fl/Downloads/interpol.csv'
#PATH = '/Users/fl/Downloads/Plan_2015.xlsx'
PATH = 'barnet-2009.csv'
with open(PATH, 'rb') as fh:
    package = index.create(source_file=PATH)
    package.source.set_contents_from_file(fh)
    convert_package(package)
    database_load(None, package)
