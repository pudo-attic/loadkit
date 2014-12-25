import os

from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.connection import Location

LOCATION = Location.EU

conn = S3Connection(os.environ.get('AWS_KEY_ID'),
                    os.environ.get('AWS_SECRET'))


bucket_name = 'data.mapthemoney.org'

try:
    bucket = conn.get_bucket(bucket_name)
except S3ResponseError, se:
    #if se.status == 404:
    bucket = conn.create_bucket(bucket_name)


from spendloader import PackageIndex
from spendloader.convert import convert_package

index = PackageIndex(bucket)

for package in index:
    #print package, package.manifest.items()
    convert_package(package)
    #print package


# package = Package(bucket)

#PATH = '/Users/fl/Downloads/interpol.csv'
#with open(PATH, 'rb') as fh:
#    package = index.create(source_file=PATH)
#    package.source.set_contents_from_file(fh)
