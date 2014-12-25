import os
import logging

from boto.s3.connection import S3Connection, S3ResponseError
from boto.s3.connection import Location

logging.basicConfig(level=logging.INFO)

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')
CSV_FIXTURE = os.path.join(FIXTURES, 'barnet-2009.csv')
CSV_URL = 'https://raw.githubusercontent.com/okfn/dpkg-barnet/master/barnet-2009.csv'


def get_bucket(bucket_name='tests.mapthemoney.org'):
    conn = S3Connection(os.environ.get('AWS_KEY_ID'),
                        os.environ.get('AWS_SECRET'))

    try:
        return conn.get_bucket(bucket_name)
    except S3ResponseError, se:
        if se.status == 404:
            return conn.create_bucket(bucket_name,
                                      location=Location.EU)
