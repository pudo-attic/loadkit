import boto
from moto import mock_s3

from loadkit import PackageIndex, extract
from loadkit.tests.util import CSV_FIXTURE, CSV_URL


@mock_s3
def test_basic_api():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('test.mapthemoney.org')

    index = PackageIndex(bucket)
    assert not len(list(index)), len(list(index))

    package = index.create({'test': 'value'})
    assert len(list(index)) == 1, len(list(index))
    assert package.id is not None, package.id

    assert package.manifest['test'] == 'value'

    assert index.get(package.id) == package, index.get(package.id)


@mock_s3
def test_extract_file():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('test.mapthemoney.org')

    package = PackageIndex(bucket).create()
    res = extract.from_file(package, CSV_FIXTURE)
    assert res is not None, res

    assert 'barnet-2009.csv' in res.path, res


@mock_s3
def test_extract_url():
    conn = boto.connect_s3()
    bucket = conn.create_bucket('test.mapthemoney.org')

    package = PackageIndex(bucket).create()
    res = extract.from_url(package, CSV_URL)
    assert res is not None, res

    assert 'barnet-2009.csv' in res.path, res

