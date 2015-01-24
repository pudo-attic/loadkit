import boto
from moto import mock_s3
from datetime import date

from barn import open_collection, Source
from loadkit import extract, transform, Artifact
from loadkit.tests.util import CSV_FIXTURE, CSV_URL
from loadkit.tests.util import GPC_FIXTURE


@mock_s3
def test_basic_api():
    index = open_collection('test', 's3', bucket_name='test.mapthemoney.org')
    assert not len(list(index)), len(list(index))

    package = index.create(manifest={'test': 'value'})
    assert len(list(index)) == 1, len(list(index))
    assert package.id is not None, package.id

    assert package.manifest['test'] == 'value'

    assert index.get(package.id) == package, index.get(package.id)


@mock_s3
def test_extract_file():
    index = open_collection('test', 's3', bucket_name='test.mapthemoney.org')
    package = index.create()
    src = extract.from_file(package, CSV_FIXTURE)
    assert src is not None, src

    sources = list(package.all(Source))
    assert len(sources) == 1, sources

    artifacts = list(package.all(Artifact))
    assert len(artifacts) == 0, artifacts

    assert 'barnet-2009.csv' in src.path, src


@mock_s3
def test_extract_url():
    index = open_collection('test', 's3', bucket_name='test.mapthemoney.org')
    package = index.create()
    src = extract.from_url(package, CSV_URL)
    assert src is not None, src

    assert 'barnet-2009.csv' in src.path, src


@mock_s3
def test_parse_with_dates():
    index = open_collection('test', 's3', bucket_name='test.mapthemoney.org')
    package = index.create()
    src = extract.from_file(package, GPC_FIXTURE)
    artifact = transform.to_table(src, 'table')

    assert artifact.name == 'table'
    recs = list(artifact.records())
    assert len(recs) == 23, len(recs)
    assert isinstance(recs[0]['transaction_date'], date)
