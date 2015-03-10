import boto
from moto import mock_s3
from datetime import date

from archivekit import open_collection, Source
from loadkit import extract
from loadkit.pipeline import Pipeline
from loadkit.types.table import Table
from loadkit.operators.table import TableExtractOperator
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

    artifacts = list(package.all(Table))
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
    extract.from_file(package, GPC_FIXTURE)
    pipeline = Pipeline(index, 'foo', {
        'process': {
            'table': {
                'operator': 'table_extract'
            }
        }
    })
    pipeline.process_package(package)

    artifacts = list(package.all(Table))
    assert len(artifacts) == 1, artifacts
    artifact = artifacts[0]
    assert artifact.name == 'table.json'
    recs = list(artifact.records())
    assert len(recs) == 23, len(recs)
    assert isinstance(recs[0]['transaction_date'], date)
