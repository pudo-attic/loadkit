# LoadKit

LoadKit is a simple Python-based ETL framework inspired by a discussion about the [OpenSpending](http://openspending.org) data warehouse platform.

It is intended to accept tabular input files, such as CSV files, Excel spreadsheets and [other formats](https://messytables.readthedocs.org/). The data is uploaded to an S3 bucket together with a JSON metadata file.

Once data has been uploaded, it can be processed and turned into a series of ``Artifacts``, which are transformed versions of the initial resource.

Finally, an ``Artifact`` can be loaded into an automatically generated SQL database table in order to be queried for analytical purposes.

### Usage

See ``demo.py`` in the project root.

### What is to be done

* Decide which bits of the ``datapackage`` specification this needs to adhere to.
* Create a Postgres FTS index when loading the data with [sqlalchemy-searchable](https://github.com/kvesteri/sqlalchemy-searchable/).

### References

* [OpenSpending Enhancement Protocol 2](https://github.com/openspending/osep/blob/gh-pages/02-data-storage-and-data-pipeline.md).

