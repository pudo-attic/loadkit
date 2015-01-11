from loadkit.tests.util import get_bucket, CSV_FIXTURE, CSV_URL # noqa
from loadkit import PackageIndex, extract, transform


# Connect to a package index on an S3 bucket:
index = PackageIndex(get_bucket())

# create a new package within that index:
package = index.create()

# load a resource from the local file system:
source = extract.from_file(package, CSV_FIXTURE)
print 'Source uploaded:', source

# or:
# resource = extract.from_url(package, CSV_URL)

# Transform the uploaded file into a well-understood
# format (an ``Artifact``):
artifact = transform.to_table(source, 'table')
print 'Artifact generated:', artifact

# In your library: load the artifact into the table.
