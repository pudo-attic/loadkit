from sqlalchemy import create_engine

from loadkit.tests.util import get_bucket, CSV_FIXTURE, CSV_URL # noqa
from loadkit import PackageIndex, extract, transform, load


# Connect to a package index on an S3 bucket:
index = PackageIndex(get_bucket())

# create a new package within that index:
package = index.create()

# load a resource from the local file system:
resource = extract.from_file(package, CSV_FIXTURE)
print 'Resource uploaded:', resource

# or:
# resource = extract.from_url(package, CSV_URL)

# Transform the uploaded file into a well-understood
# format (an ``Artifact``):
artifact = transform.resource_to_table(resource, 'table')
print 'Artifact generated:', artifact

# Load into a database:
engine = create_engine('sqlite://')

# This will generate a table matching the columns of the
# artifact:
table = load.table(engine, artifact)
