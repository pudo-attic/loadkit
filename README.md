# LoadKit

[![Build Status](https://travis-ci.org/pudo/loadkit.png?branch=master)](https://travis-ci.org/pudo/loadkit) [![Coverage Status](https://coveralls.io/repos/pudo/loadkit/badge.svg)](https://coveralls.io/r/pudo/loadkit)

``loadkit`` is a data and document processing tool. It can be used to
construct multi-stage processing pipelines and to monitor the
execution of tasks through these pipelines.

``loadkit`` will traverse a collection of ``archivekit`` packages, which
contain source documents or data files. The stages of the processing
pipeline will consume these sources and transform them into a series of
derived artifacts.

## Installation

The easiest way of using ``loadkit`` is via PyPI:

```bash
$ pip install loadkit
```

Alternatively, check out the repository from GitHub and install it locally:

```bash
$ git clone https://github.com/pudo/loadkit.git
$ cd loadkit
$ python setup.py develop
```


## Usage

Each data processing pipeline is defined as a set of operations, divided into two phases, the ``extract`` and ``transform`` steps. Operations defined in the ``extract`` phase will be executed once (to import a set of packages), while operations defined in the ``transform`` phase will be executed for each package.

A pipeline is defined through a YAML file, such as this:

```yaml
config:
    collections:
        my-project:
            type: file
            path: /srv/my-project

extract:
    docs:
        operator: 'ingest'
        source: '~/tmp/incoming'
        meta:
            source: 'Freshly scraped'

transform:
    mime:
        operator: 'mime_type'

    text:
        requires: 'mime'
        operator: 'textract'

    index:
        requires: ['text', 'mime']
        operator: 'elasticsearch'
        url: 'http://bonsai.io/...'
```

As you can see, each operation node is named and can be referenced by others as a required precondition.

Such a pipeline can be executed using the following command:

```bash
$ loadkit run pipeline.yaml
```

Alternatively, each phase of the process can be executed individually: 

```bash
$ loadkit extract pipeline.yaml
$ loadkit transform pipeline.yaml
```

### Available operators

The library includes a small set of pre-defined operators for document processing. Other operators can also be defined via entry points in Python packages; they will be picked up automatically once installed in the same Python environment.

* ``ingest``, the default document ingester. It accepts on configuration option, ``source``, which can be a URL, file path or directory name.

### Adding new operators

``loadkit`` is easily enhanceable, allowing for the seamless addition of domain-specific or other complex operators in a processing pipeline. Each ``operator`` is a simple Python class inherited from ``loadkit.Operator``:

```python
from loadkit import Operator

class FileSizeOperator(Operator):

    def process(self, package):
        # config is set in the pipline for each task.
        field = self.config.get('field', 'file_size')

        # For help with the document object, see docstash.
        with open(document.file, 'r') as fh:
            document[field] = len(fh.read())
        document.save()

    # Alternatively, tasks can also implement the ``extract(self)`` method.
```

To become available in processing pipelines, the operator must also be registered as an entry point in the Python package's ``setup.py`` like this:

```python
...
setup(
    ...
    entry_points={
        'loadkit.operators': [
            'my_op = my_package:FileSizeOperator'
        ]
    },
    ...
)
```

Note that changes to ``setup.py`` only come into effect after the package has been re-installed, or the following command has been executed:

```bash
$ python setup.py develop
```

## License

``loadkit`` is open source, licensed under a standard MIT license (included in this repository as ``LICENSE``).
