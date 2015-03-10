from setuptools import setup, find_packages


setup(
    name='loadkit',
    version='0.3',
    description="Light-weight tools for ETL",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    keywords='web scraping crawling http cache threading',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://github.com/pudo/loadkit',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'SQLAlchemy>=0.9.8',
        'messytables>=0.2.1',
        'requests>=2.5.1',
        'archivekit>=0.5',
        'click>=3.2',
        'normality>=0.1'
    ],
    tests_require=[],
    entry_points={
        'archivekit.resource_types': [
            'table = loadkit.types.table:Table',
            'logfile = loadkit.types.logfile:LogFile',
            'stage = loadkit.types.stage:Stage'
        ],
        'loadkit.operators': [
            'text_extract = loadkit.operators.text:TextExtractOperator',
            'table_extract = loadkit.operators.table:TableExtractOperator',
            'normalize = loadkit.operators.normalize:NormalizeOperator',
            'regex = loadkit.operators.regex:RegExOperator',
            'ingest = loadkit.operators.ingest:IngestOperator'
        ],
        'console_scripts': [
            'loadkit = loadkit.cli:cli'
        ]
    }
)
