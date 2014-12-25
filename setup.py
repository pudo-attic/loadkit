from setuptools import setup, find_packages


setup(
    name='loadkit',
    version='0.0.1',
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
    install_requires=[
        'SQLAlchemy>=0.9.8',
        'boto>=2.34.0',
        'moto>=0.3.9',
        'messytables>=0.2.1',
        'requests>=2.5.1',
        'python-slugify>=0.1.0'
    ],
    tests_require=[],
    entry_points={
        'console_scripts': []
    }
)
