"""blah."""

from setuptools import find_packages, setup  # type: ignore

with open("README.md") as f:
    long_description = f.read()

setup(
    name="nmdc_runtime",
    url="https://github.com/microbiomedata/nmdc-runtime",
    packages=find_packages(
        include=["nmdc_runtime*", "components*"],
        exclude=["tests", "tests2"],
    ),
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    author="Donny Winston",
    author_email="donny@polyneme.xyz",
    description="A runtime system for NMDC data management and orchestration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "nmdcdb-mongoexport = nmdc_runtime.site.backup.nmdcdb_mongoexport:main",
            "nmdcdb-mongodump = nmdc_runtime.site.backup.nmdcdb_mongodump:main",
            "nmdcdb-mongoimport = nmdc_runtime.site.backup.nmdcdb_mongoimport:main",
        ]
    },
)
