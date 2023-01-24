"""blah."""
from setuptools import find_packages, setup  # type: ignore

with open("README.md") as f:
    long_description = f.read()

with open("requirements/main.in") as f:
    install_requires = f.read().splitlines()

with open("requirements/dev.in") as f:
    dev_requires = f.read().splitlines()[1:]  # Elide `-c main.txt` constraint
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
    install_requires=install_requires,
    extras_require={
        "dev": dev_requires,
    },
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "schemagen-terminusdb = nmdc_runtime.site.terminusdb.generate:cli",
            "nmdcdb-mongoexport = nmdc_runtime.site.backup.nmdcdb_mongoexport:main",
            "nmdcdb-mongodump = nmdc_runtime.site.backup.nmdcdb_mongodump:main",
            "nmdcdb-mongoimport = nmdc_runtime.site.backup.nmdcdb_mongoimport:main",
        ]
    },
)
