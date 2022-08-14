import setuptools

with open("README.md") as f:
    long_description = f.read()

with open("requirements/main.in") as f:
    install_requires = f.read().splitlines()

with open("requirements/dev.in") as f:
    dev_requires = f.read().splitlines()[1:]  # Elide `-c main.txt` constraint

setuptools.setup(
    name="nmdc_runtime_client",
    url="https://github.com/microbiomedata/nmdc-runtime",
    packages=["nmdc_runtime.client"],
    version="0.1.0",
    author="Donny Winston",
    author_email="donny@polyneme.xyz",
    description="Client for the NMDC Runtime",
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
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "nmdc-runtime-client = nmdc_runtime.client.main:cli",
        ]
    },
)
