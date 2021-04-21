import setuptools

setuptools.setup(
    name="nmdc_runtime",
    packages=setuptools.find_packages(exclude=["tests"]),
    install_requires=[
        "dagster>=0.11.2",
        "dagit>=0.11.2",
        "linkml",
        "pytest",
    ],
    entry_points={
        "console_scripts": [
            "gen-terminusdb = nmdc_runtime.terminusdbgen:cli",
        ]
    },
)
