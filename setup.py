#!/usr/bin/env python
from setuptools import setup

tests_require = [
    "pytest",
    "pytest-runner",
    "pre-commit",
    "black",
    "isort",
    "keyring",
    "testcontainers",
    "boto3",
    "pandas",
    "pyarrow",
]

setup(
    name="dask-trino",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Dask + Trino intergration",
    license="BSD",
    maintainer="Padraig O'Sullivan",
    maintainer_email="padraig.osullivan@starburstdata.com",
    packages=["dask_trino"],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    install_requires=[
        "dask>=2024.3.0",
        "distributed",
        "trino>=0.333.0",
        "trino[sqlalchemy]",
    ],
    include_package_data=True,
    zip_safe=False,
    extras_require={
        "tests": tests_require,
    },
)
