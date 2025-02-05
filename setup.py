#!/usr/bin/env python
import os
from codecs import open
from typing import Any
from typing import Dict

from setuptools import setup

about: Dict[str, Any] = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "dask_trino", "_version.py"), "r", "utf-8") as f:
    exec(f.read(), about)

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
    name=about["__title__"],
    author=about["__author__"],
    author_email=about["__author_email__"],
    version=about["__version__"],
    url=about["__url__"],
    setup_requires=["setuptools_scm"],
    description="Dask + Trino integration",
    license=about["__license__"],
    packages=["dask_trino"],
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.10",
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
