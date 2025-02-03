import os
import uuid

import pandas as pd
import pytest
import trino
from trino.sqlalchemy import URL
from sqlalchemy import create_engine, text

import dask
import dask.dataframe as dd
import dask.datasets
from dask.utils import is_dataframe_like, parse_bytes
from distributed import Client, Lock, worker_client
from distributed.utils_test import cleanup  # noqa: F401
from distributed.utils_test import client  # noqa: F401
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import loop  # noqa: F401
from distributed.utils_test import loop_in_thread  # noqa: F401

from dask_trino import to_trino, read_trino


@pytest.fixture
def client():
    with Client(n_workers=2, threads_per_worker=10) as client:
        yield client


@pytest.fixture
def table(connection_kwargs):
    name = f"test_table_{uuid.uuid4().hex}"

    yield name

    engine = create_engine(URL(**connection_kwargs))
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {name}"))


@pytest.fixture(scope="module")
def connection_kwargs():
    return dict(
        user=os.environ["TRINO_USER"],
        host=os.environ["TRINO_HOST"],
        port=os.environ["TRINO_PORT"],
        catalog=os.environ.get("TRINO_CATALOG", "system"),
        schema=os.environ.get("TRINO_SCHEMA", "runtime"),
    )


df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
ddf = dd.from_pandas(df, npartitions=2)


def test_read_empty_result(table, connection_kwargs):
    # A query that yields in an empty results set should return an empty DataFrame
    to_trino(ddf, name=table, connection_kwargs=connection_kwargs)

    result = read_trino(
        f"SELECT * FROM {table} where a > {df.a.max()}",
        connection_kwargs=connection_kwargs,
        npartitions=2,
    )
    assert is_dataframe_like(result)
    assert len(result.index) == 0
    assert len(result.columns) == 0


def test_write_read_roundtrip(table, connection_kwargs):
    to_trino(ddf, name=table, connection_kwargs=connection_kwargs)

    query = f"SELECT * FROM {table}"
    engine = create_engine(URL(**connection_kwargs))
    connection = engine.connect()
    rows = connection.execute(text(query)).fetchall()
    assert len(rows) == 10

    df_out = read_trino(query, connection_kwargs=connection_kwargs, npartitions=2)
    assert df_out.shape[0].compute() == 10
    assert list(df.columns) == list(df_out.columns)
    dd.utils.assert_eq(
        df.set_index('a'), df_out.set_index('a')
    )


def test_query_with_filter(table, connection_kwargs):
    to_trino(ddf, name=table, connection_kwargs=connection_kwargs)

    query = f"SELECT * FROM {table} WHERE a = 3"
    df_out = read_trino(query, connection_kwargs=connection_kwargs, npartitions=2)
    assert df_out.shape[0].compute() == 1
    assert list(df.columns) == list(df_out.columns)
    dd.utils.assert_eq(
        df[df["a"] == 3],
        df_out,
        check_dtype=False,
        check_index=False,
    )


def test_write_read_large_resultset(connection_kwargs, client):
    query = f"select l.* from tpch.tiny.lineitem l, table(sequence( start => 1, stop => 5, step => 1)) n"
    df_out = read_trino(query, connection_kwargs=connection_kwargs, npartitions=2)
    assert df_out.shape[0].compute() == 11
    dd.utils.assert_eq(
        df.set_index('a'), df_out.set_index('a')
    )
