import os
import uuid

import dask.dataframe as dd
import dask.datasets
import pandas as pd
import pytest
import trino
from dask.utils import is_dataframe_like
from distributed import Client
from sqlalchemy import create_engine
from sqlalchemy import text
from trino.sqlalchemy import URL

from dask_trino import read_trino
from dask_trino import to_trino


@pytest.fixture()
def trino_connection(request, run_trino):
    host, port = run_trino
    encoding = request.param

    yield trino.dbapi.Connection(
        host=host,
        port=port,
        user="test",
        source="test",
        max_attempts=1,
        encoding=encoding
    )


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
def connection_kwargs(run_trino):
    host, port = run_trino
    return dict(
        user="pytest",
        host=host,
        port=port,
        catalog=os.environ.get("TRINO_CATALOG", "memory"),
        schema=os.environ.get("TRINO_SCHEMA", "default"),
    )


df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
ddf = dd.from_pandas(df, npartitions=2)


def test_read_empty_result(table, connection_kwargs):
    # A query that yields in an empty results set should return
    # an empty DataFrame
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

    df_out = read_trino(
        query,
        connection_kwargs=connection_kwargs,
        npartitions=2
    )
    assert df_out.shape[0].compute() == 10
    assert list(df.columns) == list(df_out.columns)
    assert df_out['a'].count().compute() == 10
    dd.utils.assert_eq(
        df.set_index('a'), df_out.set_index('a')
    )


def test_query_with_filter(table, connection_kwargs):
    to_trino(ddf, name=table, connection_kwargs=connection_kwargs)

    query = f"SELECT * FROM {table} WHERE a = 3"
    df_out = read_trino(
        query,
        connection_kwargs=connection_kwargs,
        npartitions=2
    )
    assert df_out.shape[0].compute() == 1
    assert list(df.columns) == list(df_out.columns)
    dd.utils.assert_eq(
        df[df["a"] == 3],
        df_out,
        check_dtype=False,
        check_index=False,
    )


def test_writing_large_dataframe(connection_kwargs):
    df = dask.datasets.timeseries(end='2000-01-03')
    to_trino(df, name="large_table", connection_kwargs=connection_kwargs)
    df_out = read_trino(
        "select * from large_table",
        connection_kwargs=connection_kwargs,
        npartitions=2
    )
    assert df_out.shape[0].compute() == df.shape[0].compute()
    engine = create_engine(URL(**connection_kwargs))
    connection = engine.connect()
    connection.execute(text("drop table large_table"))


def test_read_large_resultset(connection_kwargs, client):
    query = """
    select
      l.*
    from
      tpch.tiny.lineitem l,
      table(sequence( start => 1, stop => 5, step => 1)) n"""
    df_out = read_trino(
        query,
        connection_kwargs=connection_kwargs,
        npartitions=2
    )
    assert df_out.shape[0].compute() == 300875
