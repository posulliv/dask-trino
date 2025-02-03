from functools import partial

import trino
from trino.mapper import RowMapperFactory
from trino.client import SegmentIterator, SpooledSegment
from trino.sqlalchemy import URL
from sqlalchemy import create_engine, text

import pandas as pd

import re
import dask
import dask.dataframe as dd
from dask.delayed import delayed


def df_to_sql_bulk_insert(df: pd.DataFrame, table: str) -> str:
    """Converts DataFrame to bulk INSERT sql query
    >>> data = [(1, "_suffixnan", 1), (2, "Noneprefix", 0), (3, "fooNULLbar", 1, 2.34)]
    >>> df = pd.DataFrame(data, columns=["id", "name", "is_deleted", "balance"])
    >>> df
       id        name  is_deleted  balance
    0   1  _suffixnan           1      NaN
    1   2  Noneprefix           0      NaN
    2   3  fooNULLbar           1     2.34
    >>> query = df_to_sql_bulk_insert(df, "users")
    >>> print(query)
    INSERT INTO users (id, name, is_deleted, balance)
    VALUES (1, '_suffixnan', 1, NULL),
           (2, 'Noneprefix', 0, NULL),
           (3, 'fooNULLbar', 1, 2.34);
    """
    df = df.copy()
    columns = ", ".join(df.columns)
    tuples = map(str, df.itertuples(index=False, name=None))
    values = re.sub(r"(?<=\W)(nan|None)(?=\W)", "NULL", (",\n" + " " * 7).join(tuples))
    return f"INSERT INTO {table} ({columns})\nVALUES {values}"


@delayed
def write_trino(
    df: pd.DataFrame,
    name: str,
    connection_kwargs: dict,
):
    # do a single INSERT statement with multiple VALUES
    engine = create_engine(URL(**connection_kwargs))
    with engine.connect() as conn:
        conn.execute(text(df_to_sql_bulk_insert(df, name)))


@delayed
def create_table_if_not_exists(
    df: pd.DataFrame,
    name: str,
    connection_kwargs,
):
    sql = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE 
            table_catalog = '{connection_kwargs.get('catalog', 'system')}'
        AND table_schema = '{connection_kwargs.get('schema', 'runtime')}'
        AND table_name = '{name}'
    """
    engine = create_engine(URL(**connection_kwargs))
    with engine.connect() as conn:
        if conn.execute(text(sql)).fetchall()[0][0] == 0:
            print('table does not exist, creating it')
            df.to_sql(
                name=name,
                schema=connection_kwargs.get("schema", None),
                con=engine,
                index=False,
                if_exists="fail",
            )


def to_trino(
    df: dd.DataFrame,
    name: str,
    connection_kwargs: dict,
):
    """Write a Dask DataFrame to a trino table.

    Parameters
    ----------
    df:
        Dask DataFrame to save.
    name:
        Name of the table to save to.
    connection_kwargs:
        Connection arguments used when connecting to trino.
    Examples
    --------

    >>> from dask_trino import to_trino
    >>> df = ...  # Create a Dask DataFrame
    >>> to_trino(
    ...     df,
    ...     name="my_table",
    ...     connection_kwargs={
    ...         "user": "...",
    ...         "password": "...",
    ...     },
    ... )

    """
    # create table first if necessary before writing partitions
    create_table_if_not_exists(df._meta, name, connection_kwargs).compute()
    parts = [
        write_trino(partition, name, connection_kwargs)
        for partition in df.to_delayed()
    ]
    dask.compute(parts)


def _fetch_segments(segments, row_mapper, columns):
    # TODO: why is this being called after the dask
    # dataframe has been created again?
    # for example df_out = read_trino(...)
    # now if you call df_out['col_name'] it will enter
    # into this function again with columns = ['col_name']
    df_columns = [column['name'] if isinstance(column, dict) else column for column in columns]
    dataframes = []    
    for segment in segments:
        rows = list(SegmentIterator(segment, row_mapper))
        dataframes.append(pd.DataFrame(rows, columns=df_columns))
    
    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame(columns=df_columns)


def _simple_partition_segments(segments, npartitions: None | int = None):
    # split segments into npartitions lists
    if npartitions is None:
        return [segments]
    segments_partitioned = [[] for _ in range(npartitions)]
    for i, segment in enumerate(segments):
        segments_partitioned[i % npartitions].append(segment)
    return segments_partitioned


def read_trino(
    query: str,
    *,
    connection_kwargs: dict,
    npartitions: int | None = None,
) -> dd.DataFrame:
    """Load a Dask DataFrame based on the result of a trino query.

    Parameters
    ----------
    query:
        The trino query to execute.
    connection_kwargs:
        Connection arguments used when connecting to trino.
    npartitions: int
        An integer number of partitions for the target Dask DataFrame.

    Examples
    --------

    >>> from dask_trino import read_trino
    >>> example_query = '''
    ...    SELECT *
    ...    TPCH.SF1.CUSTOMER;
    ... '''
    >>> ddf = read_trino(
    ...     query=example_query,
    ...     connection_kwargs={
    ...         "user": "...",
    ...         "password": "...",
    ...     },
    ... )

    """

    connection = trino.dbapi.Connection(**connection_kwargs)
    cur = connection.cursor('segment')
    cur.execute(query)
    segments = cur.fetchall()
    columns = cur._query.columns
    row_mapper = RowMapperFactory().create(columns=columns, legacy_primitive_types=False)
    
    # segments is the list of segments we want to read 
    # from object storage. this will be done by dask in parallel.
    # if there are no segments, we return an empty DataFrame
    if len(segments) == 0:
        return dd.from_pandas(pd.DataFrame(), npartitions=1)
    
    # Read the first segment to determine meta, which is useful for a
    # better size estimate when partitioning maybe?
    # We don't need this for now but will leave this here for future reference.
    #meta = _fetch_segments([segments[0]], row_mapper, columns)

    segments_partitioned = _simple_partition_segments(segments, npartitions)
    
    return dd.from_map(
        partial(_fetch_segments, row_mapper=row_mapper, columns=columns),
        segments_partitioned,
    )