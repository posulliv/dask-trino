from functools import partial
from typing import Any
from typing import List
from typing import Optional
from typing import Union
from collections.abc import Iterator

import trino
from trino.mapper import RowMapperFactory
from trino.client import DecodableSegment, SpooledSegment, SegmentDecoder, CompressedQueryDataDecoderFactory
from trino.mapper import RowMapper
from trino.sqlalchemy import URL
from sqlalchemy import create_engine, text

import pandas as pd

import dask
import dask.dataframe as dd
from dask.delayed import delayed

MAX_QUERY_LENGTH = 1000000


def df_to_sql_bulk_insert(df: pd.DataFrame, table: str) -> list:
    """Converts a DataFrame to multiple bulk INSERT SQL statements,
    ensuring each stays within MAX_QUERY_LENGTH."""

    # Replace NaN with None for SQL NULL conversion
    df = df.where(pd.notna(df), None)
    columns = ", ".join(df.columns)
    prefix = f"INSERT INTO {table} ({columns}) VALUES "
    # Convert DataFrame rows into formatted tuples
    tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
    values_list = [str(row).replace("None", "NULL") for row in tuples]

    queries = []
    current_query = prefix

    for values in values_list:
        row_length = len(values) + 1  # Account for ", " or ")"
        current_length = len(current_query)
        if current_length + row_length > MAX_QUERY_LENGTH - 1:
            queries.append(current_query)
            current_query = prefix + values
        else:
            if current_query == prefix:
                current_query += values
            else:
                current_query += ",\n" + values
    # if we just have one query that was under MAX_QUERY_LENGTH
    # add it to the list of queries
    if current_query and len(queries) == 0:
        queries.append(current_query)

    return queries


@delayed
def write_trino(
    df: pd.DataFrame,
    name: str,
    connection_kwargs: dict,
):
    engine = create_engine(URL(**connection_kwargs))
    with engine.connect() as conn:
        insert_queries = df_to_sql_bulk_insert(df, name)
        for query in insert_queries:
            conn.execute(text(query))


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


class CustomSegmentIterator:
    def __init__(self, segments: Union[DecodableSegment, List[DecodableSegment]], mapper: RowMapper) -> None:
        self._segments = iter(segments if isinstance(segments, List) else [segments])
        self._mapper = mapper
        self._decoder = None
        self._rows: Iterator[List[List[Any]]] = iter([])
        self._finished = False
        self._current_segment: Optional[DecodableSegment] = None

    def __iter__(self) -> Iterator[List[Any]]:
        return self

    def __next__(self) -> List[Any]:
        # If rows are exhausted, fetch the next segment
        while True:
            try:
                return next(self._rows)
            except StopIteration:
                if self._finished:
                    raise StopIteration
                self._load_next_segment()

    def _load_next_segment(self):
        try:
            # TODO - client needs to delete segments from storage
            # causes issues with _fetch_segments method if 
            # segments are deleted.
            #if self._current_segment:
             #   segment = self._current_segment.segment
                #if isinstance(segment, SpooledSegment):
                #    segment.acknowledge()

            self._current_segment = next(self._segments)
            if self._decoder is None:
                self._decoder = SegmentDecoder(CompressedQueryDataDecoderFactory(self._mapper)
                                               .create(self._current_segment.encoding))
            self._rows = iter(self._decoder.decode(self._current_segment.segment))
        except StopIteration:
            self._finished = True


def _fetch_segments(segments, row_mapper, columns):
    # TODO: why is this being called after the dask
    # dataframe has been created again?
    # for example df_out = read_trino(...)
    # now if you call df_out['col_name'] it will enter
    # into this function again with columns = ['col_name']
    df_columns = [column['name'] if isinstance(column, dict) else column for column in columns]
    dataframes = []    
    for segment in segments:
        rows = list(CustomSegmentIterator(segment, row_mapper))
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
