# dask-trino

This connector is an experimental integration between Dask and Trino. It
only works using the [spooling protocol](https://trinodb.org/docs/current/develop/spooling.html)
introduced in trino [version 466](https://trino.io/docs/current/release/release-466.html).

[![CI](https://github.com/posulliv/dask-trino/actions/workflows/ci.yml/badge.svg)](https://github.com/posulliv/dask-trino/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/dask-trino.svg)](https://badge.fury.io/py/dask-trino)

# Installation

`dask-trino` can be installed with `pip`:

```shell
pip install dask-trino
```

# trino configuration

The spooling protocol is currently disabled by default when you deploy
trino. The [trino docs](https://trino.io/docs/current/admin/configuration.html#spooling-protocol)
outline how to enable it.

One extra consideration for the dask-trino connector is the expiry of 
segments on object storage. By default, when a client acknowledges a segment,
the segment is deleted from object storage. We recommend disabling this
and letting segments be pruned by trino. This can be achieved by adding
this setting to the `spooling-manager.properties` configuration file:

```
fs.segment.explicit-ack=false
```

We recommend this because when working with a dask dataframe, the data
may be requested multiple times. If a segment is deleted after the first
request, the client will not be able to read from the segment again.


# Usage

`dask-trino` provides `read_trino` and `to_trino` methods
for parallel IO from trino with Dask.

```python
>>> from dask_trino import read_trino
>>> example_query = '''
...    SELECT *
...    FROM tpch.sf1.customer
... '''
>>> ddf = read_trino(
...     query=example_query,
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...     },
... )
```

Note that the `to_trino` method generates bulk `INSERT` statements
for each partition of the dataframe. There is no other option
to do bulk insert of data into trino at this time.

```python
>>> from dask_trino import to_trino
>>> to_trino(
...     ddf,
...     name="my_table",
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...     },
... )
```