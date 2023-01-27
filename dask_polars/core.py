import numbers
import operator
from math import ceil

import dask
import polars as pl
from dask.utils import apply, funcname


def create_empty_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create an empty polars DataFrame without increasing the reference count

    Parameters
    ----------
    df
        DataFrame to create an empty from
    """
    return pl.DataFrame(
        [pl.Series(name, [], dtype=dtype) for name, dtype in zip(df.columns, df.dtypes)]
    )


class DataFrame(dask.base.DaskMethodsMixin):
    def __init__(self, name: str, graph: dict, meta: pl.DataFrame, npartitions: int):
        self._name = name
        self._graph = graph
        # also used as identity in folds
        self._meta = meta
        self.npartitions = npartitions

    def __dask_graph__(self):
        return self._graph

    def __dask_keys__(self):
        return [(self._name, i) for i in range(self.npartitions)]

    @staticmethod
    def __dask_optimize__(graph, keys):
        return graph

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __dask_postcompute__(self):
        return pl.concat, ()

    def __dask_tokenize__(self):
        return self._name

    def map_partitions(self, func, *args, **kwargs):
        name = funcname(func) + "-" + dask.base.tokenize(self, func, **kwargs)
        graph = {
            (name, i): (apply, func, [key] + list(args), kwargs)
            for i, key in enumerate(self.__dask_keys__())
        }
        meta = func(self._meta, *args, **kwargs)
        return DataFrame(name, {**self._graph, **graph}, meta, self.npartitions)

    def __add__(self, other):
        if not isinstance(other, numbers.Number):
            return NotImplemented
        return self.map_partitions(operator.add, other)

    def head(self, length: int = 5):
        name = "head-" + dask.base.tokenize(self, length)
        graph = {(name, 0): (pl.DataFrame.head, self._graph[(self._name, 0)], length)}
        return DataFrame(name, {**self._graph, **graph}, self._meta, 1)

    def sum(self):
        tmp = self.map_partitions(pl.DataFrame.sum)
        name = "sum-" + dask.base.tokenize(tmp)
        graph = {(name, 0): (pl.DataFrame.sum, (pl.concat, tmp.__dask_keys__()))}
        return DataFrame(name, {**tmp._graph, **graph}, self._meta.sum(), 1)

    def __repr__(self):
        return self.head().compute().__repr__()


def from_polars(df: pl.DataFrame, npartitions: int = 1) -> DataFrame:
    name = "from-polars-" + dask.base.tokenize(df, npartitions)
    nrows = len(df)
    chunksize = ceil(nrows / npartitions)
    locations = [i for i in range(0, nrows, chunksize)] + [nrows]
    graph = {
        (name, i): df[start:stop]
        for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
    }

    return DataFrame(name, graph, create_empty_df(df), npartitions)


def from_dask_dataframe(df) -> DataFrame:
    if df._meta is None:
        raise ValueError("DataFrame metadata required for determining dtype")

    token = dask.base.tokenize(df)
    name = f"from-dask-dataframe-{token}"
    graph = {
        (name, i): (pl.from_pandas, key) for i, key in enumerate(df.__dask_keys__())
    }
    meta = pl.from_pandas(df._meta)
    return DataFrame(name, {**(df.__dask_graph__()), **graph}, meta, df.npartitions)


def to_dask_dataframe(df: DataFrame):
    from dask.dataframe.core import new_dd_object

    if df._meta is None:
        raise ValueError("DataFrame metadata required for determining dtype")

    token = dask.base.tokenize(df)
    name = f"to-dask-dataframe-{token}"
    graph = {
        (name, i): (lambda df: df.to_pandas(), key)
        for i, key in enumerate(df.__dask_keys__())
    }
    meta = df._meta.to_pandas()
    divisions = [None] * (df.npartitions + 1)

    return new_dd_object(
        {**(df.__dask_graph__()), **graph}, name, meta, divisions=divisions
    )
