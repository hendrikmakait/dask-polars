import dask.dataframe as dd
import pandas as pd
import polars as pl
from dask.dataframe.utils import assert_eq as assert_dask_eq
from polars.testing import assert_frame_equal

import dask_polars as dp

df = pl.DataFrame(
    {"a": [1, 2, 3, 4, 5, 6, 7], "b": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]}
)
ddf = dp.from_polars(df, npartitions=3)


def test_basic():
    assert_frame_equal(ddf.compute(), df)


def test_meta():
    assert list(ddf._meta.schema.keys()) == df.columns
    assert list(ddf._meta.schema.values()) == df.dtypes


def test_sum():
    assert str(ddf.sum().compute()) == str(df.sum())


def test_add():
    assert str((ddf + 2).compute()) == str(df + 2)


def test_from_dask_dataframe():
    pandas_df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5, 6, 7], "b": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]}
    )
    dask_df = dd.from_pandas(pandas_df, npartitions=3)
    ddf = dp.from_dask_dataframe(dask_df)
    assert ddf.npartitions == dask_df.npartitions
    assert_frame_equal(ddf.compute(), pl.from_pandas(pandas_df))


def test_to_dask_dataframe():
    dask_df = dp.to_dask_dataframe(ddf)
    assert dask_df.npartitions == ddf.npartitions
    assert_dask_eq(df.to_pandas(), dask_df, check_index=False)
