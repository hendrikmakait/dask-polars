import polars as pl
from polars.testing import assert_frame_equal

import dask_polars as dp

df = pl.DataFrame(
    {
        "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "b": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    }
)
ddf = dp.from_dataframe(df, npartitions=3)


def test_basic():
    assert str(ddf.compute()) == str(df)


def test_meta():
    assert list(ddf._meta.schema.keys()) == df.columns
    assert list(ddf._meta.schema.values()) == df.dtypes


def test_sum():
    assert str(ddf.sum().compute()) == str(df.sum())


def test_add():
    assert str((ddf + 2).compute()) == str(df + 2)

def test_roundtrip():
    actual = dp.from_dask_dataframe(dp.to_dask_dataframe(ddf))
    assert_frame_equal(actual.compute(), ddf.compute(), check_exact=True)
