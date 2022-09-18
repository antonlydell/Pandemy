r"""Tests for the _dataframe internal module of Pandemy.

The _dataframe module handles operations on and transformations of pandas DataFrames.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library

# Third Party
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

# Local
import pandemy._dataframe

# ===============================================================
# Tests
# ===============================================================


class TestConvertNaNToNone:
    r"""Test the function `convert_nan_to_none`.

    The function converts NaN values (numpy.nan, pandas.NA, pandas.NaT)
    to standard Python None.

    Fixtures
    --------
    df_customer : pd.DataFrame
        The table Customer of the test database Runescape.db.

    df_item_traded_in_store : pd.DataFrame
        The table ItemTradedInStore of the test database Runescape.db.
    """

    def test_0_nan_values(self, df_customer):
        r"""Test a DataFrame with 0 NaN values.

        The returned DataFrame should be equal to the input DataFrame.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        df_converted = pandemy._dataframe.convert_nan_to_none(df=df_customer)

        # Verify
        # ===========================================================
        assert_frame_equal(df_converted, df_customer)

        # Clean up - None
        # ===========================================================

    def test_6_nan_values(self, df_item_traded_in_store):
        r"""Test DataFrame with 6 NaN values.

        The input DataFrame contains a mixture of numpy.nan, pandas.NA and pandas.NaT.
        The NaN values of the returned DataFrame should be replaced by None.
        """

        # Setup
        # ===========================================================
        df_exp_result = df_item_traded_in_store.copy()

        df_item_traded_in_store.loc[1, 'StoreId'] = pd.NA
        df_item_traded_in_store.loc[2, 'ItemId'] = pd.NA
        df_item_traded_in_store.loc[2, 'TransactionTimestamp'] = pd.NaT
        df_item_traded_in_store.loc[7, 'TransactionTimestamp'] = pd.NaT
        df_item_traded_in_store.loc[4, 'TradePricePerItem'] = np.nan
        df_item_traded_in_store.loc[5, 'TotalTradePrice'] = np.nan

        df_exp_result = df_exp_result.astype(
            {
                'StoreId': object,
                'ItemId': object,
                'TransactionTimestamp': object,
                'TradePricePerItem': object,
                'TotalTradePrice': object
            }
        )

        df_exp_result.loc[1, 'StoreId'] = None
        df_exp_result.loc[2, 'ItemId'] = None
        df_exp_result.loc[2, 'TransactionTimestamp'] = None
        df_exp_result.loc[7, 'TransactionTimestamp'] = None
        df_exp_result.loc[4, 'TradePricePerItem'] = None
        df_exp_result.loc[5, 'TotalTradePrice'] = None

        # Exercise
        # ===========================================================
        df_converted = pandemy._dataframe.convert_nan_to_none(df=df_item_traded_in_store)

        # Verify
        # ===========================================================
        assert_frame_equal(df_converted, df_exp_result)

        # Clean up - None
        # ===========================================================


class TestDfToParametersInChunks:
    r"""Test the function `df_to_parameters_in_chunks`.

    The function converts a DataFrame into an iterator yielding a list of dicts,
    where each dict is a row of the DataFrame.

    Fixtures
    --------
    df_item_traded_in_store : pd.DataFrame
        The table ItemTradedInStore of the test database Runescape.db.
    """

    def test_chunksize_None(self, df_item_traded_in_store):
        r"""Test to process a DataFrame with `chunksize=None`.

        The entire DataFrame is expected to be returned in one chunk.
        """

        # Setup
        # ===========================================================
        total_nr_rows_exp: int = df_item_traded_in_store.shape[0]

        # Exercise
        # ===========================================================
        params_iter = pandemy._dataframe.df_to_parameters_in_chunks(df=df_item_traded_in_store, chunksize=None)

        # Verify
        # ===========================================================
        for iter, params in enumerate(params_iter, start=1):
            assert len(params) == total_nr_rows_exp

        assert iter == 1

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'chunksize, chunks, nr_chunks_exp',
        (
            pytest.param(2, {1: 2, 2: 2, 3: 2, 4: 1}, 4, id='chunksize=2'),
            pytest.param(3, {1: 3, 2: 3, 3: 1}, 3, id='chunksize=3'),
            pytest.param(4, {1: 4, 2: 3}, 2, id='chunksize=4'),
            pytest.param(10, {1: 7}, 1, id='chunksize=10')
        )
    )
    def test_chunksize(self, chunksize, chunks, nr_chunks_exp, df_item_traded_in_store):
        r"""Test to process a DataFrame with different values of `chunksize`.

        Parameters
        ----------
        chunksize: int
            The number of rows from the DataFrame to return in each chunk.

        chunks: dict of int
            The number of rows expected to be returned in each chunk.

        nr_chunks_exp: int
            The number of chunks expected from the iterator.
        """

        # Setup
        # ===========================================================
        total_chunk_length: int = 0
        total_nr_rows_exp: int = df_item_traded_in_store.shape[0]

        # Exercise
        # ===========================================================
        params_iter = pandemy._dataframe.df_to_parameters_in_chunks(df=df_item_traded_in_store, chunksize=chunksize)

        # Verify
        # ===========================================================
        for iter, params in enumerate(params_iter, start=1):
            chunk_length = len(params)
            assert chunk_length == chunks[iter]
            total_chunk_length += chunk_length

        assert iter == nr_chunks_exp
        assert total_chunk_length == total_nr_rows_exp  # All rows should have been processed

        # Clean up - None
        # ===========================================================
