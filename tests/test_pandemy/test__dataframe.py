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
