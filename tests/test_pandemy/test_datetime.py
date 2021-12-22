"""Tests for the datetime module of pandemy.

The datetime module handles timezone conversions.
"""

# =================================================
# Imports
# =================================================

# Standard Library
import io

# Third Party
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

# Local
import pandemy
import pandemy.datetime

# =================================================
# Setup
# =================================================

# The csv delimiter for reading DataFrames from io.StringIO
CSV_DELIM = ';'

@pytest.fixture()
def df_datetime() -> pd.DataFrame:
    r"""A DataFrame with naive datetime columns.

    Used for testing to localize and convert the datetime columns
    to a desired timezone.
    """

    data = io.StringIO(
        """
Id;Datetime1;Datetime2;Datetime3;Price;City;DatetimeAsString
1;2008-12-12;2012-12-12 12:12:12;1990-07-14 07:14;123;Falador;1972-08-27 00:02
2;2009-06-06 09:06:08;1977-08-16 14:31;1959-08-14;9000;Yanille;1976-11-28 01:02
3;2002-02-14 18:39:00;1865-10-29 15:21:22;1987-08-22 21:33;4250;Varrock;1993-07-03
        """
    )

    dtypes = {
        'Id': 'int16',
        'Price': 'string',
        'City': 'string',
        'DatetimeAsString': 'string',
    }

    datetime_cols = ['Datetime1', 'Datetime2', 'Datetime3']

    return pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='Id',
                       parse_dates=datetime_cols, dtype=dtypes)

# =================================================
# Tests
# =================================================


class TestDatetimeColumnsToTimezone:
    r"""
    Test the function datetime_columns_to_timezone
    from the pandemy.datetime module.

    Fixtures
    --------
    df_datetime : pd.DataFrame
       Input DataFrame to localize and convert to desired timezone.
    """

    @pytest.mark.parametrize('localize_tz', [pytest.param('UTC', id='UTC'),
                                             pytest.param('CET', id='CET')])
    def test_localize_to_tz(self, localize_tz, df_datetime):
        r"""Localize naive datetime columns to specified timezone.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.
        """

        # Setup
        # ===========================================================
        df_exp_result = df_datetime.copy()
        datetime_cols = df_datetime.select_dtypes(include=['datetime']).columns
        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)

        # Exercise
        # ===========================================================
        pandemy.datetime.datetime_columns_to_timezone(df=df_datetime,
                                                      localize_tz=localize_tz, target_tz=None)

        # Verify
        # ===========================================================
        assert_frame_equal(df_datetime, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_localize_to_tz_no_datetime_columns(self, df_datetime):
        r"""Localize naive datetime columns to CET timezone.

        No naive datetime columns are found in the input DataFrame.
        No columns of the input DataFrame should be modified.
        """

        # Setup
        # ===========================================================
        datetime_cols = df_datetime.select_dtypes(include=['datetime']).columns

        # Make all datetime columns string columns
        df_datetime = df_datetime.astype({col: pd.StringDtype() for col in datetime_cols})
        df_exp_result = df_datetime.copy()

        # Exercise
        # ===========================================================
        pandemy.datetime.datetime_columns_to_timezone(df=df_datetime,
                                                      localize_tz='UTC', target_tz='CET')

        # Verify
        # ===========================================================
        assert_frame_equal(df_datetime, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('localize_tz, target_tz',
                             [pytest.param('UTC', 'CET', id='localize=UTC, target=CET'),
                              pytest.param('UTC', 'UTC', id='localize=UTC, target=UTC'),
                              pytest.param('UTC', None, id='localize=UTC, target=None')])
    def test_localize_and_convert_to_tz(self, localize_tz, target_tz, df_datetime):
        r"""
        Localize naive datetime columns to a timezone and then convert them
        into the target timezone.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.

        target_tz : str or None, default 'CET'
            Name of the target timezone of the timezone aware columns.
            If `target_tz` is None or `target_tz = `localize_tz`
            no timezone conversion will be performed.
        """

        # Setup
        # ===========================================================
        df_exp_result = df_datetime.copy()
        datetime_cols = df_datetime.select_dtypes(include=['datetime']).columns

        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)
            if target_tz is not None or target_tz == localize_tz:
                df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_convert(target_tz)

        # Exercise
        # ===========================================================
        pandemy.datetime.datetime_columns_to_timezone(df=df_datetime,
                                                      localize_tz=localize_tz, target_tz=target_tz)

        # Verify
        # ===========================================================
        assert_frame_equal(df_datetime, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('localize_tz, target_tz',
                             [pytest.param('UT', 'CET', id='localize=UT, target=CET'),
                              pytest.param('UTC', 'T', id='localize=UTC, target=T'),
                              pytest.param(None, None, id='localize=None, target=None')])
    def test_localize_and_convert_with_unknown_timezone(self, localize_tz, target_tz, df_datetime):
        r"""
        Try to localize and convert naive datetime columns when supplying an
        unknown timezone.

        pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.

        target_tz : str or None, default 'CET'
            Name of the target timezone of the timezone aware columns.
            If `target_tz` is None or `target_tz = `localize_tz`
            no timezone conversion will be performed.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy.datetime.datetime_columns_to_timezone(df=df_datetime,
                                                          localize_tz=localize_tz, target_tz=target_tz)

        # Clean up - None
        # ===========================================================
