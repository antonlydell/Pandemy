r"""Tests for the _datetime module of Pandemy.

The _datetime module handles datetime operations and timezone conversions.
"""

# =================================================
# Imports
# =================================================

# Standard Library
import io

# Third Party
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import pytest
from typing import Dict, List, Tuple

# Local
import pandemy
import pandemy._datetime

# =================================================
# Setup
# =================================================

# The csv delimiter for reading DataFrames from io.StringIO
CSV_DELIM = ';'

@pytest.fixture()
def df_datetime() -> Tuple[pd.DataFrame, List[str]]:
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

    df = pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='Id',
                     parse_dates=datetime_cols, dtype=dtypes)
    
    return df, datetime_cols


@pytest.fixture()
def df_datetime_tz_aware(df_datetime: Tuple[pd.DataFrame, List[str]]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    r"""A DataFrame with timezone aware columns.

    Used for testing to localize and convert the datetime columns
    to a desired timezone.
    """

    df, datetime_cols = df_datetime
    df_output = df.copy()

    col_tz = {
        datetime_cols[0]: 'EET',
        datetime_cols[1]: 'UTC',
        datetime_cols[2]: 'America/Denver'
    }

    for col, tz in col_tz.items():
        df_output[col] = df_output[col].dt.tz_localize(tz)

    return df_output, col_tz


@pytest.fixture()
def df_datetime_naive_and_tz_aware(df_datetime: Tuple[pd.DataFrame, List[str]]) -> Tuple[pd.DataFrame, Dict[str, str]]:
    r"""A DataFrame with a mixture of datetime columns that are naive and timezone aware.

    Used for testing to localize and convert the datetime columns
    to a desired timezone.
    """

    df, datetime_cols = df_datetime
    df_output = df.copy()

    cols_tz = {
        datetime_cols[0]: None, 
        datetime_cols[1]: 'UTC',
        datetime_cols[2]: 'America/Denver'
    }

    for col, tz in cols_tz.items():
        df_output[col] = df_output[col].dt.tz_localize(tz)

    return df_output, cols_tz


# =================================================
# Tests
# =================================================


class TestDatetimeColumnsToTimezone:
    r"""Test the function `datetime_columns_to_timezone`.

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
        df, datetime_cols = df_datetime
        df_exp_result = df.copy()

        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)

        # Exercise
        # ===========================================================
        pandemy._datetime.datetime_columns_to_timezone(df=df, localize_tz=localize_tz, target_tz=None)

        # Verify
        # ===========================================================
        assert_frame_equal(df, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_localize_to_tz_no_datetime_columns(self, df_datetime):
        r"""Localize naive datetime columns to CET timezone.

        No naive datetime columns are found in the input DataFrame.
        No columns of the input DataFrame should be modified.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime

        # Make all datetime columns string columns
        df = df.astype({col: pd.StringDtype() for col in datetime_cols})
        df_exp_result = df.copy()

        # Exercise
        # ===========================================================
        pandemy._datetime.datetime_columns_to_timezone(df=df, localize_tz='UTC', target_tz='CET')

        # Verify
        # ===========================================================
        assert_frame_equal(df, df_exp_result)

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
        df, datetime_cols = df_datetime
        df_exp_result = df.copy()

        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)
            if target_tz is not None or target_tz == localize_tz:
                df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_convert(target_tz)

        # Exercise
        # ===========================================================
        pandemy._datetime.datetime_columns_to_timezone(df=df, localize_tz=localize_tz, target_tz=target_tz)

        # Verify
        # ===========================================================
        assert_frame_equal(df, df_exp_result)

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

        # Setup
        # ===========================================================
        df, _ = df_datetime

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy._datetime.datetime_columns_to_timezone(df=df, localize_tz=localize_tz, target_tz=target_tz)

        # Clean up - None
        # ===========================================================

class TestConvertDatetimeColumns:
    r"""Test the function `convert_datetime_columns`.

    Tests the localization and timezone conversion without converting
    the data type to string or integer.

    Fixtures
    --------
    df_datetime : pd.DataFrame
       Input DataFrame to localize and convert to desired timezone.
    
    df_datetime_tz_aware : pd.DataFrame
        A DataFrame with timezone aware columns.

     df_datetime_naive_and_tz_aware: pd.DataFrame
        A DataFrame with a mixture of datetime columns that are naive and timezone aware.
    """

    @pytest.mark.parametrize(
        'localize_tz',
        (
            pytest.param('UTC', id='UTC'),
            pytest.param('CET', id='CET')
        )
    )
    def test_localize_tz_naive_datetime_columns(self, localize_tz, df_datetime):
        r"""Localize a DataFrame with naive datetime columns to specified timezone.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        df_exp_result = df.copy()

        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=None
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'localize_tz',
        (
            pytest.param('UTC', id='UTC'),
            pytest.param('CET', id='CET')
        )
    )
    def test_localize_tz_tz_aware_datetime_columns(self, localize_tz, df_datetime_tz_aware):
        r"""Localize a DataFrame with timezone aware datetime columns to specified timezone.

        The timezone aware datetime columns should not be modified since the already have a timezone.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize datetime columns into.
        """

        # Setup
        # ===========================================================
        df, _ = df_datetime_tz_aware

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=None
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'localize_tz',
        (
            pytest.param('UTC', id='UTC'),
            pytest.param('CET', id='CET')
        )
    )
    def test_localize_tz_naive_and_tz_aware_datetime_columns(self, localize_tz, df_datetime_naive_and_tz_aware):
        r"""Localize a DataFrame with naive and timezone aware datetime columns to specified timezone.

        Only the naive datetime columns should be localized.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize datetime columns into.
        """

        # Setup
        # ===========================================================
        df, _ = df_datetime_naive_and_tz_aware
        df_exp_result = df.copy()
        df_exp_result.loc[:, 'Datetime1'] = df_exp_result['Datetime1'].dt.tz_localize(localize_tz)

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=None
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'localize_tz, target_tz',
        (
            pytest.param('UTC', None, id='loc_tz=UTC, t_tz=None'),
            pytest.param('UTC', 'CET', id='loc_tz=UTC, t_tz=CET'),
        )
    )
    def test_localize_tz_and_target_tz_no_datetime_columns(self, localize_tz, target_tz, df_datetime):
        r"""Test to localize and convert to target timezone when the DataFrame has no datetime columns.

        No columns of the input DataFrame should be modified.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize datetime columns into.

        target_tz : str or None
            Name of the target timezone to convert datetime columns into.
            If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime

        # Make all datetime columns string columns
        df = df.astype({col: 'string' for col in datetime_cols})

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=target_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'localize_tz, target_tz',
        (
            pytest.param('UTC', 'CET', id='loc_tz=UTC, t_tz=CET'),
            pytest.param('UTC', 'UTC', id='loc_tz=UTC, t_tz=UTC'),
            pytest.param('UTC', None, id='loc_tz=UTC, t_tz=None')
        )
    )
    def test_localize_tz_and_target_tz_naive_datetime_columns(self, localize_tz, target_tz, df_datetime):
        r"""Localize naive datetime columns and then convert to the target timezone.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.

        target_tz : str or None
            Name of the target timezone to convert datetime columns into.
            If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        df_exp_result = df.copy()

        for col in datetime_cols:
            df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_localize(localize_tz)
            if target_tz is not None or target_tz == localize_tz:
                df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_convert(target_tz)

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=target_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'localize_tz, target_tz',
        (
            pytest.param('CET', 'UTC', id='loc_tz=CET, t_tz=UTC'),
            pytest.param('UTC', 'UTC', id='loc_tz=UTC, t_tz=UTC'),
            pytest.param('UTC', None, id='loc_tz=UTC, t_tz=None')
        )
    )
    def test_localize_tz_and_target_tz_naive_and_tz_aware_datetime_columns(
        self, localize_tz, target_tz, df_datetime_naive_and_tz_aware):
        r"""Localize and convert to desired timezone a DataFrame with naive and timezone aware datetime columns.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.

        target_tz : str or None
            Name of the target timezone to convert datetime columns into.
            If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime_naive_and_tz_aware
        df_exp_result = df.copy()
        df_exp_result.loc[:, 'Datetime1'] = df_exp_result['Datetime1'].dt.tz_localize(localize_tz)

        for col in datetime_cols:
            if target_tz is not None or target_tz == localize_tz:
                df_exp_result.loc[:, col] = df_exp_result[col].dt.tz_convert(target_tz)

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype=None,
            localize_tz=localize_tz,
            target_tz=target_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'localize_tz, target_tz',
        (
            pytest.param('UT', 'CET', id='loc_tz=UT, t_tz=CET'),
            pytest.param('UTC', 'T', id='loc_tz=UTC, t_tz=T'),
        )
    )
    def test_localize_tz_and_target_tz_with_unknown_timezone(self, localize_tz, target_tz, df_datetime):
        r"""Test to localize and convert naive datetime columns when supplying an unknown timezone.

        pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        localize_tz : str
            Name of the timezone which to localize naive datetime columns into.

        target_tz : str or None
            Name of the target timezone to convert datetime columns into.
            If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.
        """

        # Setup
        # ===========================================================
        df, _ = df_datetime

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy._datetime.convert_datetime_columns(
                df=df,
                dtype=None,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # Clean up - None
        # ===========================================================

class TestConvertDatetimeColumnsToStrAndInt:
    r"""Test the function `convert_datetime_columns`.

    Tests the localization and timezone conversion when converting
    the data type to string or integer.

    Fixtures
    --------
    df_datetime : pd.DataFrame
       Input DataFrame with datetime columns.
    
    df_datetime_tz_aware : Tuple[pd.DataFrame, Dict[str, str]]
        A DataFrame with timezone aware columns.   

    df_datetime_naive_and_tz_aware : Tuple[pd.DataFrame, Dict[str, str]]
        A DataFrame with a mixture of datetime columns that are naive and timezone aware.
    """

    def test_datetime_to_str(self, df_datetime):
        r"""Convert naive datetime columns to string."""

        # Setup
        # ===========================================================
        datetime_format = r'%Y-%m-%d'

        df, _ = df_datetime
        df_exp_result = df.copy()

        df_exp_result['Datetime1'] = df_exp_result['Datetime1'].dt.strftime(datetime_format).astype('string')
        df_exp_result['Datetime2'] = df_exp_result['Datetime2'].dt.strftime(datetime_format).astype('string')
        df_exp_result['Datetime3'] = df_exp_result['Datetime3'].dt.strftime(datetime_format).astype('string')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='str', datetime_format=datetime_format)

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_naive_datetime_to_str_localize_tz(self, df_datetime):
        r"""Convert naive datetime columns to string format and localize the timezone to CET."""

        # Setup
        # ===========================================================
        datetime_format = r'%Y-%m-%d+%z'

        df, _ = df_datetime
        df_exp_result = df.copy()

        df_exp_result['Datetime1'] = df_exp_result['Datetime1'].dt.tz_localize('CET').dt.strftime(datetime_format).astype('string')
        df_exp_result['Datetime2'] = df_exp_result['Datetime2'].dt.tz_localize('CET').dt.strftime(datetime_format).astype('string')
        df_exp_result['Datetime3'] = df_exp_result['Datetime3'].dt.tz_localize('CET').dt.strftime(datetime_format).astype('string')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype='str',
            datetime_format=datetime_format,
            localize_tz='CET'
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_naive_datetime_to_str_localize_tz_and_target_tz(self, df_datetime):
        r"""Localize naive datetime columns and convert to desired timezone in string format."""

        # Setup
        # ===========================================================
        datetime_format : str = r'%Y-%m-%d %H:%M:%S%z'
        localize_tz : str = 'CET'
        target_tz : str = 'UTC'

        df, _ = df_datetime
        df_exp_result = df.copy()

        df_exp_result['Datetime1'] = (
            df_exp_result['Datetime1']
                .dt.tz_localize(localize_tz)
                .dt.tz_convert(target_tz)
                .dt.strftime(datetime_format)
                .astype('string')
        )
        df_exp_result['Datetime2'] = (
            df_exp_result['Datetime2']
                .dt.tz_localize(localize_tz)
                .dt.tz_convert(target_tz)
                .dt.strftime(datetime_format)
                .astype('string')
        )
        df_exp_result['Datetime3'] = (
            df_exp_result['Datetime3']
                .dt.tz_localize(localize_tz)
                .dt.tz_convert(target_tz)
                .dt.strftime(datetime_format)
                .astype('string')
        )

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype='str',
            datetime_format=datetime_format,
            localize_tz=localize_tz,
            target_tz=target_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_tz_aware_datetime_to_str_localize_tz(self, df_datetime_tz_aware):
        r"""Convert timezone aware datetime columns to string format.
        
        The parameter `localize_tz` is set to 'CET', but should have no effect
        since the datetime columns are already timezone aware.
        """

        # Setup
        # ===========================================================
        datetime_format : str = r'%Y-%m-%d %H:%M:%S%z'
        localize_tz : str = 'CET'

        df, col_tz = df_datetime_tz_aware
        df_exp_result = df.copy()

        for col in col_tz.keys():
            df_exp_result[col] = df_exp_result[col].dt.strftime(datetime_format).astype('string')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype='str',
            datetime_format=datetime_format,
            localize_tz=localize_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    def test_naive_and_tz_aware_datetime_to_str_localize_tz_and_target_tz(self, df_datetime_naive_and_tz_aware):
        r"""Convert naive and timezone aware datetime columns to string format.
        
        The parameter `localize_tz` is set to 'CET', and `target_tz` is set to 'UTC'.
        One naive column should be localized to CET and then converted to to UTC.
        The other two timezone aware columns should be converted to UTC without being localized first.
        """

        # Setup
        # ===========================================================
        datetime_format : str = r'%Y-%m-%d %H:%M:%S%z'
        localize_tz : str = 'CET'
        target_tz : str = 'UTC'

        df, col_tz = df_datetime_naive_and_tz_aware
        df_exp_result = df.copy()

        for col, tz in col_tz.items():
            if tz is None:
                df_exp_result[col] = df_exp_result[col].dt.tz_localize(localize_tz)

            df_exp_result[col] = df_exp_result[col].dt.tz_convert(target_tz)
            df_exp_result[col] = df_exp_result[col].dt.strftime(datetime_format).astype('string')


        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(
            df=df,
            dtype='str',
            datetime_format=datetime_format,
            localize_tz=localize_tz,
            target_tz=target_tz
        )

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_naive_and_tz_aware_datetime_to_str_target_tz_no_localize_tz(self, df_datetime_naive_and_tz_aware):
        r"""Convert naive and timezone aware datetime columns to string format without localizing.
        
        The parameter `localize_tz` is not specified, and `target_tz` is set to 'UTC'.
        The naive datetime column Datetime1 should raise pandemy.InvalidInputError
        since it is not possible to convert the timezone of a naive datetime column.
        """

        # Setup
        # ===========================================================
        localize_tz = None
        target_tz : str = 'UTC'

        df, _ = df_datetime_naive_and_tz_aware

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy._datetime.convert_datetime_columns(
                df=df,
                dtype='str',
                target_tz=target_tz
            )

        # Verify
        # ===========================================================
        assert 'Datetime1' in exc_info.value.args[0]
        assert f'{target_tz=}' in exc_info.value.args[0]
        assert f'{localize_tz=}' in exc_info.value.args[0]
        assert 'Datetime1' in exc_info.value.data

        # Clean up - None
        # ===========================================================

    def test_naive_datetime_to_int(self, df_datetime):
        r"""Convert naive datetime columns to integer.

        The integer value represents the number of seconds since
        the Unix Epoch of 1970-01-01 in UTC timezone. The naive
        datetime columns are assumed to be in UTC timezone.
        """

        # Setup
        # ===========================================================
        df, _ = df_datetime
        df = df.loc[:, ['Datetime1', 'Datetime2']]

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, 'Datetime1'] = 1229040000   # 2008-12-12 00:00:00 UTC
        df_exp_result.loc[2, 'Datetime1'] = 1244279168   # 2009-06-06 09:06:08 UTC
        df_exp_result.loc[3, 'Datetime1'] = 1013711940   # 2002-02-14 18:39:00 UTC

        df_exp_result.loc[1, 'Datetime2'] = 1355314332   # 2012-12-12 12:12:12 UTC
        df_exp_result.loc[2, 'Datetime2'] = 240589860    # 1977-08-16 14:31:00 UTC
        df_exp_result.loc[3, 'Datetime2'] = -3287378318  # 1865-10-29 15:21:22 UTC

        df_exp_result = df_exp_result.astype('int64')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result, check_exact=True)

        # Clean up - None
        # ===========================================================

    def test_naive_datetime_to_int_localize_tz(self, df_datetime):
        r"""Localize naive datetime columns to UTC timezone and convert to Unix Epoch."""

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        datetime_cols = datetime_cols[:-1]
        df = df.loc[:, datetime_cols]

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, datetime_cols[0]] = 1229040000   # 2008-12-12 00:00:00 UTC
        df_exp_result.loc[2, datetime_cols[0]] = 1244279168   # 2009-06-06 09:06:08 UTC
        df_exp_result.loc[3, datetime_cols[0]] = 1013711940   # 2002-02-14 18:39:00 UTC

        df_exp_result.loc[1, datetime_cols[1]] = 1355314332   # 2012-12-12 12:12:12 UTC
        df_exp_result.loc[2, datetime_cols[1]] = 240589860    # 1977-08-16 14:31:00 UTC
        df_exp_result.loc[3, datetime_cols[1]] = -3287378318  # 1865-10-29 15:21:22 UTC

        df_exp_result = df_exp_result.astype('int64')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int', localize_tz='UTC')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result, check_exact=True)

        # Clean up - None
        # ===========================================================

    def test_tz_aware_datetime_to_int(self, df_datetime):
        r"""Convert UTC timezone aware datetime columns to integer.

        No timezone localization or conversion should be performed
        since `localize_tz` and `target_tz` are not specified.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        datetime_cols = datetime_cols[:-1]
        df = df.loc[:, datetime_cols]

        for col in datetime_cols:
            df.loc[:, col] = df.loc[:, col].dt.tz_localize('UTC')

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, 'Datetime1'] = 1229040000   # 2008-12-12 00:00:00 UTC
        df_exp_result.loc[2, 'Datetime1'] = 1244279168   # 2009-06-06 09:06:08 UTC
        df_exp_result.loc[3, 'Datetime1'] = 1013711940   # 2002-02-14 18:39:00 UTC

        df_exp_result.loc[1, 'Datetime2'] = 1355314332   # 2012-12-12 12:12:12 UTC
        df_exp_result.loc[2, 'Datetime2'] = 240589860    # 1977-08-16 14:31:00 UTC
        df_exp_result.loc[3, 'Datetime2'] = -3287378318  # 1865-10-29 15:21:22 UTC

        df_exp_result = df_exp_result.astype('int64')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result, check_exact=True)

        # Clean up - None
        # ===========================================================

    def test_tz_aware_datetime_to_int_convert_to_target_tz(self, df_datetime):
        r"""Convert timezone aware datetime columns to target timezone and then to integer.

        The target timezone is UTC. Only column Datetime1 of timezone EET
        needs to be converted to UTC.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        datetime_cols = datetime_cols[:-1]
        df = df.loc[:, datetime_cols]

        df[datetime_cols[0]] = df[datetime_cols[0]].dt.tz_localize('EET')
        df[datetime_cols[1]] = df[datetime_cols[1]].dt.tz_localize('UTC')

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, datetime_cols[0]] = 1229032800   # 2008-12-12 00:00:00 EET --> 2008-12-11 22:00:00 UTC
        df_exp_result.loc[2, datetime_cols[0]] = 1244268368   # 2009-06-06 09:06:08 EET --> 2009-06-06 06:06:08 UTC
        df_exp_result.loc[3, datetime_cols[0]] = 1013704740   # 2002-02-14 18:39:00 EET --> 2002-02-14 16:39:00 UTC

        df_exp_result.loc[1, datetime_cols[1]] = 1355314332   # 2012-12-12 12:12:12 UTC
        df_exp_result.loc[2, datetime_cols[1]] = 240589860    # 1977-08-16 14:31:00 UTC
        df_exp_result.loc[3, datetime_cols[1]] = -3287378318  # 1865-10-29 15:21:22 UTC

        df_exp_result = df_exp_result.astype('int64')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int', target_tz='UTC')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result, check_exact=True)

        # Clean up - None
        # ===========================================================

    def test_naive_and_tz_aware_datetime_to_int_localize_tz_and_target_tz(self, df_datetime):
        r"""Convert naive and timezone aware datetime columns to target timezone and then to integer.

        The target timezone is UTC. Column Datetime2 is naive but is of timezone CET. It should be
        localized to CET before being converted to UTC. Column Datetime1 of timezone EET should
        not be converted to CET since it is already timezone aware.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        datetime_cols = datetime_cols[:-1]
        df = df.loc[:, datetime_cols]
        df['Datetime1'] = df['Datetime1'].dt.tz_localize('EET')

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, datetime_cols[0]] = 1229032800   # 2008-12-12 00:00:00 EET --> 2008-12-11 22:00:00 UTC
        df_exp_result.loc[2, datetime_cols[0]] = 1244268368   # 2009-06-06 09:06:08 EET --> 2009-06-06 06:06:08 UTC
        df_exp_result.loc[3, datetime_cols[0]] = 1013704740   # 2002-02-14 18:39:00 EET --> 2002-02-14 16:39:00 UTC

        df_exp_result.loc[1, datetime_cols[1]] = 1355310732   # 2012-12-12 12:12:12 CET --> 2012-12-12 11:12:12 UTC
        df_exp_result.loc[2, datetime_cols[1]] = 240582660    # 1977-08-16 14:31:00 CET --> 1977-08-16 12:31:00 UTC
        df_exp_result.loc[3, datetime_cols[1]] = -3287381918  # 1865-10-29 15:21:22 CET --> 1865-10-29 14:21:22 UTC

        df_exp_result = df_exp_result.astype('int64')

        # Exercise
        # ===========================================================
        df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int', localize_tz='CET', target_tz='UTC')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp_result, check_exact=True)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_convert_non_UTC_timezone_to_int(self, df_datetime):
        r"""Localize a naive datetime column to timezone CET and then convert to Unix Epoch.

        A UserWarning is expected to be raised since the timezone of the column
        being converted to Unix Epoch should be in UTC and not CET.

        Datetime2 of timezone CET is assumed to be in UTC when the conversion to Unix Epoch
        occurs, which means that the Unix Epoch timestamp will be incorrect.
        """

        # Setup
        # ===========================================================
        df, datetime_cols = df_datetime
        datetime_cols = datetime_cols[:-1]
        df = df.loc[:, datetime_cols]
        df['Datetime1'] = df['Datetime1'].dt.tz_localize('UTC')

        df_exp_result = pd.DataFrame(index=df.index, columns=df.columns)

        df_exp_result.loc[1, datetime_cols[0]] = 1229040000   # 2008-12-12 00:00:00 UTC
        df_exp_result.loc[2, datetime_cols[0]] = 1244279168   # 2009-06-06 09:06:08 UTC
        df_exp_result.loc[3, datetime_cols[0]] = 1013711940   # 2002-02-14 18:39:00 UTC

        # Do not assert column Datetime2

        df_exp_result.loc[:, datetime_cols[0]] = df_exp_result[datetime_cols[0]].astype('int64')

        # Exercise
        # ===========================================================
        with pytest.warns(UserWarning):
            df_result = pandemy._datetime.convert_datetime_columns(df=df, dtype='int', localize_tz='CET')

        # Verify
        # ===========================================================
        assert_series_equal(df_result[datetime_cols[0]], df_exp_result[datetime_cols[0]], check_exact=True)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_dtype(self, df_datetime):
        r"""Supply an invalid value to the `dtype` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError, match=r"{'str', 'int', None}"):
            pandemy._datetime.convert_datetime_columns(df=df_datetime, dtype='float')

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_datetime_format(self, df_datetime):
        r"""Supply an invalid datetime format to the `datetime_format` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        df, _ = df_datetime

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError, match='datetime'):
            pandemy._datetime.convert_datetime_columns(df=df, dtype='str', datetime_format=2)

        # Clean up - None
        # ===========================================================
