r"""Internal module that contains functions to handle datetime related operations."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging
from typing import Optional
import warnings

# Third Party
import numpy as np
import pandas as pd

# Local
import pandemy

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# ===============================================================
# Functions
# ===============================================================


def localize_and_convert_timezone(
    s: pd.Series,
    localize_tz: Optional[str] = None,
    target_tz: Optional[str] = None
) -> pd.Series:
    r"""Set a timezone to a naive datetime serie.

    Localize a naive datetime serie to the desired timezone and
    perform an optional conversion to the target timezone.

    Parameters
    ----------
    s : pd.Series
        The datetime serie to localize and convert to desired timezone.

    localize_tz : str or None, default None.
        Name of the timezone which to localize `s` into.
        If None (the default) localization is omitted.

    target_tz : str or None, default None 
        Name of the target timezone to convert `s` into after localization.
        If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.

    Returns
    -------
    s : pd.Series
        `s` localized and converted to desired timezone.

    Raises
    ------
    pandemy.InvalidInputError
        If an unknown timezone is supplied or if trying to convert a timezone of a naive datetime column.
    """

    tz = s.dt.tz
    col_name: str = s.name
    error_msg: str = ''

    try:
        if tz is None and localize_tz is not None:
            s = s.dt.tz_localize(localize_tz)
            tz = s.dt.tz
        if target_tz is not None and target_tz != str(tz):
            if tz is None:
                error_msg = (
                    f'Cannot convert naive datetime column {col_name} to {target_tz=}. '
                    'Supply a timezone to localize_tz to localize the column into its current timezone. '
                    f'{localize_tz=}, {target_tz=}'
                )
                raise pandemy.InvalidInputError(message=error_msg, data=target_tz)
            s = s.dt.tz_convert(target_tz)
    except Exception as e:
        if not error_msg:
            error_msg = f'Column = {col_name} | {type(e).__name__}: {e.args[0]} | {localize_tz=}, {target_tz=}',
        raise pandemy.InvalidInputError(message=error_msg, data=(col_name, e.args)) from None

    return s


def to_unix_epoch(s: pd.Series, target_tz: Optional[str] = None) -> pd.Series:
    r"""Convert a datetime serie to Unix Epoch time.
    
    Parameters
    ----------
    s : pd.Series
        The datetime serie to convert to Unix Epoch time.

    target_tz : str or None, default None
        Name of the target timezone that `s` was converted into prior to calling this function.
        Used in the UserWarning if the timezone of `s` is not UTC. If None the timezone of `s`
        was not converted prior to calling this function.
    
    Returns
    -------
    pd.Serie
        `s` converted to Unix Epoch time.

    Raises
    ------
    UserWarning
        If the timezone of `s` is not in timezone UTC.

    References
    ----------
    Converting timestamps to Unix Epoch:
    - https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch
    """

    tz = s.dt.tz

    if tz is not None and str(tz) != 'UTC':
        warnings.warn(
            message=(
                f'Converting to Unix Epoch but timezone ({tz}) of column {s.name} is not set to UTC. '
                f'target_tz={target_tz!r}. The result may be incorrect!'
            ),
            category=UserWarning,
            stacklevel=2
        )

    return (s - pd.Timestamp('1970-01-01',  tz=tz)) // pd.Timedelta('1s')


def convert_datetime_columns(df: pd.DataFrame,
                             dtype: Optional[str] = None,
                             datetime_format: str = r'%Y-%m-%d %H:%M:%S',
                             localize_tz: Optional[str] = None,
                             target_tz: Optional[str] = None) -> pd.DataFrame:
    r"""Convert the datetime columns of a DataFrame to desired data type.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with datetime columns to convert.

    dtype : {'str', 'int'} or None, default None
        The data type to convert the datetime columns into.
        'int' converts the datetime columns to the number of seconds since
        the Unix Epoch of 1970-01-01 in UTC timezone. If None (the default)
        the datetime data type is kept.

    datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
        The datetime format to use when converting datetime columns to string.

    localize_tz : str or None, default None 
        Name of the timezone which to localize naive datetime columns into.
        If None localization is omitted.

    target_tz : str or None, default None
        Name of the target timezone to convert timezone aware datetime columns into.
        If `target_tz` is None or `target_tz = `localize_tz` timezone conversion is omitted.

    Returns
    -------
    df_output: pd.DataFrame
        A copy of the input DataFrame `df` with the datetime columns converted.

    Raises
    ------
    pandemy.InvalidInputError
        If an unknown `dtype` is supplied or if the timezone localization or conversion fails.

    UserWarning
        If the timezone of a datetime column is not in timezone UTC when converting to Unix Epoch.
    """

    if dtype not in {'str', 'int', None}:
        raise pandemy.InvalidInputError(f"Invalid option ({dtype}) for dtype. Valid options are: {{'str', 'int', None}}.")

    df_output = df.copy()

    for col in df_output.select_dtypes(include=['datetime', 'datetime64', np.datetime64, 'datetimetz']).columns:
        s = df_output[col]
        s = localize_and_convert_timezone(s=s, localize_tz=localize_tz, target_tz=target_tz)

        try:
            if dtype is None:
                df_output[col] = s
            elif dtype == 'str':
                df_output[col] = s.dt.strftime(datetime_format).astype('string')
            elif dtype == 'int':
                df_output[col] = to_unix_epoch(s=s, target_tz=target_tz)
        except Exception as e:
            if dtype == 'int':
                error_msg: str = (
                    f'Could not convert datetime column {col} to {dtype!r} '
                    f'with localize_tz={localize_tz!r} and target_tz={target_tz!r}'
                )
                data = (col, localize_tz, target_tz, e.args)
            elif dtype == 'str':
                error_msg: str = (
                    f'Could not convert datetime column {col} to {dtype!r} '
                    f'with format string: {datetime_format!r}'
                )
                data = (col, datetime_format, e.args)

            error_msg = f'{error_msg}\n{type(e).__name__} : {e.args[0]}'

            raise pandemy.InvalidInputError(error_msg, data=data) from None

    return df_output
