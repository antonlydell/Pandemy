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


def datetime_columns_to_timezone(df: pd.DataFrame,  localize_tz: str = 'UTC',
                                 target_tz: Optional[str] = 'CET') -> None:
    r"""Set a timezone to naive datetime columns.

    Localize naive datetime columns of DataFrame `df` to the desired timezone.
    Optionally convert the localized columns to desired target timezone.

    Modifies DataFrame `df` inplace.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with columns to convert to datetime.

    localize_tz : str, default 'UTC'
        Name of the timezone which to localize naive datetime columns into.

    target_tz : str or None, default 'CET'
        Name of the target timezone to convert datetime columns into after
        they have been localized. If `target_tz` is None or `target_tz = `localize_tz`
        no timezone conversion will be performed.

    Returns
    -------
    None

    Raises
    ------
    pandemy.InvalidInputError
        If an unknown timezone is supplied.
    """

    # The datetime columns of the DataFrame
    cols = df.select_dtypes(include=['datetime']).columns

    for col in cols:
        try:
            df.loc[:, col] = df[col].dt.tz_localize(localize_tz)

            if target_tz is not None or target_tz == localize_tz:
                df.loc[:, col] = df[col].dt.tz_convert(target_tz)
        except Exception as e:
            raise pandemy.InvalidInputError(f'{type(e).__name__}: {e.args[0]}. '
                                            f'localize_tz={localize_tz}, target_tz={target_tz}',
                                            data=(e.args[0], localize_tz, target_tz)) from None


def convert_datetime_columns(df: pd.DataFrame,
                             dtype: str = 'str',
                             datetime_format: str = r'%Y-%m-%d %H:%M:%S',
                             localize_tz: Optional[str] = None,
                             target_tz: Optional[str] = None) -> pd.DataFrame:
    r"""Convert the datetime columns of a DataFrame to desired data type.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with datetime columns to convert.

    dtype : {'str', 'int'}, default 'str'
        The data type to convert the datetime columns into.
        'int' converts the datetime columns to the number of seconds since
        the unix epoch of 1970-01-01 in UTC timezone.

    datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
        The datetime format to use when converting datetime columns to string.

    localize_tz : str or None, default None 
        Name of the timezone which to localize naive datetime columns into.
        If None no localization is performed.

    target_tz : str or None, default None
        Name of the target timezone to convert timezone aware datetime columns into.
        If `target_tz` is None or `target_tz = `localize_tz` no timezone conversion
        will be performed.

    Returns
    -------
    df_output: pd.DataFrame
        A copy of the input DataFrame `df` with the datetime columns converted.

    Raises
    ------
    pandemy.InvalidInputError
        If an unknown `dtype` is supplied or the timezone localization or conversion fails.

    References
    ----------
    Converting timestamps to unix epoch:
    - https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch
    """

    if dtype not in {'str', 'int'}:
        raise pandemy.InvalidInputError(f"Invalid option ({dtype}) for dtype. Valid options are: {{'str', 'int'}}.")

    df_output = df.copy()

    for col in df_output.select_dtypes(include=['datetime', 'datetime64', np.datetime64, 'datetimetz']).columns:

        s = df_output[col]
        tz = s.dt.tz

        # Localize and convert timezone
        try:
            if tz is None and localize_tz is not None:
                s = s.dt.tz_localize(localize_tz)
                df_output.loc[:, col] = s 
                tz = s.dt.tz

            if target_tz is not None and tz is not None and target_tz != str(tz):
                s = df_output[col].dt.tz_convert(target_tz)
                tz = s.dt.tz
                df_output.loc[:, col] = s 
        except TypeError as e:
            raise pandemy.InvalidInputError(f'Column = {col} : {e.args[0]}', data=(col, e.args)) from None

        # Convert to string or Unix Epoch
        try:
            if dtype == 'str':
                df_output[col] = s.dt.strftime(datetime_format).astype('string')
            elif dtype == 'int':  # Convert to Unix Epoch (UTC)
                if tz is not None and str(tz) != 'UTC':
                    warnings.warn(
                        message=(
                            f'Converting to Unix Epoch but timezone ({tz}) of column {col} is not set to UTC. '
                            f'target_tz={target_tz}. The result may be incorrect!'
                        ),
                        category=UserWarning,
                        stacklevel=2
                    )
                df_output[col] = (s - pd.Timestamp('1970-01-01',  tz=tz)) // pd.Timedelta('1s')
        except Exception as e:
            if dtype == 'int':
                error_msg: str = (
                    f'Could not convert datetime column {col} to {dtype} '
                    f'with localize_tz={localize_tz} and target_tz={target_tz}\n'
                )
                data = (col, localize_tz, target_tz, e.args)
            elif dtype == 'str':
                error_msg: str = (
                    f'Could not convert datetime column {col} to {dtype} '
                    f'to {dtype} with format string: {datetime_format}\n'
                )
                data = (col, datetime_format, e.args)

            raise pandemy.InvalidInputError(error_msg, data=data) from None

    return df_output
