"""Internal module that contains functions to handle datetime related operations."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging
from typing import Optional

# Third Party
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
