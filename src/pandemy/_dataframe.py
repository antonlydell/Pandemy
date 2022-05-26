r"""Internal helper module that contains functions to handle operations on and transformations of DataFrames."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging

# Third Party
import pandas as pd

# Local

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# ===============================================================
# Functions
# ===============================================================


def convert_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    r"""Convert missing values (NaN values) of type pandas.NA, pandas.NaT or numpy.nan to None.

    Some databases cannot handle pandas.NA, pandas.NaT or numpy.nan values in parametrized
    SQL statements to update and insert data. Therefore these values need to be converted to
    standard Python None to make it work. Before conversion to None can be performed the
    data type of the columns must be converted to the object data type.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame with possible missing values.

    Returns
    -------
    pd.DataFrame
        `df` with NaN values converted to None.
    """

    mask = df.isna()
    cols_with_nan = df.columns[mask.any()]

    return (
        df
        .astype({col: object for col in cols_with_nan})
        .mask(mask, None)
    )
