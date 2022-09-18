r"""Internal helper module that contains functions to handle operations on and transformations of DataFrames."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging
from typing import Any, Dict, Iterator, List, Optional

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


def df_to_parameters_in_chunks(
    df: pd.DataFrame,
    chunksize: Optional[int] = None
) -> Iterator[List[Dict[str, Any]]]:
    r"""Convert a DataFrame into an iterator yielding a list of dicts, where each dict is a row of `df`.

    The keys in each dict are the column names of `df` and the values are the values of the row for each column.
    E.g. [{column1: value12, column2: value12, ...}, {column1: value21, column2: value22, ...}, {...}]

    The list of dicts can be used as input to parametrized SQL statements where the column names
    match the parameter names of the statement.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to split into chunks.

    chunksize: int or None, default None
        The number of rows from `df` to return in each chunk.
        If None all rows are returned in one chunk which is the default.

    Returns
    -------
    Iterator[List[Dict[str, Any]]
        An iterator yielding a list of `chunksize` dicts.
    """

    end = df.shape[0]  # The number of rows in df
    if chunksize is None:
        chunksize = end

    start = 0
    while start < end:
        yield df.iloc[start:start + chunksize, :].to_dict(orient='records')
        start += chunksize
