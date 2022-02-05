r"""
Pandemy is a wrapper around `pandas`_ and `SQLAlchemy`_ to provide an easy class based interface
for working with DataFrames and databases. This package is for those who enjoy working with `pandas`_
and SQL but do not want to learn all "bells and whistles" of `SQLAlchemy`_. Use your existing SQL
knowledge and provide text based SQL statements to load DataFrames from and write DataFrames to databases.

.. _pandas: https://pandas.pydata.org/
.. _SQLAlchemy: https://www.sqlalchemy.org/
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
from datetime import date
from typing import Tuple, Union

# Local
from pandemy.exceptions import (
   PandemyError,
   InvalidInputError,
   DatabaseManagerError,
   CreateEngineError,
   DatabaseFileNotFoundError,
   DataTypeConversionError,
   DeleteFromTableError,
   ExecuteStatementError,
   InvalidTableNameError,
   LoadTableError,
   SaveDataFrameError,
   SetIndexError,
   TableExistsError
)

from pandemy.sqlcontainer import (
   Placeholder,
   SQLContainer
)

from pandemy.databasemanager import (
   DatabaseManager,
   SQLiteDb
)

# ===============================================================
# Attributes
# ===============================================================

__versiontuple__: Tuple[Union[int, str], ...] = (1, 0, 0)
r"""The version of Pandemy in a comparable form.

Adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_
(MAJOR.MINOR.PATCH). Useful for checking if Pandemy is in a certain version range.

Examples
--------
.. code-block:: python

   >>> pandemy.__versiontuple__
   (1, 0, 0)
   >>> pandemy.__versiontuple__ > (0, 0, 1) and pandemy.__versiontuple__ < (2, 0, 0)
   True
"""

__version__: str = '.'.join(str(x) for x in __versiontuple__)
r"""The Pandemy version string derived from :data:`__versiontuple__`."""

__releasedate__: date = date(2022, 1, 30)
r"""The release date of the version specified in :data:`__versiontuple__`."""

# ===============================================================
# The Public API
# ===============================================================

__all__ = [
   # exceptions
   'PandemyError',
   'InvalidInputError',
   'DatabaseManagerError',
   'CreateEngineError',
   'DatabaseFileNotFoundError',
   'DataTypeConversionError',
   'DeleteFromTableError',
   'ExecuteStatementError',
   'InvalidTableNameError',
   'LoadTableError',
   'SaveDataFrameError',
   'SetIndexError',
   'TableExistsError',

   # sqlcontainer
   'Placeholder',
   'SQLContainer',

   # databasemanager
   'DatabaseManager',
   'SQLiteDb',

   # attributes
   '__versiontuple__',
   '__version__',
   '__releasedate__'
]
