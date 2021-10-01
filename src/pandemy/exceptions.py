"""Module with the exceptions raised by pandemy."""

# ===============================================================
# Imports
# ===============================================================

# Standard library
from typing import Optional


# ===============================================================
# Classes
# ===============================================================

class PandemyError(Exception):
    r""""Base Exception for pandemy"""

    def __init__(self, message: str, data: Optional[str] = None) -> None:
        r"""Save optional data as an attribute on the Exception instance."""

        self.data = data
        super().__init__(message)


class InvalidInputError(PandemyError):
    r"""Invalid input to a function or method"""

# ---------------------------------------------------------------
# DatabaseManagerError
# ---------------------------------------------------------------


class DatabaseManagerError(PandemyError):
    r"""Base Exception for errors related to the DatabaseManager class."""


class CreateEngineError(DatabaseManagerError):
    r"""Error when creating the database engine."""


class DatabaseFileNotFoundError(DatabaseManagerError):
    r"""Error when the file of an SQLite database cannot be found."""


class DataTypeConversionError(DatabaseManagerError):
    r"""Errors converting data types of columns in a DataFrame."""


class DeleteFromTableError(DatabaseManagerError):
    r"""Errors when deleting data from a table in the database."""


class ExecuteStatementError(DatabaseManagerError):
    r"""Errors when executing an SQL statement with the DatabaseManager."""


class InvalidTableNameError(DatabaseManagerError):
    r"""Errors when supplying an invalid table name to a database operation."""


class LoadTableError(DatabaseManagerError):
    r"""Errors when loading tables from the database."""


class SaveDataFrameError(DatabaseManagerError):
    r"""Errors when saving a DataFrame to a table in the database."""


class SetIndexError(DatabaseManagerError):
    r"""Errors when setting an index of a DataFrame after loading a table from the database."""


class TableExistsError(DatabaseManagerError):
    r"""Errors when saving a DataFrame to a table and the table already exists."""
