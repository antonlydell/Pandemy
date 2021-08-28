"""
Module with the exceptions raised by pandemy.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard library
from typing import Optional


class PandemyError(Exception):
    """"Base Exception for pandemy"""

    def __init__(self, message: str, data: Optional[str] = None) -> None:
        """Save optional data as an attribute on the Exception instance."""

        self.data = data
        super().__init__(message)


class InvalidInputError(PandemyError):
    """Invalid input to a function or method"""


class DatabaseManagerError(PandemyError):
    """Base exceptions for errors from the DatabaseManager class."""


class ExecuteStatementError(DatabaseManagerError):
    """Errors when executing an SQL statement with the DatabaseManager."""


class CreateEngineError(DatabaseManagerError):
    """Error when creating the database engine.."""


class TableExistsError(DatabaseManagerError):
    """Error when saving a DataFrame to a table and the table already exists."""


class CreateTableError(DatabaseManagerError):
    """Errors when creating tables in the database."""


class LoadTableError(DatabaseManagerError):
    """Errors when loading tables from the database."""


class DataTypeConversionError(DatabaseManagerError):
    """Errors converting data types of columns in a DataFrame."""


class SetIndexError(DatabaseManagerError):
    """Errors when setting an index of a DataFrame."""


class CreateIndexError(DatabaseManagerError):
    """Errors when creating indices in the database."""


class DeleteFromTableError(DatabaseManagerError):
    """Errors when deleting data from a table in the database."""


class SaveDataFrameError(DatabaseManagerError):
    """Errors when saving a DataFrame to a table in the database."""


class InvalidTableNameError(DatabaseManagerError):
    """When supplying an invalid table name."""
