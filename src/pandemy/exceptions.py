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


class CreateTableError(DatabaseManagerError):
    """Errors when creating tables in the database."""


class CreateIndexError(DatabaseManagerError):
    """Errors when creating indices in the database."""


class DeleteTableError(DatabaseManagerError):
    """Errors when creating indices in the database."""


class SaveTableError(DatabaseManagerError):
    """Errors when saving a DataFrame as a table to database."""


class InvalidTableNameError(DatabaseManagerError):
    """When supplying an invalid table name."""
