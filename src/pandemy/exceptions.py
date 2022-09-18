r"""The exception hierarchy of Pandemy."""

# ===============================================================
# Imports
# ===============================================================

# Standard library
from typing import Any, Optional


# ===============================================================
# Classes
# ===============================================================

class PandemyError(Exception):
    r"""*The* base :exc:`Exception` of Pandemy.

    Parameters
    ----------
    message : str
        The exception message.

    data : Any, default None
        Optional extra data to to save as an attribute on the instance.
        Useful to give more details about the cause of the exception.
    """

    def __init__(self, message: str, data: Optional[Any] = None) -> None:

        self.data = data
        super().__init__(message)


class InvalidInputError(PandemyError):
    r"""Invalid input to a function or method."""

# ---------------------------------------------------------------
# DatabaseManagerError
# ---------------------------------------------------------------


class DatabaseManagerError(PandemyError):
    r"""Base :exc:`Exception` for errors related to the
    :class:`DatabaseManager <pandemy.DatabaseManager>` class.
    """


class CreateConnectionURLError(DatabaseManagerError):
    r"""Error when creating a connection URL to create the database :class:`Engine <sqlalchemy.engine.Engine>`.

    .. versionadded:: 1.1.0
    """


class CreateEngineError(DatabaseManagerError):
    r"""Error when creating the database :class:`Engine <sqlalchemy.engine.Engine>`."""


class DatabaseFileNotFoundError(DatabaseManagerError):
    r"""Error when the file of a SQLite database cannot be found."""


class DataTypeConversionError(DatabaseManagerError):
    r"""Errors when converting data types of columns in a :class:`pandas.DataFrame`."""


class DeleteFromTableError(DatabaseManagerError):
    r"""Errors when deleting data from a table in the database."""


class ExecuteStatementError(DatabaseManagerError):
    r"""Errors when executing a SQL statement with a
    :class:`DatabaseManager <pandemy.DatabaseManager>`.
    """


class InvalidColumnNameError(DatabaseManagerError):
    r"""Errors when supplying an invalid column name to a database operation.

    .. versionadded:: 1.2.0
    """


class InvalidTableNameError(DatabaseManagerError):
    r"""Errors when supplying an invalid table name to a database operation."""


class LoadTableError(DatabaseManagerError):
    r"""Errors when loading tables from the database."""


class SaveDataFrameError(DatabaseManagerError):
    r"""Errors when saving a :class:`pandas.DataFrame` to a table in the database."""


class SetIndexError(DatabaseManagerError):
    r"""Errors when setting an index of a :class:`pandas.DataFrame`
    after loading a table from the database.
    """


class SQLStatementNotSupportedError(DatabaseManagerError):
    r"""Errors when executing a method that triggers a SQL statement not supported by the database dialect.

    .. versionadded:: 1.2.0
    """


class TableExistsError(DatabaseManagerError):
    r"""Errors when saving a :class:`pandas.DataFrame` to a table and the table already exists."""
