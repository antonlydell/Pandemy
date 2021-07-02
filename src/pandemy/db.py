"""
Contains the classes that represent the Database interface.

namedtuple
----------

Placeholder: namedtuple('Placeholder', ['key', values', 'new_key'], defaults=(True,))

    key: str
        The placeholder to replace in the substring.

    values: Union[str, int, float, Iterable[Union[str, int, float]]]
        The value(s) to replace the placeholder with.

    new_key: bool, default True
        If the values should be added as new placeholders.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import logging
from pathlib import Path

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Any, Dict, Iterable, Optional, Union

# Third Party
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import ArgumentError

# Local
import pandemy

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# A Placeholder to replace and its substituion value
Placeholder = namedtuple('Placeholder', ['key', 'values', 'new_key'], defaults=(True,))

# ===============================================================
# Classes
# ===============================================================


class DbStatement(ABC):
    """
    Base class of a container of database statements. 

    Each SQL-dialect will inherit from this class.
    Each statement is implemented as a class variable.
    """

    @staticmethod
    def validate_class_variables(cls: object, parent_cls: object, type_validation: str) -> None:
        """
        Validate that a subclass has implmeneted the class variables
        specified on its parent class.

        Intended to be used in special method `__init_subclass__`.

        Parameters
        ----------

        cls: object
            The class being validated.

        parent_cls: object
            The parent class that `cls` inherits from.

        type_validation: str ('isinstance', 'type')
            How to validate the type of the class variables.
            'type' should be used if an uninstantiated class is assigned to class variables.

        Raises
        ------

        AttributeError
            If the parent class is missing annotated class variables.

        NotImplementedError
            If a class variable is not implemented.

        TypeError
            If a class variable is not of the type specified in the parent class.

        ValueError
            If a value other than 'isinstance', 'type' is given to type_validation parameter.
        """

        # Get the annotated class variables of the parent class
        class_vars = parent_cls.__annotations__

        for var, dtype in class_vars.items():

            # Check that the class variable exists
            if (value := getattr(cls, var, None)) is None:
                raise NotImplementedError(f'Class {cls.__name__} has not implemented the requried variable: {var}')

            # Check for correct data type
            if type_validation == 'isinstance':
                is_valid = isinstance(value, dtype)
            elif type_validation == 'type':
                is_valid = type(value) == type(dtype)
            else:
                raise ValueError(f"type_validation = {type_validation}. Expected 'isinstance', or 'type'")

            if not is_valid:
                raise TypeError(f'Class variable "{var}"" of class {cls.__name__} '
                                f'is of type {type(value)} ({value}). Expected {dtype}.')

    @staticmethod
    def replace_placeholders(stmt: str, placeholders: Union[Placeholder, Iterable[Placeholder]]) -> str:
        """
        Replace placeholders in an SQL statement.

        Replaces the specified keys in placeholders with supplied values
        in the statement string `stmt`. A single value (str, int or float)
        are replaced as is and not replaced with new placeholders.

        Parameters
        ----------

        stmt: str
            The SQL statement in which to replace placeholders

        placeholders: Placeholder or iterable of Placeholder
            The replacement values for each placeholder.
            Placeholder = namedtuple('Placeholder', ['key', 'values', 'new_key'], defaults=(True,))

        Returns
        -------

        stmt: str
            The SQL statement after replacements.

        params: dict
            Containing the mapping of inserted placeholders and their mapped values.
            {'new_placeholder1': 'value1', 'new_placeholder2': 'value2'}

        Raises
        ------

        TypeError
            If the replacement values in a Placeholder is not of type str, int, float, bool or None
        """

        def is_valid_replacement_value(value: Union[str, int, float, bool, None], raises: bool = False) -> bool:
            """
            Helper function to validate values of the replacements.

            Parameters
            ----------

            value: str, int or float
                The value to validate.

            raises: bool, default False
                If True TypeError will be raised if value is not valid.
                If False the function will return False.
            """

            if (isinstance(value, str) or isinstance(value, int) or isinstance(value, float) 
               or isinstance(value, bool) or value is None):
                return True
            else:
                if raises:
                    raise TypeError('values in replacements must be: str, int or float. '
                                    f'Got {value} ({type(value)})')
                else:
                    return False

        # Stores new placeholders and their mapped values
        params = dict()

        # Counter of number of new placeholders added. Makes sure each new placeholder is unique
        counter = 0

        # Convert to list if a single Placeholder object is passed
        if isinstance(placeholders, Placeholder):
            placeholders = [placeholders]

        for placeholder in placeholders:
            if is_valid_replacement_value(placeholder.values, raises=False):

                # Build replacement string of new placeholder
                if placeholder.new_key:
                    new_placeholder = f'v{counter}'
                    counter += 1
                    repl_str = f':{new_placeholder}'
                    params[new_placeholder] = placeholder.values
                else:
                    repl_str = placeholder.values

            elif hasattr(placeholder.values, '__iter__'):  # list like

                repl_str = ''
                for value in placeholder.values:

                    # Check that we have a valid replacement valuec
                    is_valid_replacement_value(value, raises=True)

                    # Build replacement string of new placeholders
                    if placeholder.new_key:
                        new_placeholder = f'v{counter}'
                        counter += 1
                        repl_str += f':{new_placeholder}, '
                        params[new_placeholder] = value
                    else:
                        repl_str += f'{value}, '

                # Remove last unwanted ', '
                repl_str = repl_str[:-2]

            else:
                raise TypeError(f'placeholder replacements must be a string, int or tuple or an iterable of those. '
                                f'Got {placeholder.values} ({type(placeholder.values)})')

            # Replace the placeholder with the replacement string
            stmt = stmt.replace(placeholder.key, repl_str)

        return stmt, params


class DatabaseManager(ABC):
    """
    Base class of database functionality.

    Each specific database type will subclass from DatabaseManager.
    """

    @abstractmethod
    def __init__(self, statement: Optional[DbStatement] = None, **kwargs) -> None:
        """
        The initializer should support **kwargs because different types of databases
        will need different input parameters.

        Parameters
        ----------

        statement: DbStatement or None, default None
            A DbSatement object containing database statements
            that can be used by the DatabaseManager.
        """

    def __str__(self) -> str:
        """String representation of the object"""

        return self.__class__.__name__

    def __repr__(self) -> str:
        """Debug representation of the object"""

        # Get the attribute names of the class instance
        attributes = self.__dict__.items()

        # The name of the class
        repr_str = f'{self.__class__.__name__}('

        # Append the attribute names and values
        for attrib, value in attributes:
            repr_str += f'\t{attrib}={value}, '

        # Remove last unwanted ', '
        repr_str = repr_str[:-2]

        repr_str += '\t)'

        return repr_str

    def execute_statement(self, sql: Union[str, text], conn, params: Union[dict, list, None] = None):
        """
        Execute an SQL statement.

        To process the result from the method the database connection must remain open.

        See also
        --------

        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection.execute

        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.CursorResult

        Parameters
        ----------

        sql: str or sqlalchemy.text
            The SQL statement string to process.

        conn: sqlalchemy database connection or None
            An open connection to the database to use.
            If None a connection to the database will be opened from the engine.

        params: dict or list of dict, default None
            Parameters to bind to the sql query using % or :name.
            All params should be dicts or sequences of dicts as of SQLAlchemy 2.0

        Returns
        -------

        sqlalchemy.engine.CursorResult
            A result object from the statement.

        Raises
        ------

        TypeError
            If `sql` is not of type str or sqlalchemy.text.

        pandemy.ExecuteStatementError
            If an error occurs when executing the statement.
        """

        if isinstance(sql, str):
            stmt = text(sql)
        elif isinstance(sql, text):
            stmt = sql
        else:
            raise TypeError(f'Invalid type {type(sql)} for sql. Expected str or sqlalchemy.text')

        try:
            if params is None:
                return conn.execute(stmt)
            else:
                return conn.execute(stmt, params)  # Parameters to bind to the sql statement
        except Exception as e:
            raise pandemy.ExecuteStatementError(f'{type(e).__name__}: {e.args}', data=e.args) from None


class SQLiteDb(DatabaseManager):
    """
    A SQLite database.

    Reference
    ---------

    - https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine

    - https://docs.sqlalchemy.org/en/14/dialects/sqlite.html
    """

    def _set_validate_parameters(self, file: Union[str, Path],  must_exist: bool, statement: Optional[DbStatement],
                                 engine_config: Optional[Dict[str, Any]]) -> None:
        """
        Validate the input parameters from `self.__init__` and set attributes on the instance.
        """

        # file
        # =================================
        if isinstance(file, Path) or file == ':memory:':
            self.file = file
        elif isinstance(file, str):
            self.file = Path(file)
        else:
            raise TypeError('file must be a string or pathlib.Path. '
                            f'Received: {file} {type(file)}')

        # must_exist
        # =================================
        if isinstance(must_exist, bool):
            self.must_exist = must_exist
        else:
            raise TypeError('must_exist must be a boolean '
                            f'Received: {must_exist} {type(must_exist)}')

        # statement
        # =================================
        if statement is None:
            self.statement = statement
        else:
            try:
                if issubclass(statement, DbStatement):  # Throws TypeError if statement is not a class
                    self.statement = statement
                    error = False
                else:
                    error = True
            except TypeError:
                error = True

            if error:
                raise TypeError('statement must be a subclass of DbStatement '
                                f'Received: {statement} {type(statement)}') from None

        # engine_config
        # =================================
        if engine_config is None or isinstance(engine_config, dict):
            self.engine_config = engine_config
        else:
            raise TypeError('engine_config must be a dict '
                            f'Received: {engine_config} {type(engine_config)}') from None

    def _build_conn_str(self) -> None:
        """
        Build the database connection string and assgin it to `self.conn_str`.

        Raises
        ------

        FileNotFoundError
            If the database file `file` does not exist.
        """

        if self.file == ':memory:':  # Use an in memory database
            self.conn_str = r'sqlite://'

        else:  # Use a database on file
            if not self.file.exists() and self.must_exist:
                raise FileNotFoundError(f'file = {self.file} does not exist and '
                                        f'and must_exist = {self.must_exist}. Cannot instantiate the SQLite database.')

            self.conn_str = fr'sqlite:///{self.file}'

    def _create_engine(self) -> None:
        """
        Create the databse engine and assign it to `self.engine`.

        Parameters
        ----------

        config: dict
            Additional key word arguments passed to the SQLAlchemy create_engine function.

        Raises
        ------

        pandemy.CreateEngineError
            If the creation of the Database engine fails.
        """

        try:
            if self.engine_config is not None:
                self.engine = create_engine(self.conn_str, **self.engine_config)
            else:
                self.engine = create_engine(self.conn_str)

        except Exception as e:
            raise pandemy.CreateEngineError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None

        else:
            logger.debug(f'Successfully created database engine from conn_str: {self.conn_str}.')

    def __init__(self, file: Union[str, Path] = ':memory:', must_exist: bool = True,
                 statement: Optional[DbStatement] = None, engine_config: Optional[Dict[str, Any]] = None,
                 **kwargs: dict) -> None:
        """
        Initialize the database instance.

        Creates the connection string and the database engine.

        Parameters
        ----------

        file: str or Path, default ':memory:'
            The file (with path) to the SQLite database.
            The default creates an in memory database.

        must_exist: bool, default True
            If True validate that file exists unless file = ':memory:'.
            If it does not exist FileNotFoundError is raised.
            If False the validation is omitted.

        statement: DbStatement or None, default None
            A DbStatement class that contains database statements that the SQLite database can use.

        engine_config: dict or None
            Additional key word arguments passed to the SQLAlchemy create_engine function.

        **kwargs: dict
            Additional key word arguments that are not used by SQLiteDb.
            Allows unpacking a config dict as the parameters to SQLiteDb.

        Raises
        ------

        TypeError
            If invalid types are supplied to `file`, `must_exist` and `statement`.

        FileNotFoundError
            If the database file `file` does not exist.

        pandemy.CreateEngineError
            If the creation of the Database engine fails.
        """

        self._set_validate_parameters(file=file, must_exist=must_exist,
                                      statement=statement, engine_config=engine_config)
        self._build_conn_str()
        self._create_engine()

    def __str__(self):
        """ String representation of the object """

        return f'SQLiteDb(file={self.file}, must_exist={self.must_exist})'
