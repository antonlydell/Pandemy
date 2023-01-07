r"""Contains the classes that represent the database interface.

The Database is managed by the :class:`DatabaseManager <pandemy.DatabaseManager>` class.
Each SQL-dialect is implemented as a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>`.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
from __future__ import annotations
import itertools
import logging
from pathlib import Path
import re
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union
import urllib
import warnings

# Third Party
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine import CursorResult, Engine, make_url, URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import TextClause

# Local
import pandemy
import pandemy._dataframe
import pandemy._datetime

# ===============================================================
# Set Logger
# ===============================================================

# Initiate the module logger
# Handlers and formatters will be inherited from the root logger
logger = logging.getLogger(__name__)

# ===============================================================
# DatabaseManager
# ===============================================================


class DatabaseManager():
    r"""Base class with functionality for managing a database.

    Each database type will subclass from :class:`DatabaseManager` and
    implement the initializer which is specific to each database type.
    Initialization of a :class:`DatabaseManager` creates the database `engine`,
    which is used to connect to and interact with the database.

    The :class:`DatabaseManager` can be used on its own but with **limited** functionality
    and the initialization requires a SQLAlchemy :class:`URL <sqlalchemy.engine.URL>` or
    :class:`Engine <sqlalchemy.engine.Engine>`, which require some knowledge about `SQLAlchemy`_.

    .. _SQLAlchemy: https://docs.sqlalchemy.org/en/14/core/engines.html#engine-configuration

    .. note::
       Some methods like :meth:`~DatabaseManager.upsert_table` and :meth:`~DatabaseManager.merge_df`
       use dialect specific SQL syntax. These methods may not work properly if using the
       :class:`DatabaseManager` directly. :class:`DatabaseManager` should *only* be used
       if there is no subclass implemented that matches the desired SQL dialect.

    Parameters
    ----------
    url : :class:`str` or :class:`sqlalchemy.engine.URL` or None, default None
        A SQLAlchemy connection URL to use for creating the database engine.
        If ``None`` an :class:`engine <sqlalchemy.engine.Engine>` is expected
        to be supplied to the `engine` parameter.

    container : SQLContainer or None, default None
        A container of database statements that :class:`DatabaseManager` can use.

    connect_args : dict or None, default None
        Additional arguments sent to the driver upon connection that further
        customizes the connection.

    engine_config : dict or None, default None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    engine : :class:`sqlalchemy.engine.Engine` or None, default None
        A SQLAlchemy Engine to use as the database engine of :class:`DatabaseManager`.
        If ``None`` (the default) the engine will be created from `url`.

    **kwargs : dict
        Additional keyword arguments that are not used by :class:`DatabaseManager`.

    Raises
    ------
    pandemy.CreateConnectionURLError
        If there are errors with `url`.

    pandemy.CreateEngineError
        If the creation of the database engine fails.

    pandemy.InvalidInputError
        If `url` and `engine` are specified or are ``None`` at the same time.

    See Also
    --------
    * :class:`OracleDb` : An Oracle :class:`DatabaseManager`.

    * :class:`SQLiteDb` : A SQLite :class:`DatabaseManager`.

    Examples
    --------
    Create an instance of a :class:`DatabaseManager` that connects to a SQLite in-memory database.

    >>> import pandemy
    >>> db = pandemy.DatabaseManager(url='sqlite://')
    >>> db
    DatabaseManager(
        url=sqlite://,
        container=None,
        connect_args={},
        engine_config={},
        engine=Engine(sqlite://)
    )
    """

    # Class variables
    # ---------------

    # The template statements can be modified by subclasses of DatabaseManager if necessary.
    _stmt_space = '    '  # Spacing to use in template statements.

    # Template statement for deleting all records in a table.
    _delete_from_table_stmt: str = """DELETE FROM :table"""

    # Template statement to update a table.
    _update_table_stmt: str = (
        """UPDATE :table
SET
    :update_cols
WHERE
    :where_cols"""
    )

    # Template statement to insert new rows, that do not exist already, into a table.
    _insert_into_where_not_exists_stmt: str = (
        """INSERT INTO :table (
    :insert_cols
)
    SELECT
        :select_values
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM :table
            WHERE
                :where_cols
        )"""
    )

    # Template of the MERGE statement. If empty string the database does not support the statement.
    _merge_df_stmt: str = ''

    __slots__ = ('url', 'container', 'connect_args', 'engine_config', 'engine')

    def __init__(
        self,
        url: Optional[Union[str, URL]] = None,
        container: Optional[pandemy.SQLContainer] = None,
        connect_args: Optional[Dict[str, Any]] = None,
        engine_config: Optional[dict] = None,
        engine: Optional[Engine] = None,
        **kwargs
    ) -> None:
        self.container = container
        self.connect_args = connect_args if connect_args is not None else {}
        self.engine_config = engine_config if engine_config is not None else {}

        if url and engine:
            raise pandemy.InvalidInputError(
                'url and engine cannot be specified at the same time.\n'
                f'url={url!r}, engine={engine!r}',
                data=(url, engine)
            )

        if url is None and engine is None:
            raise pandemy.InvalidInputError('Specify either url or engine. Both cannot be None at the same time.')

        if url is not None:
            try:
                url = make_url(url)  # Create a SQLAlchemy database URL
                # Encode username and password to make special characters url compatible
                username = url.username if (tmp := url.username) is None else urllib.parse.quote_plus(tmp)
                password = url.password if (tmp := url.password) is None else urllib.parse.quote_plus(tmp)
                self.url = url.set(username=username, password=password)
            except UnicodeEncodeError as e:
                raise pandemy.CreateConnectionURLError(
                    f'Could not URL encode username or password: {e.args}', data=e.args
                ) from None
            except Exception as e:
                raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None
        else:
            self.url = url

        # Create the engine
        if engine is None:
            try:
                self.engine = create_engine(self.url, connect_args=self.connect_args, **self.engine_config)
            except Exception as e:
                raise pandemy.CreateEngineError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None
            else:
                logger.debug(f'Successfully created database engine from url: {self.url}.')
        else:
            self.engine = engine
            self.url = engine.url

    def __str__(self) -> str:
        r"""String representation of the object."""

        return f'{self.__class__.__name__}({repr(self.url)})'

    def __repr__(self) -> str:
        r"""Debug representation of the object."""

        # Get the slot names of the parent classes
        slots = itertools.chain.from_iterable(
            getattr(cls, '__slots__', tuple()) for cls in type(self).__mro__
        )

        attributes = {attrib: self.__getattribute__(attrib) for attrib in slots}

        # The space to add before each new parameter on a new line
        space = ' ' * 4

        # The name of the class
        repr_str = f'{self.__class__.__name__}(\n'

        # Append the attribute names and values
        for attrib, value in attributes.items():
            if attrib == 'password':  # Mask the password
                value = '***'

            repr_str += f'{space}{attrib}={value!r},\n'

        # Remove last unwanted ', '
        repr_str = repr_str[:-2]

        # Add closing parentheses
        repr_str += '\n)'

        return repr_str

    def _is_valid_table_name(self, table: str) -> None:
        r"""Check if the table name is valid.

        Parameters
        ----------
        table : str
            The table name to validate.

        Raises
        ------
        pandemy.InvalidInputError
            If the table name is not a string.

        pandemy.InvalidTableNameError
            If the table name is not valid.
        """

        if not isinstance(table, str):
            raise pandemy.InvalidInputError(f'table must be a string. Got {type(table)} ({table})',
                                            data=table)

        # Get the first word of the string to prevent entering a SQL query.
        table_splitted = table.split(' ')

        # Check that only one word was input as the table name
        if (len_table_splitted := len(table_splitted)) > 1:
            raise pandemy.InvalidTableNameError(f'Table name contains spaces ({len_table_splitted - 1})! '
                                                f'The table name must be a single word.\ntable = {table}',
                                                data=table)

    @staticmethod
    def _validate_chunksize(chunksize: Optional[int]) -> None:
        r"""Validate that the `chunksize` parameter is an integer > 0 or None.

        The `chunksize` parameter is used by the methods:

        - load_table
        - merge_df
        - save_df
        - upsert_table

        Parameters
        ----------
        chunksize : int or None
            The number of rows from a DataFrame to process in each chunk.

        Raises
        ------
        pandemy.InvalidInputError
            If `chunksize` is not an integer > 0 or None.
        """

        if chunksize is None:
            return

        if not isinstance(chunksize, int):
            raise pandemy.InvalidInputError(
                f'chunksize must be of type int, got {type(chunksize)}', data=chunksize
            )

        if chunksize <= 0:
            raise pandemy.InvalidInputError(
                f'chunksize ({chunksize}) be an integer > 0', data=chunksize
            )

    def _supports_merge_statement(self) -> None:
        r""""Check if the :class:`DatabaseManger` supports the MERGE statement.

        If is does not support the MERGE statement :exc:`pandemy.SQLStatementNotSupportedError` is raised.

        Raises
        ------
        pandemy.SQLStatementNotSupportedError
            If the MERGE statement is not supported by the database SQL dialect.
        """

        if not self._merge_df_stmt:
            raise pandemy.SQLStatementNotSupportedError(
                f'{self.__class__.__name__} does not support the MERGE statement.'
                f'Try the similar upsert_table method instead.'
            )

    def _prepare_input_data_for_modify_statements(
            self,
            df: pd.DataFrame,
            update_cols: Optional[Union[str, Sequence[str]]],
            update_index_cols: Union[bool, Sequence[str]],
            where_cols: Sequence[str]) -> Tuple[pd.DataFrame, List[str], List[str]]:
        r"""Prepare input data to be ready to use in an UPSERT or MERGE statement.

        Parameters
        ----------
        df : pandas.DataFrame
            The input DataFrame from which to select the columns to use in the statements.

        update_cols : str or Sequence[str] or None
            The columns to update and or insert data into.
            The string 'all' includes all columns of `df`.

        update_index_cols : bool or Sequence[str]
            If the index columns of `df` should be included in the columns to update.
            ``True`` indicates that the index should be included. If the index is a :class:`pandas.MultiIndex`
            a sequence of str that maps against the levels to include can be used to only include the desired levels.
            ``False`` excludes the index column(s) from being updated.

        where_cols : Sequence[str]
            The columns to include in the WHERE clause.

        Returns
        -------
        df_output : pandas.DataFrame
            `df` with columns not affected by the columns used in the statement removed.

        update_cols : list of str
            The columns to update.

        insert_cols : list of str
            The columns to insert data into.

        Raises
        ------
        pandemy.InvalidColumnNameError
            If a column name of `update_cols`, `update_index_cols` or `where_cols` are not
            among the columns of the input DataFrame `df`.
        """

        # Get the columns to update and convert to list
        if update_cols == 'all':
            update_cols = df.columns.tolist()
        elif update_cols is None:
            update_cols = []
        else:
            update_cols = list(update_cols)

        # Add selected index columns to the columns to update
        if update_index_cols:
            update_cols.extend(
                list(df.index.names) if update_index_cols is True else list(update_index_cols)
            )

        insert_cols = update_cols
        update_cols = [col for col in update_cols if col not in where_cols]  # where_cols should not be updated

        cols_in_stmts = list(set(update_cols + insert_cols + where_cols))  # The columns used in the statements

        df_output = df.reset_index()

        # Check for column names that are not part of the DataFrame
        if len((invalid_cols := [col for col in cols_in_stmts if col not in df_output.columns])) > 0:
            raise pandemy.InvalidColumnNameError(
                f'Invalid column names: {invalid_cols}.\n'
                f'Columns and index of DataFrame: {df_output.columns.tolist()}',
                data=invalid_cols) from None

        df_output = df_output[cols_in_stmts]  # Keep only the columns affected by the statements

        return df_output, update_cols, insert_cols

    def _create_update_statement(self,
                                 table: str,
                                 update_cols: Sequence[str],
                                 where_cols: Sequence[str],
                                 space_factor: int = 1) -> str:
        r"""Create an UPDATE statement with placeholders parametrized.

        Creates the statement from the template `self._update_table_stmt`.

        Parameters
        ----------
        table : str
            The name of the table to update.

        update_cols : Sequence[str]
            The columns to update.

        where_cols : Sequence[str]
            The columns to include in the WHERE clause.

        space_factor : int, default 1
            The factor to multiply the `self._stmt_space` with to determine
            the level of indentation of the columns in the SET and WHERE clause.
        """

        update_stmt = self._update_table_stmt.replace(':table', table)

        update_stmt = update_stmt.replace(
            ':update_cols',
            f',\n{self._stmt_space*space_factor}'.join(f'{col} = :{col}' for col in update_cols)
        )

        update_stmt = update_stmt.replace(
            ':where_cols',
            f' AND\n{self._stmt_space*space_factor}'.join(f'{col} = :{col}' for col in where_cols)
        )

        return update_stmt

    def _create_insert_into_where_not_exists_statement(self,
                                                       table: str,
                                                       insert_cols: Sequence[str],
                                                       where_cols: Sequence[str],
                                                       select_values_space_factor: int = 2,
                                                       where_cols_space_factor: int = 4) -> str:
        r"""Create an "INSERT INTO WHERE NOT EXISTS" statement with placeholders parametrized.

        Creates the statement from the template `self._insert_into_where_not_exists_stmt`.

        Parameters
        ----------
        table : str
            The name of the table to insert values into.

        insert_cols : Sequence[str]
            The columns to insert values into.

        where_cols : Sequence[str]
            The columns to include in the WHERE clause.

        select_values_space_factor : int, default 2
            The factor to multiply the `self._stmt_space` with to determine
            the level of indentation of the columns in the SELECT clause.

        where_cols_space_factor : int, default 4
            The factor to multiply the `self._stmt_space` with to determine
            the level of indentation of the columns in the WHERE clause.
        """

        insert_stmt = self._insert_into_where_not_exists_stmt.replace(':table', table)
        stmt_space = self._stmt_space

        insert_stmt = insert_stmt.replace(
            ':insert_cols',
            f',\n{stmt_space}'.join(insert_cols)
        )

        insert_stmt = insert_stmt.replace(
            ':select_values',
            f',\n{stmt_space*select_values_space_factor}'.join(f':{col}' for col in insert_cols)
        )

        insert_stmt = insert_stmt.replace(
            ':where_cols',
            f' AND\n{stmt_space*where_cols_space_factor}'.join(f'{col} = :{col}' for col in where_cols)
        )

        return insert_stmt

    def _create_merge_statement(self,
                                table: str,
                                insert_cols: Sequence[str],
                                on_cols: Sequence[str],
                                update_cols: Sequence[str],
                                target_prefix: str = 't',
                                source_prefix: str = 's',
                                omit_update_where_clause: bool = True,
                                when_not_matched_by_source_delete: bool = False) -> str:
        r"""Create a MERGE statement with placeholders parametrized.

        Creates the statement from the template `self._merge_df_stmt`.

        Parameters
        ----------
        table : str
            The name of the table to merge values into.

        insert_cols : Sequence[str]
            The columns to insert values into.

        on_cols : Sequence[str]
            The columns to include in the ON clause.

        update_cols : Sequence[str]
            The columns to update.

        target_prefix : str, default 't'
            The prefix to use for columns of the target table in the database.

        source_prefix : str, default 's'
            The prefix to use for columns of the source table (the DataFrame)
            to merge into the target table.

        omit_update_where_clause : bool, default True
            If the WHERE clause of the UPDATE clause should be omitted.

        when_not_matched_by_source_delete : bool, default False
            If the THE WHEN NOT MATCHED BY SOURCE DELETE clause should be included.
            This clause is only supported by Microsoft SQL Server.
        """

        t = target_prefix
        s = source_prefix
        stmt_space = self._stmt_space

        # Table name
        merge_stmt = self._merge_df_stmt.replace(':table', table)

        # Prefixes
        merge_stmt = merge_stmt.replace(':target', t)
        merge_stmt = merge_stmt.replace(':source', s)

        # USING SELECT values
        merge_stmt = merge_stmt.replace(
            ':select_values',
            f',\n{stmt_space*2}'.join(f':{col} AS {col}' for col in insert_cols)
        )

        # ON Clause
        merge_stmt = merge_stmt.replace(
            ':on',
            f' AND\n{stmt_space}'.join(f'{t}.{col} = {s}.{col}' for col in on_cols)
        )

        # WHEN MATCHED THEN UPDATE
        merge_stmt = merge_stmt.replace(
            ':update_cols',
            f',\n{stmt_space*2}'.join(f't.{col} = s.{col}' for col in update_cols)
        )

        # WHEN MATCHED THEN UPDATE WHERE
        if omit_update_where_clause:
            merge_stmt = merge_stmt.replace(f'{stmt_space}WHERE\n{stmt_space*2}:update_where_cols\n', '')
        else:
            merge_stmt = merge_stmt.replace(
                ':update_where_cols',
                f' OR\n{stmt_space*2}'.join(f't.{col} <> s.{col}' for col in update_cols)
            )

        # WHEN NOT MATCHED THEN INSERT INTO
        merge_stmt = merge_stmt.replace(
            ':insert_cols',
            f',\n{stmt_space*2}'.join(f'{t}.{col}' for col in insert_cols)
        )

        # WHEN NOT MATCHED THEN INSERT VALUES
        merge_stmt = merge_stmt.replace(
            ':insert_values',
            f',\n{stmt_space*2}'.join(f'{s}.{col}' for col in insert_cols)
        )

        # Optionally use WHEN NOT MATCHED BY SOURCE DELETE (Only Microsoft SQL Server)

        return merge_stmt

    def manage_foreign_keys(self, conn: Connection, action: str) -> None:
        r"""Manage how the database handles foreign key constraints.

        Should be implemented by DatabaseManagers whose SQL dialect
        supports enabling/disabling checking foreign key constraints.
        E.g. SQLite.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        action : str
            How to handle foreign key constraints in the database.

        Raises
        ------
        pandemy.InvalidInputError
            If invalid input is supplied to `action`.

        pandemy.ExecuteStatementError
            If changing the handling of foreign key constraint fails.

        See Also
        --------
        * :meth:`~DatabaseManager.execute` : Execute a SQL statement.

        Examples
        --------
        Enable and trigger a foreign key constraint using a SQLite in-memory database.

        >>> import pandemy
        >>> db = pandemy.SQLiteDb()  # Create an in-memory database
        >>> with db.engine.begin() as conn:
        ...     db.execute(
        ...         sql="CREATE TABLE Owner(OwnerId INTEGER PRIMARY KEY, OwnerName TEXT)",
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql=(
        ...             "CREATE TABLE Store("
        ...             "StoreId INTEGER PRIMARY KEY, "
        ...             "StoreName TEXT, "
        ...             "OwnerId INTEGER REFERENCES Owner(OwnerId)"
        ...             ")"
        ...         ),
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql="INSERT INTO Owner(OwnerId, OwnerName) VALUES(1, 'Shop keeper')",
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql=(
        ...             "INSERT INTO Store(StoreId, StoreName, OwnerId) "
        ...             "VALUES(1, 'Lumbridge General Supplies', 2)"
        ...         ),
        ...         conn=conn  # OwnerId = 2 is not a valid FOREIGN KEY reference
        ...     )
        ...     db.manage_foreign_keys(conn=conn, action='ON')
        ...     db.execute(
        ...         sql=(
        ...             "INSERT INTO Store(StoreId, StoreName, OwnerId) "
        ...             "VALUES(1, 'Falador General Store', 3)"
        ...         ),
        ...         conn=conn  # OwnerId = 3 is not a valid FOREIGN KEY reference
        ...     )  # doctest: +IGNORE_EXCEPTION_DETAIL, +ELLIPSIS
        Traceback (most recent call last):
        ...
        pandemy.exceptions.ExecuteStatementError: IntegrityError: ('(sqlite3.IntegrityError) FOREIGN KEY constraint failed',)
        """

    def execute(self, sql: Union[str, TextClause], conn: Connection, params: Union[dict, List[dict], None] = None):
        r"""Execute a SQL statement.

        Parameters
        ----------
        sql : str or sqlalchemy.sql.elements.TextClause
            The SQL statement to execute. A string value is automatically converted to a
            :class:`sqlalchemy.sql.elements.TextClause` with the
            :func:`sqlalchemy.sql.expression.text` function.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        params : dict or list of dict or None, default None
            Parameters to bind to the SQL statement `sql`.
            Parameters in the SQL statement should be prefixed by a colon (*:*) e.g. ``:myparameter``.
            Parameters in `params` should *not* contain the colon (``{'myparameter': 'myvalue'}``).

            Supply a list of parameter dictionaries to execute multiple
            parametrized statements in the same method call, e.g.
            ``[{'parameter1': 'a string'}, {'parameter2': 100}]``.
            This is useful for INSERT, UPDATE and DELETE statements.

        Returns
        -------
        sqlalchemy.engine.CursorResult
            A result object from the executed statement.

        Raises
        ------
        pandemy.InvalidInputError
            If `sql` is not of type str or sqlalchemy.sql.elements.TextClause.

        pandemy.ExecuteStatementError
            If an error occurs when executing the statement.

        See Also
        --------
        * :meth:`sqlalchemy.engine.Connection.execute` : The method used for executing the SQL statement.

        * :class:`sqlalchemy.engine.CursorResult` : The return type from the method.

        Examples
        --------
        To process the result from the method the database connection must remain open after the method
        is executed i.e. the context manager *cannot* be closed before processing the result:

        .. code-block:: python

           import pandemy

           db = SQLiteDb(file='Runescape.db')

           with db.engine.connect() as conn:
               result = db.execute('SELECT * FROM StoreSupply', conn=conn)

                for row in result:
                    print(row)  # process the result
                    ...
        """

        if isinstance(sql, str):
            stmt = text(sql)
        elif isinstance(sql, TextClause):
            stmt = sql
        else:
            raise pandemy.InvalidInputError(f'Invalid type {type(sql)} for sql. '
                                            'Expected str or sqlalchemy.sql.elements.TextClause.')

        try:
            if params is None:
                return conn.execute(stmt)
            else:
                return conn.execute(stmt, params)  # Parameters to bind to the sql statement
        except Exception as e:
            raise pandemy.ExecuteStatementError(f'{type(e).__name__}: {e.args}', data=e.args) from None

    def delete_all_records_from_table(self, table: str, conn: Connection) -> None:
        r"""Delete all records from the specified table.

        Parameters
        ----------
        table : str
            The table to delete all records from.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        Raises
        ------
        pandemy.InvalidTableNameError
            If the supplied table name is invalid.

        pandemy.DeleteFromTableError
            If data cannot be deleted from the table.

        See Also
        --------
        * :meth:`~DatabaseManager.load_table` : Load a SQL table into a :class:`pandas.DataFrame`.

        * :meth:`~DatabaseManager.save_df` : Save a :class:`pandas.DataFrame` to a table in the database.

        Examples
        --------
        Delete all records from a table in a SQLite in-memory database.

        >>> import pandas as pd
        >>> import pandemy
        >>> df = pd.DataFrame(data=[
        ...         [1, 'Lumbridge General Supplies', 'Lumbridge', 1],
        ...         [2, 'Varrock General Store', 'Varrock', 2],
        ...         [3, 'Falador General Store', 'Falador', 3]
        ...     ],
        ...     columns=['StoreId', 'StoreName', 'Location', 'OwnerId']
        ... )
        >>> df = df.set_index('StoreId')
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                                  StoreName   Location  OwnerId
        StoreId
        1        Lumbridge General Supplies  Lumbridge        1
        2             Varrock General Store    Varrock        2
        3             Falador General Store    Falador        3
        >>> db = pandemy.SQLiteDb()  # Create an in-memory database
        >>> with db.engine.begin() as conn:
        ...     db.save_df(df=df, table='Store', conn=conn)
        ...     df_loaded = db.load_table(sql='Store', conn=conn, index_col='StoreId')
        >>> df_loaded  # doctest: +NORMALIZE_WHITESPACE
                                  StoreName   Location  OwnerId
        StoreId
        1        Lumbridge General Supplies  Lumbridge        1
        2             Varrock General Store    Varrock        2
        3             Falador General Store    Falador        3
        >>> with db.engine.begin() as conn:
        ...     db.delete_all_records_from_table(table='Store', conn=conn)
        ...     df_loaded = db.load_table(sql='Store', conn=conn, index_col='StoreId')
        >>> assert df_loaded.empty
        >>> df_loaded  # doctest: +NORMALIZE_WHITESPACE
        Empty DataFrame
        Columns: [StoreName, Location, OwnerId]
        Index: []
        """

        self._is_valid_table_name(table=table)

        # SQL statement to delete all existing data from the table
        sql = self._delete_from_table_stmt.replace(':table', table)

        try:
            conn.execute(sql)

        except SQLAlchemyError as e:
            raise pandemy.DeleteFromTableError(f'Could not delete records from table: {table}: {e.args[0]}',
                                               data=e.args) from None

        else:
            logger.debug(f'Successfully deleted existing data from table {table}.')

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        conn: Connection,
        if_exists: str = 'append',
        index: bool = True,
        index_label: Optional[Union[str, Sequence[str]]] = None,
        chunksize: Optional[int] = None,
        schema: Optional[str] = None,
        dtype: Optional[Union[Dict[str, Union[str, object]], object]] = None,
        datetime_cols_dtype: Optional[str] = None,
        datetime_format: str = r'%Y-%m-%d %H:%M:%S',
        localize_tz: Optional[str] = None,
        target_tz: Optional[str] = None,
        method: Optional[Union[str, Callable]] = None) -> None:
        r"""Save the :class:`pandas.DataFrame` `df` to specified table in the database.

        If the table does not exist it will be created. If the table already exists
        the column names of the :class:`pandas.DataFrame` `df` must match the table column definitions.
        Uses :meth:`pandas.DataFrame.to_sql` method to write the :class:`pandas.DataFrame` to the database.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame to save to the database.

        table : str
            The name of the table where to save the :class:`pandas.DataFrame`.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        if_exists : str, {'append', 'replace', 'drop-replace', 'fail'}
            How to update an existing table in the database:

            * 'append': Append `df` to the existing table.

            * 'replace': Delete all records from the table and then write `df` to the table.

            * 'drop-replace': Drop the table, recreate it, and then write `df` to the table.

            * 'fail': Raise :exc:`pandemy.TableExistsError` if the table exists.

            .. versionadded:: 1.2.0
               'drop-replace'

        index : bool, default True
            Write :class:`pandas.DataFrame` index as a column. Uses the name of the index as the
            column name for the table.

        index_label : str or sequence of str or None, default None
            Column label for index column(s). If None is given (default) and `index` is True,
            then the index names are used. A sequence should be given if the :class:`pandas.DataFrame`
            uses a :class:`pandas.MultiIndex`.

        chunksize : int or None, default None
            The number of rows in each batch to be written at a time.
            If None, all rows will be written at once.

        schema : str, None, default None
            Specify the schema (if database flavor supports this). If None, use default schema.

        dtype : dict or scalar, default None
            Specifying the data type for columns. If a dictionary is used, the keys should be the column names
            and the values should be the SQLAlchemy types or strings for the sqlite3 legacy mode.
            If a scalar is provided, it will be applied to all columns.

        datetime_cols_dtype : {'str', 'int'} or None, default None
            If the datetime columns of `df` should be converted to string or integer data types
            before saving the table. If ``None`` no conversion of datetime columns is performed,
            which is the default. When using ``'int'`` the datetime columns are converted to the
            number of seconds since the Unix Epoch of 1970-01-01. The timezone of the datetime 
            columns should be in UTC when using ``'int'``.

            .. versionadded:: 1.2.0

        datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
            The datetime (:meth:`strftime <datetime.datetime.strftime>`) format
            to use when converting datetime columns to strings.

            .. versionadded:: 1.2.0

        localize_tz : str or None, default None 
            The name of the timezone which to localize naive datetime columns into.
            If None (the default) timezone localization is omitted.

            .. versionadded:: 1.2.0

        target_tz : str or None, default None
            The name of the target timezone to convert timezone aware datetime columns, or columns that
            have been localized by `localize_tz`, into. If None (the default) timezone conversion is omitted.

            .. versionadded:: 1.2.0

        method : None, 'multi', callable, default None
            Controls the SQL insertion clause used:

            * None:
                Uses standard SQL INSERT clause (one per row).

            * 'multi':
                Pass multiple values in a single INSERT clause. It uses a special SQL syntax not supported by
                all backends. This usually provides better performance for analytic databases like Presto and
                Redshift, but has worse performance for traditional SQL backend if the table contains many
                columns. For more information check the SQLAlchemy documentation.

            * callable with signature (pd_table, conn, keys, data_iter):
                This can be used to implement a more performant insertion method based on specific
                backend dialect features.
                See: `pandas SQL insertion method`_.

        Raises
        ------
        pandemy.TableExistsError
            If the table exists and ``if_exists='fail'``.

        pandemy.DeleteFromTableError
            If data in the table cannot be deleted when ``if_exists='replace'``.

        pandemy.InvalidInputError
            Invalid values or types for input parameters.

        pandemy.InvalidTableNameError
            If the supplied table name is invalid.

        pandemy.SaveDataFrameError
            If the :class:`pandas.DataFrame` cannot be saved to the table.

        See Also
        --------
        * :meth:`pandas.DataFrame.to_sql` : Write records stored in a DataFrame to a SQL database.

        * `pandas SQL insertion method`_ : Details about using the `method` parameter.

        .. _pandas SQL insertion method: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method

        Examples
        --------
        Save a :class:`pandas.DataFrame` to a SQLite in-memory database.

        >>> import pandas as pd
        >>> import pandemy
        >>> df = pd.DataFrame(data=[
        ...         [1, 'Lumbridge General Supplies', 'Lumbridge', 1],
        ...         [2, 'Varrock General Store', 'Varrock', 2],
        ...         [3, 'Falador General Store', 'Falador', 3]
        ...     ],
        ...     columns=['StoreId', 'StoreName', 'Location', 'OwnerId']
        ... )
        >>> df = df.set_index('StoreId')
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                                  StoreName   Location  OwnerId
        StoreId
        1        Lumbridge General Supplies  Lumbridge        1
        2             Varrock General Store    Varrock        2
        3             Falador General Store    Falador        3
        >>> db = pandemy.SQLiteDb()  # Create an in-memory database
        >>> with db.engine.begin() as conn:
        ...     db.save_df(df=df, table='Store', conn=conn)
        """

        # ==========================================
        # Validate input
        # ==========================================
        self._validate_chunksize(chunksize=chunksize)

        if not isinstance(df, pd.DataFrame):
            raise pandemy.InvalidInputError(f'df must be of type pandas.DataFrame. Got {type(df)}. df = {df}.')

        # Validate if_exists
        if not isinstance(if_exists, str) or if_exists not in {'append', 'replace', 'drop-replace', 'fail'}:
            raise pandemy.InvalidInputError(f'Invalid input if_exists = {if_exists}. '
                                            "Expected 'append', 'replace', 'drop-replace' or 'fail'.")

        # Validate connection
        if not isinstance(conn, Connection):
            raise pandemy.InvalidInputError(f'conn must be of type {type(Connection)}. Got {type(conn)}. conn = {conn}')

        # Validate table
        if not isinstance(table, str):
            raise pandemy.InvalidInputError(f'table must be a string. Got {type(table)}. table = {table}')

        # ==========================================
        # Process existing table
        # ==========================================

        if if_exists == 'replace':
            # self._is_valid_table_name(table=table) is called within delete_all_records_from_table
            self.delete_all_records_from_table(table=table, conn=conn)
            if_exists = 'append'
        elif if_exists == 'drop-replace':
            self._is_valid_table_name(table=table)
            if_exists = 'replace'  # replace is the name for drop-replace in pandas.DataFrame.to_sql
        else:
            self._is_valid_table_name(table=table)

        # ==========================================
        # Convert datetime columns
        # ==========================================

        if datetime_cols_dtype is None and localize_tz is None and target_tz is None:
            df_save = df
        else:
            df_save = pandemy._datetime.convert_datetime_columns(
                df=df,
                dtype=datetime_cols_dtype,
                datetime_format=datetime_format,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # ==========================================
        # Write the DataFrame to the SQL table
        # ==========================================

        try:
            df_save.to_sql(
                table,
                con=conn,
                if_exists=if_exists,
                index=index,
                index_label=index_label,
                chunksize=chunksize,
                schema=schema,
                dtype=dtype,
                method=method
            )

        except ValueError as e:
            if re.search(fr'.*{table}.+ already exists', e.args[0]):
                raise pandemy.TableExistsError(
                    f'Table {table} already exists! and if_exists = {if_exists!r}', data=table
                ) from None
            else:
                raise pandemy.SaveDataFrameError(
                    f'Could not save DataFrame to table {table}: {e.args}', data=e.args
                ) from None

        # Unexpected error
        except Exception as e:
            raise pandemy.SaveDataFrameError(
                f'Could not save DataFrame to table {table}: {e.args}', data=e.args
            ) from None

        else:
            nr_cols = df.shape[1] + len(df.index.names) if index else df.shape[1]
            nr_rows = df.shape[0]
            logger.info(f'Successfully wrote {nr_rows} rows over {nr_cols} columns to table {table}.')

    def load_table(self, sql: Union[str, TextClause], conn: Connection,
                   params: Optional[Dict[str, str]] = None,
                   index_col: Optional[Union[str, Sequence[str]]] = None,
                   columns: Optional[Sequence[str]] = None,
                   parse_dates: Optional[Union[list, dict]] = None,
                   localize_tz: Optional[str] = None, target_tz: Optional[str] = None,
                   dtypes: Optional[Dict[str, Union[str, object]]] = None,
                   chunksize: Optional[int] = None,
                   coerce_float: bool = True) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        r"""Load a SQL table into a :class:`pandas.DataFrame`.

        Specify a table name or a SQL query to load the :class:`pandas.DataFrame` from.
        Uses :func:`pandas.read_sql` function to read from the database.

        Parameters
        ----------
        sql : str or sqlalchemy.sql.elements.TextClause
            The table name or SQL query.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database to use for the query.

        params : dict of str or None, default None
            Parameters to bind to the SQL query `sql`.
            Parameters in the SQL query should be prefixed by a colon (*:*) e.g. ``:myparameter``.
            Parameters in `params` should *not* contain the colon (``{'myparameter': 'myvalue'}``).

        index_col : str or sequence of str or None, default None
            The column(s) to set as the index of the :class:`pandas.DataFrame`.

        columns : list of str or None, default None
            List of column names to select from the SQL table (only used when `sql` is a table name).

        parse_dates : list or dict or None, default None
            * List of column names to parse as dates.

            * Dict of `{column_name: format string}` where format string is strftime compatible
              in case of parsing string times, or is one of (D, s, ns, ms, us)
              in case of parsing integer timestamps.

            * Dict of `{column_name: arg dict}`, where the arg dict corresponds to the keyword arguments of
              :func:`pandas.to_datetime`. Especially useful with databases without native datetime support,
              such as SQLite.

        localize_tz : str or None, default None
            Localize naive datetime columns of the returned :class:`pandas.DataFrame` to specified timezone.
            If None no localization is performed.

        target_tz : str or None, default None
            The timezone to convert the datetime columns of the returned :class:`pandas.DataFrame` into after
            they have been localized. If None no conversion is performed.

        dtypes : dict or None, default None
            Desired data types for specified columns `{'column_name': data type}`.
            Use pandas or numpy data types or string names of those.
            If None no data type conversion is performed.

        chunksize : int or None, default None
            If `chunksize` is specified an iterator of DataFrames will be returned where `chunksize`
            is the number of rows in each :class:`pandas.DataFrame`.
            If `chunksize` is supplied timezone localization and conversion as well as dtype
            conversion cannot be performed i.e. `localize_tz`, `target_tz` and `dtypes` have
            no effect.

        coerce_float : bool, default True
            Attempts to convert values of non-string, non-numeric objects (like decimal.Decimal)
            to floating point, useful for SQL result sets.

        Returns
        -------
        df : pandas.DataFrame or Iterator[pandas.DataFrame]
            :class:`pandas.DataFrame` with the result of the query or an iterator of DataFrames
            if `chunksize` is specified.

        Raises
        ------
        pandemy.LoadTableError
            If errors when loading the table using :func:`pandas.read_sql`.

        pandemy.SetIndexError
            If setting the index of the returned :class:`pandas.DataFrame` fails when `index_col` is specified
            and chunksize is None.

        pandemy.DataTypeConversionError
            If errors when converting data types using the `dtypes` parameter.

        See Also
        --------
        * :func:`pandas.read_sql` : Read SQL query or database table into a :class:`pandas.DataFrame`.

        * :func:`pandas.to_datetime` : The function used for datetime conversion with `parse_dates`.

        Examples
        --------
        When specifying the `chunksize` parameter the database connection must remain open
        to be able to process the DataFrames from the iterator. The processing
        *must* occur *within* the context manager:

        .. code-block:: python

           import pandemy

           db = pandemy.SQLiteDb(file='Runescape.db')

           with db.engine.connect() as conn:
               df_gen = db.load_table(sql='ItemTradedInStore', conn=conn, chunksize=3)

               for df in df_gen:
                   print(df)  # Process your DataFrames
                   ...
        """

        # To get the correct index column(s) if chunksize is specified
        # Not using index_col in pd.read_sql allows to set the data type of the index explicitly
        index_col_pd_read_sql = None if chunksize is None else index_col

        try:
            df = pd.read_sql(sql, con=conn, params=params, parse_dates=parse_dates, index_col=index_col_pd_read_sql,
                             columns=columns, chunksize=chunksize, coerce_float=coerce_float)
        except Exception as e:
            raise pandemy.LoadTableError(f'{e.args}\nsql={sql}\nparams={params}\ncolumns={columns}',
                                         data=(e.args, sql, params, columns)) from None

        if chunksize:
            return df

        if len(df.index) == 0:
            logger.warning('No rows were returned form the query.')

        # Convert specifed columns to desired data types
        if dtypes is not None:
            logger.debug('Convert columns to desired data types.')
            try:
                df = df.astype(dtype=dtypes)
            except KeyError:
                # The keys that are not in the columns
                difference = ', '.join([key for key in dtypes.keys() if key not in (cols := set(df.columns))])
                cols = df.columns.tolist()
                raise pandemy.DataTypeConversionError(f'Only column names can be used for the keys in dtypes parameter.'
                                                      f'\nColumns   : {cols}\ndtypes    : {dtypes}\n'
                                                      f'Difference: {difference}',
                                                      data=(cols, dtypes, difference)) from None
            except TypeError as e:
                raise pandemy.DataTypeConversionError(f'Cannot convert data types: {e.args[0]}.\ndtypes={dtypes}',
                                                      data=(e.args, dtypes)) from None

        # Localize (and convert) to desired timezone
        if localize_tz is not None:
            pandemy._datetime.datetime_columns_to_timezone(df=df, localize_tz=localize_tz, target_tz=target_tz)

        # Nr of rows and columns retrieved by the query
        nr_rows = df.shape[0]
        nr_cols = df.shape[1]

        # Set the index/indices column(s)
        if index_col is not None:
            try:
                df.set_index(index_col, inplace=True)
            except KeyError as e:
                raise pandemy.SetIndexError(f'Cannot set index to {index_col}: {e.args[0]}.',
                                            data=index_col) from None
            except TypeError as e:
                raise pandemy.SetIndexError(f'Cannot set index to {index_col}: '
                                            f'{e.args[0].replace("""keys""", "index_col")}.',
                                            data=index_col) from None

        logger.info(f'Successfully loaded {nr_rows} rows and {nr_cols} columns.')

        return df

    def upsert_table(
            self,
            df: pd.DataFrame,
            table: str,
            conn: Connection,
            where_cols: Sequence[str],
            upsert_cols: Optional[Union[str, Sequence[str]]] = 'all',
            upsert_index_cols: Union[bool, Sequence[str]] = False,
            update_only: bool = False,
            chunksize: Optional[int] = None,
            nan_to_none: bool = True,
            datetime_cols_dtype: Optional[str] = None,
            datetime_format: str = r'%Y-%m-%d %H:%M:%S',
            localize_tz: Optional[str] = None,
            target_tz: Optional[str] = None,
            dry_run: bool = False
    ) -> Union[Tuple[CursorResult, Optional[CursorResult]], Tuple[str, Optional[str]], Tuple[None, None]]:
        r"""Update a table with data from a :class:`pandas.DataFrame` and insert new rows if any.

        This method executes an UPDATE statement followed by an INSERT statement (UPSERT)
        to update the rows of a table with a :class:`pandas.DataFrame` and insert new rows.
        The INSERT statement can be omitted with the `update_only` parameter.

        The column names of `df` and `table` must match.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame with data to upsert.

        table : str
            The name of the table to upsert.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        where_cols : Sequence[str]
            The columns from `df` to use in the WHERE clause to identify the rows to upsert.

        upsert_cols : str or Sequence[str] or None, default 'all'
            The columns from `table` to upsert with data from `df`.
            The default string ``'all'`` will upsert all columns.
            If ``None`` no columns will be selected for upsert. This is useful if only columns
            of the index of `df` should be upserted by specifying `upsert_index_cols`.

        upsert_index_cols : bool or Sequence[str], default False
            If the index columns of `df` should be included in the columns to upsert.
            ``True`` indicates that the index should be included. If the index is a :class:`pandas.MultiIndex`
            a sequence of strings that maps against the levels to include can be used to only include the desired levels.
            ``False`` excludes the index column(s) from being upserted which is the default.

        update_only : bool, default False
            If ``True`` the `table` should only be updated and new rows not inserted.
            If ``False`` (the default) perform an update and insert new rows.

        chunksize : int or None, default None
            Divide `df` into chunks and perform the upsert in chunks of `chunksize` rows.
            If None all rows of `df` are processed in one chunk, which is the default.
            If `df` has many rows dividing it into chunks may increase performance.

        nan_to_none : bool, default True
            If columns with missing values (NaN values) that are of type :attr:`pandas.NA` :attr:`pandas.NaT`
            or :attr:`numpy.nan` should be converted to standard Python ``None``. Some databases do not support
            these types in parametrized SQL statements.

        datetime_cols_dtype : {'str', 'int'} or None, default None
            If the datetime columns of `df` should be converted to string or integer data types
            before upserting the table. SQLite cannot handle datetime objects as parameters
            and should use this option. If ``None`` conversion of datetime columns is omitted,
            which is the default. When using ``'int'`` the datetime columns are converted to the
            number of seconds since the Unix Epoch of 1970-01-01. The timezone of the datetime
            columns should be in UTC when using ``'int'``.

        datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
            The datetime (:meth:`strftime <datetime.datetime.strftime>`) format
            to use when converting datetime columns to strings.

        localize_tz : str or None, default None 
            The name of the timezone which to localize naive datetime columns into.
            If None (the default) timezone localization is omitted.

        target_tz : str or None, default None
            The name of the target timezone to convert timezone aware datetime columns, or columns that have been
            localized by `localize_tz`, into. If None (the default) timezone conversion is omitted.

        dry_run : bool, default False
            Do not execute the upsert. Instead return the SQL statements that would have been executed on the database.
            The return value is a tuple ('UPDATE statement', 'INSERT statement'). If `update_only` is ``True`` the
            INSERT statement will be ``None``.

        Returns
        -------
        Tuple[sqlalchemy.engine.CursorResult, Optional[sqlalchemy.engine.CursorResult]] or Tuple[str, Optional[str]] or Tuple[None, None]
            Result objects from the executed statements or the SQL statements that would have been executed
            if `dry_run` is ``True``. The result objects will be ``None`` if `df` is empty.

        Raises
        ------
        pandemy.ExecuteStatementError
            If an error occurs when executing the UPDATE and or INSERT statement.

        pandemy.InvalidColumnNameError
            If a column name of `upsert_cols` or `upsert_index_cols` are not
            among the columns or index of `df`.

        pandemy.InvalidInputError
            Invalid values or types for input parameters.

        pandemy.InvalidTableNameError
            If the supplied table name is invalid.

        See Also
        --------
        :meth:`~DatabaseManager.execute()` : Execute a SQL statement.

        :meth:`~DatabaseManager.load_table()` : Load a table into a :class:`pandas.DataFrame`.

        :meth:`~DatabaseManager.merge_df()` : Merge data from a :class:`pandas.DataFrame` into a table.

        :meth:`~DatabaseManager.save_df()` : Save a :class:`pandas.DataFrame` to a table in the database.

        Examples
        --------
        Create a simple table called *Customer* and insert some data from a :class:`pandas.DataFrame` (``df``).
        Change the first row and add a new row to ``df``. Finally upsert the table with ``df``.

        >>> import pandas as pd
        >>> import pandemy
        >>> df = pd.DataFrame(data={
        ...         'CustomerId': [1, 2],
        ...         'CustomerName': ['Zezima',  'Dr Harlow']
        ...     }
        ... )
        >>> df = df.set_index('CustomerId')
        >>> df  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
                   CustomerName
        CustomerId
        1                Zezima
        2             Dr Harlow
        >>> db = pandemy.SQLiteDb()  # Create an in-memory database
        >>> with db.engine.begin() as conn:
        ...     _ = db.execute(
        ...         sql=(
        ...             'CREATE TABLE Customer('
        ...             'CustomerId INTEGER PRIMARY KEY, '
        ...             'CustomerName TEXT NOT NULL)'
        ...         ),
        ...         conn=conn
        ...     )
        ...     db.save_df(df=df, table='Customer', conn=conn)
        >>> df.loc[1, 'CustomerName'] = 'Baraek'  # Change data
        >>> df.loc[3, 'CustomerName'] = 'Mosol Rei'  # Add new data
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                   CustomerName
        CustomerId
        1                Baraek
        2             Dr Harlow
        3             Mosol Rei
        >>> with db.engine.begin() as conn:
        ...     _, _ = db.upsert_table(
        ...         df=df,
        ...         table='Customer',
        ...         conn=conn,
        ...         where_cols=['CustomerId'],
        ...         upsert_index_cols=True
        ...     )
        ...     df_upserted = db.load_table(
        ...         sql='SELECT * FROM Customer ORDER BY CustomerId ASC',
        ...         conn=conn,
        ...         index_col='CustomerId'
        ...     )
        >>> df_upserted  # doctest: +NORMALIZE_WHITESPACE
                   CustomerName
        CustomerId
        1                Baraek
        2             Dr Harlow
        3             Mosol Rei
        """

        self._is_valid_table_name(table=table)
        self._validate_chunksize(chunksize=chunksize)

        df_upsert, update_cols, insert_cols = self._prepare_input_data_for_modify_statements(
                                                    df=df,
                                                    update_cols=upsert_cols,
                                                    update_index_cols=upsert_index_cols,
                                                    where_cols=where_cols
                                                )

        # Create the UPDATE statement
        update_stmt = self._create_update_statement(
                            table=table,
                            update_cols=update_cols,
                            where_cols=where_cols,
                            space_factor=1
                        )

        # Create the INSERT statement
        if not update_only:
            insert_stmt = self._create_insert_into_where_not_exists_statement(
                                table=table,
                                insert_cols=insert_cols,
                                where_cols=where_cols,
                                select_values_space_factor=2,
                                where_cols_space_factor=4
                            )
        else:
            insert_stmt = None

        if dry_run:  # Early exit to check the statements
            return update_stmt, insert_stmt

        if datetime_cols_dtype is None and localize_tz is None and target_tz is None:
            pass
        else:
            df_upsert = pandemy._datetime.convert_datetime_columns(
                df=df_upsert,
                dtype=datetime_cols_dtype,
                datetime_format=datetime_format,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # Convert missing values
        if nan_to_none and df_upsert.isna().any().any():  # If at least 1 missing value
            df_upsert = pandemy._dataframe.convert_nan_to_none(df=df_upsert)

        # Turn the DataFrame into an iterator yielding a list of dicts [{parameter: value}, {...}]
        params_iter = pandemy._dataframe.df_to_parameters_in_chunks(df=df_upsert, chunksize=chunksize)

        # Perform the UPDATE and optionally INSERT
        result_update, result_insert = None, None  # In case of df_upsert being empty
        for chunk, params in enumerate(params_iter, start=1):
            logger.debug(f'UPSERT {len(params)} rows, chunk {chunk}')
            try:
                result_update = conn.execute(text(update_stmt), params)
                result_insert = conn.execute(text(insert_stmt), params) if not update_only else None
            except Exception as e:
                log_level = 40  # ERROR
                raise pandemy.ExecuteStatementError(f'{type(e).__name__}: {e.args}', data=e.args) from None
            else:
                log_level = 10  # DEBUG
            finally:
                logger.log(log_level, f'UPDATE statement for table {table}:\n{update_stmt}\n')
                logger.log(log_level, f'update_only={update_only}')
                logger.log(log_level, f'INSERT statement for table {table}:\n{insert_stmt}')
                logger.log(
                    log_level,
                    'params:\n{params_row}'.format(params_row="\n".join(str(row) for row in params))
                )

        return result_update, result_insert

    def merge_df(
            self,
            df: pd.DataFrame,
            table: str,
            conn: Connection,
            on_cols: Sequence[str],
            merge_cols: Optional[Union[str, Sequence[str]]] = 'all',
            merge_index_cols: Union[bool, Sequence[str]] = False,
            omit_update_where_clause: bool = True,
            chunksize: Optional[int] = None,
            nan_to_none: bool = True,
            datetime_cols_dtype: Optional[str] = None,
            datetime_format: str = r'%Y-%m-%d %H:%M:%S',
            localize_tz: Optional[str] = None,
            target_tz: Optional[str] = None,
            dry_run: bool = False) -> Union[CursorResult, str, None]:
        r"""Merge data from a :class:`pandas.DataFrame` into a table.

        This method executes a combined UPDATE and INSERT statement on a table using the
        MERGE statement. The method is similar to :meth:`~DatabaseManager.upsert_table()`
        but it only executes one statement instead of two.

        Databases implemented in Pandemy that support the MERGE statement:

        - Oracle

        The column names of `df` and `table` must match.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame with data to merge into `table`.

        table : str
            The name of the table to merge data into.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        on_cols : Sequence[str] or None
            The columns from `df` to use in the ON clause to identify how to merge rows into `table`.

        merge_cols : str or Sequence[str] or None, default 'all'
            The columns of `table` to merge (update or insert) with data from `df`.
            The default string ``'all'`` will update all columns.
            If ``None`` no columns will be selected for merge. This is useful if only columns
            of the index of `df` should be updated by specifying `merge_index_cols`.

        merge_index_cols : bool or Sequence[str], default False
            If the index columns of `df` should be included in the columns to merge.
            ``True`` indicates that the index should be included. If the index is a :class:`pandas.MultiIndex`
            a sequence of strings that maps against the levels to include can be used to only include the desired levels.
            ``False`` excludes the index column(s) from being updated which is the default.

        omit_update_where_clause : bool, default True
            If the WHERE clause of the UPDATE clause should be omitted from the MERGE statement.
            The WHERE clause is implemented as OR conditions where the target and source columns to update
            are not equal.

            Databases in Pandemy that support this option are: Oracle

            Example of the SQL generated when ``omit_update_where_clause=True``:

            .. code-block::

               [...]
               WHEN MATCHED THEN
                   UPDATE
                   SET
                       t.IsAdventurer = s.IsAdventurer,
                       t.CustomerId = s.CustomerId,
                       t.CustomerName = s.CustomerName
                    WHERE
                       t.IsAdventurer <> s.IsAdventurer OR
                       t.CustomerId <> s.CustomerId OR
                       t.CustomerName <> s.CustomerName
               [...]

        chunksize : int or None, default None
            Divide `df` into chunks and perform the merge in chunks of `chunksize` rows.
            If None all rows of `df` are processed in one chunk, which is the default.
            If `df` has many rows dividing it into chunks may increase performance.

        nan_to_none : bool, default True
            If columns with missing values (NaN values) that are of type :attr:`pandas.NA` :attr:`pandas.NaT`
            or :attr:`numpy.nan` should be converted to standard Python ``None``. Some databases do not support
            these types in parametrized SQL statements.

        datetime_cols_dtype : {'str', 'int'} or None, default None
            If the datetime columns of `df` should be converted to string or integer data types
            before updating the table. If ``None`` conversion of datetime columns is omitted,
            which is the default. When using ``'int'`` the datetime columns are converted to the
            number of seconds since the Unix Epoch of 1970-01-01. The timezone of the datetime
            columns should be in UTC when using ``'int'``.

        datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
            The datetime (:meth:`strftime <datetime.datetime.strftime>`) format
            to use when converting datetime columns to strings.

        localize_tz : str or None, default None 
            The name of the timezone which to localize naive datetime columns into.
            If None (the default) timezone localization is omitted.

        target_tz : str or None, default None
            The name of the target timezone to convert timezone aware datetime columns, or columns that have been
            localized by `localize_tz`, into. If None (the default) timezone conversion is omitted.

        dry_run : bool, default False
            Do not execute the merge. Instead return the SQL statement
            that would have been executed on the database as a string.

        Returns
        -------
        sqlalchemy.engine.CursorResult or str or None
            A result object from the executed statement or the SQL statement
            that would have been executed if `dry_run` is ``True``.
            The result object will be ``None`` if `df` is empty.

        Raises
        ------
        pandemy.ExecuteStatementError
            If an error occurs when executing the MERGE statement.

        pandemy.InvalidColumnNameError
            If a column name of `merge_cols`, `merge_index_cols` or `on_cols` are not
            among the columns or index of the input DataFrame `df`.

        pandemy.InvalidInputError
            Invalid values or types for input parameters.

        pandemy.InvalidTableNameError
            If the supplied table name is invalid.

        pandemy.SQLStatementNotSupportedError
            If the database dialect does not support the MERGE statement.

        See Also
        --------
        * :meth:`~DatabaseManager.upsert_table()` : Update a table with a :class:`pandas.DataFrame` and optionally insert new rows.

        Examples
        --------
        Create a MERGE statement from an empty :class:`pandas.DataFrame` that represents a table in a database by
        using the parameter ``dry_run=True``. The :meth:`begin() <sqlalchemy.engine.Engine.begin>` method of the
        :class:`DatabaseManager.engine <sqlalchemy.engine.Engine>` is mocked to avoid having to connect to a real database.

        >>> import pandas as pd
        >>> import pandemy
        >>> from unittest.mock import MagicMock
        >>> df = pd.DataFrame(columns=['ItemId', 'ItemName', 'MemberOnly', 'IsAdventurer'])
        >>> df = df.set_index('ItemId')
        >>> df  # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
        Empty DataFrame
        Columns: [ItemName, MemberOnly, IsAdventurer]
        Index: []
        >>> db = pandemy.OracleDb(
        ...     username='Fred_the_Farmer',
        ...     password='Penguins-sheep-are-not',
        ...     host='fred.farmer.rs',
        ...     port=1234,
        ...     service_name='woollysheep'
        ... )
        >>> db.engine.begin = MagicMock()  # Mock the begin method
        >>> with db.engine.begin() as conn:
        ...     merge_stmt = db.merge_df(
        ...         df=df,
        ...         table='Item',
        ...         conn=conn,
        ...         on_cols=['ItemName'],
        ...         merge_cols='all',
        ...         merge_index_cols=False,
        ...         dry_run=True
        ...     )
        >>> print(merge_stmt)  # doctest: +NORMALIZE_WHITESPACE
        MERGE INTO Item t
        <BLANKLINE>
        USING (
            SELECT
                :ItemName AS ItemName,
                :MemberOnly AS MemberOnly,
                :IsAdventurer AS IsAdventurer
            FROM DUAL
        ) s
        <BLANKLINE>
        ON (
            t.ItemName = s.ItemName
        )
        <BLANKLINE>
        WHEN MATCHED THEN
            UPDATE
            SET
                t.MemberOnly = s.MemberOnly,
                t.IsAdventurer = s.IsAdventurer
        <BLANKLINE>
        WHEN NOT MATCHED THEN
            INSERT (
                t.ItemName,
                t.MemberOnly,
                t.IsAdventurer
            )
            VALUES (
                s.ItemName,
                s.MemberOnly,
                s.IsAdventurer
            )
        """

        self._supports_merge_statement()
        self._is_valid_table_name(table=table)
        self._validate_chunksize(chunksize=chunksize)

        df_merge, update_cols, insert_cols = self._prepare_input_data_for_modify_statements(
                                                    df=df,
                                                    update_cols=merge_cols,
                                                    update_index_cols=merge_index_cols,
                                                    where_cols=on_cols
                                                )

        # Create the MERGE statement
        merge_stmt = self._create_merge_statement(
                            table=table,
                            insert_cols=insert_cols,
                            on_cols=on_cols,
                            update_cols=update_cols,
                            omit_update_where_clause=omit_update_where_clause
                        )

        if dry_run:  # Early exit to check the statement
            return merge_stmt

        if datetime_cols_dtype is None and localize_tz is None and target_tz is None:
            pass
        else:
            df_merge = pandemy._datetime.convert_datetime_columns(
                df=df_merge,
                dtype=datetime_cols_dtype,
                datetime_format=datetime_format,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # Convert missing values
        if nan_to_none and df_merge.isna().any().any():  # If at least 1 missing value
            df_merge = pandemy._dataframe.convert_nan_to_none(df=df_merge)

        # Turn the DataFrame into an iterator yielding a list of dicts [{parameter: value}, {...}]
        params_iter = pandemy._dataframe.df_to_parameters_in_chunks(df=df_merge, chunksize=chunksize)

        # Perform the MERGE
        result_merge = None  # In case of df_merge being empty
        for chunk, params in enumerate(params_iter, start=1):
            logger.debug(f'MERGE {len(params)} rows, chunk {chunk}')
            try:
                result_merge = conn.execute(text(merge_stmt), params)
            except Exception as e:
                log_level = 40  # ERROR
                raise pandemy.ExecuteStatementError(f'{type(e).__name__}: {e.args}', data=e.args) from None
            else:
                log_level = 10  # DEBUG
            finally:
                logger.log(log_level, f'MERGE statement for table {table}:\n{merge_stmt}\n')
                logger.log(
                    log_level,
                    'params:\n{params_row}'.format(params_row="\n".join(str(row) for row in params))
                )

        return result_merge

# ===============================================================
# SQLite
# ===============================================================


class SQLiteDb(DatabaseManager):
    r"""A SQLite :class:`DatabaseManager`.

    .. versionadded:: 1.2.0
       Parameters: `driver`, `url`, `connect_args` and `engine`.

    Parameters
    ----------
    file : str or pathlib.Path, default ':memory:'
        The path (absolute or relative) to the SQLite database file.
        The default creates an in-memory database.

    must_exist : bool, default False
        If ``True`` validate that `file` exists unless ``file=':memory:'``.
        If it does not exist :exc:`pandemy.DatabaseFileNotFoundError`
        is raised. If ``False`` the validation is omitted.

    container : SQLContainer or None, default None
        A container of database statements that the SQLite :class:`DatabaseManager` can use.

    driver : str, default 'sqlite3'
        The database driver to use. The default is the Python built-in module :mod:`sqlite3`,
        which is also the default driver of SQLAlchemy.
        When the default is used no driver name is displayed in the connection URL.

    url : :class:`str` or :class:`sqlalchemy.engine.URL` or None, default None
        A SQLAlchemy connection URL to use for creating the database engine.
        It overrides the value of `file` and `must_exist`.

    connect_args : dict or None, default None
        Additional arguments sent to the driver upon connection that further
        customizes the connection.

    engine_config : dict or None, default None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    engine : :class:`sqlalchemy.engine.Engine` or None, default None
        A SQLAlchemy Engine to use as the database engine of :class:`SQLiteDb`.
        It overrides the value of `file` and `must_exist`. If specified the value
        of `url` should be ``None``. If ``None`` (the default) the engine will be
        created from `file` or `url`.

    **kwargs : dict
        Additional keyword arguments that are not used by :class:`SQLiteDb`.

    Raises
    ------
    pandemy.CreateConnectionURLError
        If there are errors with `url`.

    pandemy.CreateEngineError
        If the creation of the database engine fails.

    pandemy.DatabaseFileNotFoundError
        If the database `file` does not exist when ``must_exist=True``.

    pandemy.InvalidInputError
        If the parameters are specified with invalid input.

    See Also
    --------
    * :class:`pandemy.DatabaseManager` : The parent class.

    * :func:`sqlalchemy.create_engine` : The function used to create the database engine.

    * `SQLite dialect and drivers`_ : The SQLite dialect and drivers in SQLAlchemy.

    * `SQLite <https://sqlite.org/index.html>`_ : The SQLite homepage.

    .. _SQLite dialect and drivers: https://docs.sqlalchemy.org/en/14/dialects/sqlite.html
    """

    __slots__ = ('file', 'must_exist', 'driver')

    def __init__(
        self,
        file: Union[str, Path] = ':memory:',
        must_exist: bool = False,
        container: Optional[pandemy.SQLContainer] = None,
        driver: str = 'sqlite3',
        url: Optional[Union[str, URL]] = None,
        connect_args: Optional[Dict[str, Any]] = None,
        engine_config: Optional[Dict[str, Any]] = None,
        engine: Optional[Engine] = None,
        **kwargs
    ) -> None:

        # file
        if isinstance(file, Path) or file == ':memory:':
            self.file = file
        elif isinstance(file, str):
            self.file = Path(file)
        else:
            raise pandemy.InvalidInputError(
                f'file must be a string or pathlib.Path. Received: {file}, {type(file)}'
            )

        # must_exist
        if isinstance(must_exist, bool):
            self.must_exist = must_exist
        else:
            raise pandemy.InvalidInputError(
                f'must_exist must be a boolean. Received: {must_exist}, {type(must_exist)}.'
            )

        if file != ':memory:' and url is None and engine is None:
            if not self.file.exists() and must_exist:
                raise pandemy.DatabaseFileNotFoundError(
                    f'file={self.file!r} does not exist and and must_exist={must_exist!r}. '
                    'Cannot instantiate the SQLite DatabaseManager.'
                )

        self.driver = driver

        # Build the connection URL
        if url is None and engine is None:
            try:
                url = URL.create(
                    drivername='sqlite' if driver == 'sqlite3' else f'sqlite+{driver}',
                    database=str(self.file)
                )
            except Exception as e:
                raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None
        elif url is not None and engine is None:
            url = make_url(url)
            self.file = url.database
            self.driver = 'sqlite3' if len(dvr := url.drivername.split('+')) == 1 else dvr[1]  # drivername = backend+driver

        super().__init__(
            url=url,
            container=container,
            connect_args=connect_args,
            engine_config=engine_config,
            engine=engine
        )

    def __str__(self):
        r"""String representation of the object."""

        return f"SQLiteDb(file='{self.file}', must_exist={self.must_exist})"

    @property
    def conn_str(self) -> str:
        r"""Backwards compatibility for the `conn_str` attribute.

        The `conn_str` attribute is deprecated in version 1.2.0 and replaced by the url attribute.
        """

        warnings.warn(
            message='conn_str attribute is deprecated in version 1.2.0 and replaced by url. Use SQLiteDb.url instead.',
            category=DeprecationWarning,
            stacklevel=2
        )

        return str(self.url)

    def manage_foreign_keys(self, conn: Connection, action: str = 'ON') -> None:
        r"""Manage how the database handles foreign key constraints.

        In SQLite the check of foreign key constraints is not enabled by default.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        action : {'ON', 'OFF'}
            Enable ('ON') or disable ('OFF') the check of foreign key constraints.

        Raises
        ------
        pandemy.InvalidInputError
            If invalid input is supplied to `action`.

        pandemy.ExecuteStatementError
            If the enabling/disabling of the foreign key constraints fails.

        See Also
        --------
        * :meth:`~DatabaseManager.execute` : Execute a SQL statement.

        Examples
        --------
        Enable and trigger a foreign key constraint using an in-memory database.

        >>> import pandemy
        >>> db = pandemy.SQLiteDb()  # Create an in-memory database
        >>> with db.engine.begin() as conn:
        ...     db.manage_foreign_keys(conn=conn, action='ON')
        ...     db.execute(
        ...         sql="CREATE TABLE Owner(OwnerId INTEGER PRIMARY KEY, OwnerName TEXT)",
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql=(
        ...             "CREATE TABLE Store("
        ...             "StoreId INTEGER PRIMARY KEY, "
        ...             "StoreName TEXT, "
        ...             "OwnerId INTEGER REFERENCES Owner(OwnerId)"
        ...             ")"
        ...         ),
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql="INSERT INTO Owner(OwnerId, OwnerName) VALUES(1, 'Shop keeper')",
        ...         conn=conn
        ...     )
        ...     db.execute(
        ...         sql=(
        ...             "INSERT INTO Store(StoreId, StoreName, OwnerId) "
        ...             "VALUES(1, 'Lumbridge General Supplies', 2)"
        ...         ),
        ...         conn=conn  # OwnerId = 2 is not a valid FOREIGN KEY reference
        ...     )
        ...     db.manage_foreign_keys(conn=conn, action='ON')
        ...     db.execute(
        ...         sql=(
        ...             "INSERT INTO Store(StoreId, StoreName, OwnerId) "
        ...             "VALUES(1, 'Falador General Store', 3)"
        ...         ),
        ...         conn=conn  # OwnerId = 3 is not a valid FOREIGN KEY reference
        ...     )  # doctest: +IGNORE_EXCEPTION_DETAIL, +ELLIPSIS
        Traceback (most recent call last):
        ...
        pandemy.exceptions.ExecuteStatementError: IntegrityError: ('(sqlite3.IntegrityError) FOREIGN KEY constraint failed',)
        """

        actions = {'ON', 'OFF'}

        if not isinstance(action, str):
            error = True
        elif action not in actions:
            error = True
        else:
            error = False

        if error:
            raise pandemy.InvalidInputError(f'Invalid input action = {action}. Allowed values: {actions}',
                                            data=(action, actions))
        try:
            conn.execute(f'PRAGMA foreign_keys = {action};')
        except Exception as e:
            raise pandemy.ExecuteStatementError(f'{type(e).__name__}: {e.args}', data=e.args) from None


class OracleDb(DatabaseManager):
    r"""An Oracle :class:`DatabaseManager`.

    Requires the `cx_Oracle`_ package to be able
    to create a connection to the database.

    To use a DSN connection string specified in the Oracle connection config file, *tnsnames.ora*,
    set `host` to the desired network service name in *tnsnames.ora* and leave
    `port`, `service_name` and `sid` as ``None``. Using a *tnsnames.ora* file is needed
    to connect to `Oracle Cloud Autononmous Databases`_.

    .. versionadded:: 1.1.0

    .. versionadded:: 1.2.0
       The parameter `driver`.

    .. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/

    Parameters
    ----------
    username : str or None, default None
        The username of the database account.
        Must be specified if `url` or `engine` are ``None``.

    password : str or None, default None
        The password of the database account.
        Must be specified if `url` or `engine` are ``None``.

    host : str or None, default None
        The host name or server IP-address where the database is located.
        Must be specified if `url` or `engine` are ``None``.

    port : int or str or None, default None
        The port the `host` is listening on.
        The default port of Oracle databases is 1521.

    service_name : str or None, default None
        The name of the service used for the database connection.

    sid : str or None, default None
        The SID used for the connection. SID is the name of the database instance on the `host`.
        Note that `sid` and `service_name` should not be specified at the same time.

    container : SQLContainer or None, default None
        A container of database statements that :class:`OracleDb` can use.

    driver : str, default 'cx_oracle'
        The database driver to use. 

    url : :class:`str` or :class:`sqlalchemy.engine.URL` or None, default None
        A SQLAlchemy connection URL to use for creating the database engine.
        Specifying `url` overrides the parameters: `username`, `password`, `host`
        `port`, `service_name`, `sid` and `driver`. If `url` is specified
        `engine` should be ``None``.

    connect_args : dict or None, default None
        Additional arguments sent to the driver upon connection that further
        customizes the connection.

    engine_config : dict or None, default None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    engine : :class:`sqlalchemy.engine.Engine` or None, default None
        A SQLAlchemy Engine to use as the database engine of :class:`OracleDb`.
        If ``None`` (the default) the engine will be created from the other parameters.
        When specified it overrides the parameters: `username`, `password`, `host`, `port`,
        `service_name`, `sid` and `driver`. If specified `url` should be ``None``.

    **kwargs : dict
        Additional keyword arguments that are not used by :class:`OracleDb`.

    Raises
    ------
    pandemy.CreateConnectionURLError
        If the creation of the connection URL fails.

    pandemy.CreateEngineError
        If the creation of the database engine fails.

    pandemy.InvalidInputError
        If invalid combinations of the parameters are used.

    See Also
    --------
    * :class:`pandemy.DatabaseManager` : The parent class.

    * :func:`sqlalchemy.create_engine` : The function used to create the database engine.

    * `The cx_Oracle database driver`_ : Details of the cx_Oracle driver and its usage in SQLAlchemy.

    * `cx_Oracle documentation <https://cx-oracle.readthedocs.io/en/latest/>`_

    * `Specifying connect_args`_ : Details about the `connect_args` parameter.

    * `tnsnames.ora`_ : Oracle connection config file.

    * `Oracle Cloud Autononmous Databases`_

    .. _The cx_Oracle database driver: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#module-sqlalchemy.dialects.oracle.cx_oracle

    .. _Specifying connect_args: https://docs.sqlalchemy.org/en/14/core/engines.html#custom-dbapi-args

    .. _tnsnames.ora: https://docs.oracle.com/database/121/NETRF/tnsnames.htm#NETRF259

    .. _Oracle Cloud Autononmous Databases : https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connecting-to-oracle-cloud-autonomous-databases

    Examples
    --------
    Create an instance of :class:`OracleDb` and connect using a service:

    >>> db = pandemy.OracleDb(
    ...     username='Fred_the_Farmer',
    ...     password='Penguins-sheep-are-not',
    ...     host='fred.farmer.rs',
    ...     port=1234,
    ...     service_name='woollysheep'
    ... )
    >>> str(db)
    'OracleDb(oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=woollysheep)'
    >>> db.username
    'Fred_the_Farmer'
    >>> db.password
    'Penguins-sheep-are-not'
    >>> db.host
    'fred.farmer.rs'
    >>> db.port
    1234
    >>> db.service_name
    'woollysheep'
    >>> db.url
    oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=woollysheep
    >>> db.engine
    Engine(oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=woollysheep)

    Connect with a DSN connection string using a net service name from a *tnsnames.ora* config file
    and supply additional connection arguments and engine configuration:

    >>> import cx_Oracle
    >>> connect_args = {
    ...     'encoding': 'UTF-8',
    ...     'nencoding': 'UTF-8',
    ...     'mode': cx_Oracle.SYSDBA,
    ...     'events': True
    ... }
    >>> engine_config = {
    ...     'coerce_to_unicode': False,
    ...     'arraysize': 40,
    ...     'auto_convert_lobs': False
    ... }
    >>> db = pandemy.OracleDb(
    ...     username='Fred_the_Farmer',
    ...     password='Penguins-sheep-are-not',
    ...     host='my_dsn_name',
    ...     connect_args=connect_args,
    ...     engine_config=engine_config
    ... )
    >>> db
    OracleDb(
        username='Fred_the_Farmer',
        password='***',
        host='my_dsn_name',
        port=None,
        service_name=None,
        sid=None,
        driver='cx_oracle',
        url=oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name,
        container=None,
        connect_args={'encoding': 'UTF-8', 'nencoding': 'UTF-8', 'mode': 2, 'events': True},
        engine_config={'coerce_to_unicode': False, 'arraysize': 40, 'auto_convert_lobs': False},
        engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name)
    )

    If you are familiar with the connection URL syntax of SQLAlchemy you can create an instance of
    :class:`OracleDb` directly from a URL:

    >>> url = 'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@my_dsn_name'
    >>> db = pandemy.OracleDb(url=url)
    >>> db
    OracleDb(
        username='Fred_the_Farmer',
        password='***',
        host='my_dsn_name',
        port=None,
        service_name=None,
        sid=None,
        driver='cx_oracle',
        url=oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name,
        container=None,
        connect_args={},
        engine_config={},
        engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name)
    )

    If you already have a database :class:`engine <sqlalchemy.engine.Engine>` and would like to use it
    with :class:`OracleDb` simply create the instance like this:

    >>> from sqlalchemy import create_engine
    >>> url = 'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears'
    >>> engine = create_engine(url, coerce_to_unicode=False)
    >>> db = pandemy.OracleDb(engine=engine)
    >>> db
    OracleDb(
        username='Fred_the_Farmer',
        password='***',
        host='fred.farmer.rs',
        port=1234,
        service_name=None,
        sid='shears',
        driver='cx_oracle',
        url=oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears,
        container=None,
        connect_args={},
        engine_config={},
        engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears)
    )

    This is useful if you have special needs for creating the engine that cannot be accomplished
    with the engine constructor of :class:`OracleDb`. See for instance the `cx_Oracle SessionPool`_
    example in the SQLAlchemy docs.

    .. _cx_Oracle SessionPool: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#using-cx-oracle-sessionpool
    """

    # Class variables
    # ---------------

    # Template statement to insert new rows, that do not exist already, into a table.
    _insert_into_where_not_exists_stmt: str = (
        """INSERT INTO :table (
    :insert_cols
)
    SELECT
        :select_values
    FROM DUAL
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM :table
            WHERE
                :where_cols
        )"""
    )

    # MERGE DataFrame statement
    _merge_df_stmt: str = (
        """MERGE INTO :table :target

USING (
    SELECT
        :select_values
    FROM DUAL
) :source

ON (
    :on
)

WHEN MATCHED THEN
    UPDATE
    SET
        :update_cols
    WHERE
        :update_where_cols

WHEN NOT MATCHED THEN
    INSERT (
        :insert_cols
    )
    VALUES (
        :insert_values
    )"""
    )

    __slots__ = ('username', 'password', 'host', 'port', 'service_name', 'sid', 'driver')

    def __init__(self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[int, str]] = None,
        service_name: Optional[str] = None,
        sid: Optional[str] = None,
        container: Optional[pandemy.SQLContainer] = None,
        driver: str = 'cx_oracle',
        url: Optional[Union[str, URL]] = None,
        connect_args: Optional[Dict[str, Any]] = None,
        engine_config: Optional[Dict[str, Any]] = None,
        engine: Optional[Engine] = None,
        **kwargs: dict
    ) -> None:

        # Set parameters from provided url or engine
        if url is not None or engine is not None: 
            try:
                url = make_url(url) if url is not None else engine.url
            except Exception as e:
                raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None

            self.username = url.username
            self.password = url.password
            self.host = url.host
            self.port = url.port
            self.service_name = url.query.get('service_name')
            self.sid = url.database

            if len(dvr := url.drivername.split('+')) == 1 or not dvr[1]:   # drivername = backend+driver
                raise pandemy.CreateConnectionURLError(f'The url does not contain a driver. url={url}', data=url)
            else:
                self.driver = dvr[1]
        # Build the connection URL
        else:
            required_not_none = {'username': username, 'password': password, 'host': host}
            if len(none_result := {key: value for key, value in required_not_none.items() if value is None}) == 3:
                raise pandemy.InvalidInputError(
                    'username, password, host, url and engine cannot be None at the same time.'
                )
            elif len(none_result) in {1, 2}:
                raise pandemy.InvalidInputError(
                    'username, password and host must all be specified if url and engine are None. '
                    f'Got: {required_not_none}',
                    data=required_not_none
                )
            else:
                pass

            # Check that service_name and sid are not used together
            if service_name is not None and sid is not None:
                raise pandemy.InvalidInputError(
                            'Use either service_name or sid to connect to the database, not both! '
                            f'service={service_name!r}, sid={sid!r}',
                            data=(service_name, sid)
                        )

            port = port if port is None else int(port)
            self.username = username
            self.password = password
            self.host = host
            self.port = port
            self.service_name = service_name
            self.sid = sid
            self.driver = driver
            try:
                url = URL.create(
                        drivername=f'oracle+{driver}',
                        username=username,
                        password=password,
                        host=host,
                        port=port,
                        database=sid,
                        query={'service_name': service_name} if service_name is not None else {}
                    )
            except Exception as e:
                raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None

        super().__init__(
            url=url if engine is None else None,
            container=container,
            connect_args=connect_args,
            engine_config=engine_config,
            engine=engine
        )

    @classmethod
    def from_url(cls,
                 url: Union[str, URL],
                 container: Optional[pandemy.SQLContainer] = None,
                 engine_config: Optional[Dict[str, Any]] = None) -> OracleDb:
        r"""Create an instance of :class:`OracleDb` from a SQLAlchemy :class:`URL <sqlalchemy.engine.URL>`.

        .. deprecated:: 1.2.0
           Use the `url` parameter of the normal initializer of :class:`OracleDb` instead.

        Parameters
        ----------
        url : str or sqlalchemy.engine.URL
            A SQLAlchemy URL to use for creating the database engine.

        container : SQLContainer or None, default None
            A container of database statements that :class:`OracleDb` can use.

        engine_config : dict or None, default None
            Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

        Raises
        ------
        pandemy.CreateConnectionURLError
            If `url` is invalid.

        Examples
        --------
        If you are familiar with the connection URL syntax of SQLAlchemy you can create an instance of
        :class:`OracleDb` directly from a URL:

        >>> url = 'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@my_dsn_name'
        >>> db = pandemy.OracleDb.from_url(url)
        >>> db
        OracleDb(
            username='Fred_the_Farmer',
            password='***',
            host='my_dsn_name',
            port=None,
            service_name=None,
            sid=None,
            driver='cx_oracle',
            url=oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name,
            container=None,
            connect_args={},
            engine_config={},
            engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name)
        )
        """

        warnings.warn(
            message=(
                'from_url is deprecated in version 1.2.0. '
                'Use the url parameter of the normal initializer of OracleDb instead. '
                'db = pandemy.OracleDb(url=url)'
            ),
            category=DeprecationWarning,
            stacklevel=2
        )

        return cls(url=url, container=container, engine_config=engine_config)

    @classmethod
    def from_engine(cls, engine: Engine, container: Optional[pandemy.SQLContainer] = None) -> OracleDb:
        r"""Create an instance of :class:`OracleDb` from a SQLAlchemy :class:`Engine <sqlalchemy.engine.Engine>`.

        .. deprecated:: 1.2.0
           Use the `engine` parameter of the normal initializer of :class:`OracleDb` instead.

        Parameters
        ----------
        engine : sqlalchemy.engine.Engine
            A SQLAlchemy Engine to use as the database engine of :class:`OracleDb`.

        container : SQLContainer or None, default None
            A container of database statements that :class:`OracleDb` can use.

        Examples
        --------
        If you already have a database :class:`engine <sqlalchemy.engine.Engine>` and would like to use it
        with :class:`OracleDb` simply create the instance like this:

        >>> from sqlalchemy import create_engine
        >>> url = 'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears'
        >>> engine = create_engine(url, coerce_to_unicode=False)
        >>> db = pandemy.OracleDb.from_engine(engine)
        >>> db
        OracleDb(
            username='Fred_the_Farmer',
            password='***',
            host='fred.farmer.rs',
            port=1234,
            service_name=None,
            sid='shears',
            driver='cx_oracle',
            url=oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears,
            container=None,
            connect_args={},
            engine_config={},
            engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears)
        )

        This is useful if you have special needs for creating the engine that cannot be accomplished
        with the engine constructor of :class:`OracleDb`. See for instance the `cx_Oracle SessionPool`_
        example in the SQLAlchemy docs.

        .. _cx_Oracle SessionPool: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#using-cx-oracle-sessionpool
        """

        warnings.warn(
            message=(
                'from_engine is deprecated in version 1.2.0. '
                'Use the engine parameter of the normal initializer of OracleDb instead. '
                'db = pandemy.OracleDb(engine=engine)'
            ),
            category=DeprecationWarning,
            stacklevel=2
        )

        return cls(engine=engine, container=container)
