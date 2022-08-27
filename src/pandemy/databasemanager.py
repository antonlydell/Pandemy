r"""Contains the classes that represent the database interface.

The Database is managed by the :class:`DatabaseManager <pandemy.DatabaseManager>` class.
Each SQL-dialect is implemented as a subclass of :class:`DatabaseManager <pandemy.DatabaseManager>`.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
from __future__ import annotations
from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union
import urllib

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


class DatabaseManager(ABC):
    r"""Base class with functionality for managing a database.

    Each database type will subclass from :class:`DatabaseManager` and
    implement the initializer which is specific to each database type.
    :class:`DatabaseManager` is never used on its own, but merely provides the methods
    to interact with the database to its subclasses.

    Initialization of a :class:`DatabaseManager` creates the connection string (`conn_str`)
    and the database `engine`, which are used to connect to and interact with the database.
    These are available as attributes on the instance. The initializer can contain any number
    of parameters needed to connect to the database and should always support
    `container`, `engine_config` and `\*\*kwargs`.

    Parameters
    ----------
    container : SQLContainer or None, default None
        A container of database statements that can be used by the :class:`DatabaseManager`.

    engine_config : dict or None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    **kwargs : dict
        Additional keyword arguments that are not used by the :class:`DatabaseManager`.

    Attributes
    ----------
    conn_str : str
        The connection string for connecting to the database.

    engine : :class:`sqlalchemy.engine.Engine`
        The engine for interacting with the database.
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

    @abstractmethod
    def __init__(self, container: Optional[pandemy.SQLContainer] = None,
                 engine_config: Optional[dict] = None,
                 **kwargs) -> None:
        pass

    def __str__(self) -> str:
        r"""String representation of the object."""

        return f'{self.__class__.__name__}({repr(self.url)})'

    def __repr__(self) -> str:
        r"""Debug representation of the object."""

        # Get the attribute names of the class instance
        attributes = self.__dict__.items()

        # The space to add before each new parameter on a new line
        space = ' ' * 4

        # The name of the class
        repr_str = f'{self.__class__.__name__}(\n'

        # Append the attribute names and values
        for attrib, value in attributes:
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

           db = SQLiteDb(file='mydb.db')

           with db.engine.connect() as conn:
               result = db.execute('SELECT * FROM MyTable;', conn=conn)

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

    def save_df(self, df: pd.DataFrame, table: str, conn: Connection, if_exists: str = 'append',
                index: bool = True, index_label: Optional[Union[str, Sequence[str]]] = None,
                chunksize: Optional[int] = None, schema: Optional[str] = None,
                dtype: Optional[Union[Dict[str, Union[str, object]], object]] = None,
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

        if_exists : str, {'append', 'replace', 'fail'}
            How to update an existing table in the database:

            * 'append': Append the :class:`pandas.DataFrame` to the existing table.

            * 'replace': Delete all records from the table and then write the :class:`pandas.DataFrame` to the table.

            * 'fail': Raise :exc:`pandemy.TableExistsError` if the table exists.

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
        """

        # ==========================================
        # Validate input
        # ==========================================

        if not isinstance(df, pd.DataFrame):
            raise pandemy.InvalidInputError(f'df must be of type pandas.DataFrame. Got {type(df)}. df = {df}.')

        # Validate if_exists
        if not isinstance(if_exists, str) or if_exists not in {'replace', 'append', 'fail'}:
            raise pandemy.InvalidInputError(f'Invalid input if_exists = {if_exists}. '
                                            "Expected 'append', 'replace' or 'fail'.")

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
        else:
            self._is_valid_table_name(table=table)

        # ==========================================
        # Write the DataFrame to the SQL table
        # ==========================================

        try:
            df.to_sql(table, con=conn, if_exists=if_exists, index=index, index_label=index_label,
                      chunksize=chunksize, schema=schema, dtype=dtype, method=method)

        except ValueError:
            raise pandemy.TableExistsError(f'Table {table} already exists! and if_exists = {if_exists}') from None

        # Unexpected error
        except Exception as e:
            raise pandemy.SaveDataFrameError(f'Could not save DataFrame to table {table}: {e.args}',
                                             data=e.args) from None

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

           db = pandemy.SQLiteDb(file='mydb.db')

           with db.engine.connect() as conn:
               df_gen = db.load_table(sql='MyTableName', conn=conn, chunksize=3)

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
            update_cols: Optional[Union[str, Sequence[str]]] = 'all',
            update_index_cols: Union[bool, Sequence[str]] = False,
            update_only: bool = False,
            nan_to_none: bool = True,
            datetime_cols_dtype: Optional[str] = None,
            datetime_format: str = r'%Y-%m-%d %H:%M:%S',
            dry_run: bool = False) -> Union[Tuple[CursorResult, Optional[CursorResult]], Tuple[str, str]]:
        r"""Update a table with data from a :class:`pandas.DataFrame` and insert new rows if any.

        This method executes an UPDATE statement followed by an INSERT statement (UPSERT)
        to update the rows of a table with a :class:`pandas.DataFrame` and insert new rows.
        The INSERT statement can be omitted with the `update_only` parameter.

        The column names of `df` and `table` must match.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame with data to update and insert.

        table : str
            The name of the table to upsert.

        conn : sqlalchemy.engine.base.Connection
            An open connection to the database.

        where_cols : Sequence[str]
            The columns from `df` to use as the WHERE clause to identify
            the rows to update and insert.

        update_cols : str or Sequence[str] or None, default 'all'
            The columns from `table` to update with data from `df`.
            The default string ``'all'`` will update all columns.
            If ``None`` no columns will be selected for update. This is useful if only columns
            of the index of `df` should be updated by specifying `update_index_cols`.

        update_index_cols : bool or Sequence[str], default False
            If the index columns of `df` should be included in the columns to update.
            ``True`` indicates that the index should be included. If the index is a :class:`pandas.MultiIndex`
            a sequence of str that maps against the levels to include can be used to only include the desired levels.
            ``False`` excludes the index column(s) from being updated which is the default.

        update_only : bool, default False
            If ``True`` the `table` should only be updated and new rows not inserted.
            If ``False`` (the default) perform an update and insert new rows.

        nan_to_none: bool, default True
            If columns with missing values (NaN values) that are of type :attr:`pandas.NA` :attr:`pandas.NaT`
            or :attr:`numpy.nan` should be converted to standard Python ``None``. Some databases do not support
            these types in parametrized SQL statements.

        datetime_cols_dtype : {'str', 'int'} or None, default None
            If the datetime columns of `df` should be converted to string or integer data types
            before upserting the table. SQLite cannot handle datetime objects as parameters
            and should use this option. If ``None`` no conversion of datetime columns is performed,
            which is the default. When using ``'int'`` the datetime columns are converted to the
            number of seconds since the unix epoch of 1970-01-01 in UTC time zone.

        datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
            The datetime format to use when converting datetime columns to string.

        dry_run: bool, default False
            Do not execute the upsert. Instead return the SQL statements that would have been executed on the database.
            The return value is a tuple ('UPDATE statement', 'INSERT statement'). If `update_only` is ``True`` the
            INSERT statement will be ``None``.

        Returns
        -------
        Tuple[sqlalchemy.engine.CursorResult, Optional[sqlalchemy.engine.CursorResult]] or Tuple[str, Optional[str]]
            Result objects from the executed statements or the SQL statements that would have been executed
            if `dry_run` is ``True``.

        Raises
        ------
        pandemy.ExecuteStatementError
            If an error occurs when executing the UPDATE and or INSERT statement.

        pandemy.InvalidColumnNameError
            If a column name of `update_cols` or `update_index_cols` are not
            among the columns or index of the input DataFrame `df`.

        pandemy.InvalidInputError
            Invalid values or types for input parameters.

        pandemy.InvalidTableNameError
            If the supplied table name is invalid.

        See Also
        --------
        :meth:`~DatabaseManager.merge_df()` : Merge data from a :class:`pandas.DataFrame` into a table.

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
        >>> db = pandemy.SQLiteDb()  # Create an in memory database
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
        ...         df=df, table='Customer', conn=conn,
        ...         where_cols=['CustomerId'], update_index_cols=True
        ...     )
        ...     df_upserted = db.load_table(
        ...         sql='SELECT * FROM Customer ORDER BY CustomerId ASC',
        ...         conn=conn, index_col='CustomerId'
        ...     )
        >>> df_upserted  # doctest: +NORMALIZE_WHITESPACE
                   CustomerName
        CustomerId
        1                Baraek
        2             Dr Harlow
        3             Mosol Rei
        """

        self._is_valid_table_name(table=table)

        df_upsert, update_cols, insert_cols = self._prepare_input_data_for_modify_statements(
                                                    df=df,
                                                    update_cols=update_cols,
                                                    update_index_cols=update_index_cols,
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

        # Convert datetime columns to string or int
        if datetime_cols_dtype is not None:
            df_upsert = pandemy._datetime.convert_datetime_columns(
                df=df_upsert, dtype=datetime_cols_dtype, datetime_format=datetime_format
            )

        # Convert missing values
        if nan_to_none and df_upsert.isna().any().any():  # If at least 1 missing value
            df_upsert = pandemy._dataframe.convert_nan_to_none(df=df_upsert)

        # Turn the DataFrame into a list of dict [{parameter: value}, {...}]
        params = df_upsert.to_dict(orient='records')

        # Perform the UPDATE and optionally INSERT
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
            nan_to_none: bool = True,
            datetime_cols_dtype: Optional[str] = None,
            datetime_format: str = r'%Y-%m-%d %H:%M:%S',
            dry_run: bool = False) -> Union[CursorResult, str]:
        r"""Merge data from a :class:`pandas.DataFrame` into a table.

        This method performs a combined UPDATE and INSERT statement on a table using the
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
            a sequence of str that maps against the levels to include can be used to only include the desired levels.
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

        nan_to_none: bool, default True
            If columns with missing values (NaN values) that are of type :attr:`pandas.NA` :attr:`pandas.NaT`
            or :attr:`numpy.nan` should be converted to standard Python ``None``. Some databases do not support
            these types in parametrized SQL statements.

        datetime_cols_dtype : {'str', 'int'} or None, default None
            If the datetime columns of `df` should be converted to string or integer data types
            before updating the table. If ``None`` no conversion of datetime columns is performed,
            which is the default. When using ``'int'`` the datetime columns are converted to the
            number of seconds since the unix epoch of 1970-01-01 in UTC time zone.

        datetime_format : str, default r'%Y-%m-%d %H:%M:%S'
            The datetime format to use when converting datetime columns to string.

        dry_run: bool, default False
            Do not execute the merge. Instead return the SQL statement
            that would have been executed on the database as a string.

        Returns
        -------
        sqlalchemy.engine.CursorResult or str
            A result object from the executed statement or the SQL statement
            that would have been executed if `dry_run` is ``True``.

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
        """

        self._is_valid_table_name(table=table)
        self._supports_merge_statement()

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

        # Convert datetime columns to string or int
        if datetime_cols_dtype is not None:
            df_merge = pandemy._datetime.convert_datetime_columns(
                df=df_merge, dtype=datetime_cols_dtype, datetime_format=datetime_format
            )

        # Convert missing values
        if nan_to_none and df_merge.isna().any().any():  # If at least 1 missing value
            df_merge = pandemy._dataframe.convert_nan_to_none(df=df_merge)

        # Turn the DataFrame into a list of dict [{parameter: value}, {...}]
        params = df_merge.to_dict(orient='records')

        # Perform the MERGE
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

    Parameters
    ----------
    file : str or pathlib.Path, default ':memory:'
        The file (with path) to the SQLite database.
        The default creates an in memory database.

    must_exist : bool, default False
        If ``True`` validate that `file` exists unless ``file=':memory:'``.
        If it does not exist :exc:`pandemy.DatabaseFileNotFoundError`
        is raised. If ``False`` the validation is omitted.

    container : SQLContainer or None, default None
        A container of database statements that the SQLite :class:`DatabaseManager` can use.

    engine_config : dict or None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    **kwargs : dict
        Additional keyword arguments that are not used by :class:`SQLiteDb`.

    Raises
    ------
    pandemy.InvalidInputError
        If invalid types are supplied to `file`, `must_exist` and `container`.

    pandemy.DatabaseFileNotFoundError
        If the database `file` does not exist when ``must_exist=True``.

    pandemy.CreateEngineError
        If the creation of the database engine fails.

    See Also
    --------
    * :class:`pandemy.DatabaseManager` : The parent class.

    * :func:`sqlalchemy.create_engine` : The function used to create the database engine.

    * `SQLAlchemy SQLite dialect`_ : Implementation of the SQLite dialect in SQLAlchemy.

    * `SQLite <https://sqlite.org/index.html>`_ : The SQLite homepage.

    .. _SQLAlchemy SQLite dialect: https://docs.sqlalchemy.org/en/14/dialects/sqlite.html
    """

    def __init__(self, file: Union[str, Path] = ':memory:',
                 must_exist: bool = False,
                 container: Optional[pandemy.SQLContainer] = None,
                 engine_config: Optional[Dict[str, Any]] = None,
                 **kwargs: dict) -> None:

        self._set_attributes(file=file, must_exist=must_exist,
                             container=container, engine_config=engine_config)
        self._build_conn_str()
        self._create_engine()

    def _set_attributes(self,
                        file: Union[str, Path],
                        must_exist: bool,
                        container: Optional[pandemy.SQLContainer],
                        engine_config: Optional[Dict[str, Any]]) -> None:
        r"""Validate the input parameters from `self.__init__` and set attributes on the instance.

        Raises
        ------
        pandemy.InvalidInputError
            If invalid input is passed to the parameters.
        """

        # file
        # =================================
        if isinstance(file, Path) or file == ':memory:':
            self.file = file
        elif isinstance(file, str):
            self.file = Path(file)
        else:
            raise pandemy.InvalidInputError('file must be a string or pathlib.Path. '
                                            f'Received: {file} {type(file)}')

        # must_exist
        # =================================
        if isinstance(must_exist, bool):
            self.must_exist = must_exist
        else:
            raise pandemy.InvalidInputError('must_exist must be a boolean '
                                            f'Received: {must_exist} {type(must_exist)}')

        # container
        # =================================
        if container is None:
            self.container = container
        else:
            try:
                if issubclass(container, pandemy.SQLContainer):  # Throws TypeError if container is not a class
                    self.container = container
                    error = False
                else:
                    error = True
            except TypeError:
                error = True

            if error:
                raise pandemy.InvalidInputError('container must be a subclass of pandemy.SQLContainer '
                                                f'Received: {container} {type(container)}') from None

        # engine_config
        # =================================
        if engine_config is None or isinstance(engine_config, dict):
            self.engine_config = engine_config
        else:
            raise pandemy.InvalidInputError('engine_config must be a dict '
                                            f'Received: {engine_config} {type(engine_config)}') from None

    def _build_conn_str(self) -> None:
        r"""Build the database connection string and assign it to `self.conn_str`.

        Raises
        ------
        pandemy.DatabaseFileNotFoundError
            If the database file `self.file` does not exist.
        """

        if self.file == ':memory:':  # Use an in memory database
            self.conn_str = r'sqlite://'

        else:  # Use a database on file
            if not self.file.exists() and self.must_exist:
                raise pandemy.DatabaseFileNotFoundError(f"file='{self.file}' does not exist and "
                                                        f'and must_exist={self.must_exist}. '
                                                        'Cannot instantiate the SQLite DatabaseManager.')

            self.conn_str = fr'sqlite:///{self.file}'

    def _create_engine(self) -> None:
        r"""Create the database engine and assign it to `self.engine`.

        Parameters
        ----------
        config : dict
            Additional keyword arguments passed to the SQLAlchemy create_engine function.

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

    def __str__(self):
        r"""String representation of the object."""

        return f"SQLiteDb(file='{self.file}', must_exist={self.must_exist})"

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

    .. _cx_Oracle: https://oracle.github.io/python-cx_Oracle/

    Parameters
    ----------
    username : str
        The username of the database account.

    password : str
        The password of the database account.

    host : str
        The host name or server IP-address where the database is located.

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

    connect_args : dict or None, default None
        Additional arguments sent to the driver upon connection that further
        customizes the connection.

    engine_config : dict or None, default None
        Additional keyword arguments passed to the :func:`sqlalchemy.create_engine` function.

    **kwargs : dict
        Additional keyword arguments that are not used by :class:`OracleDb`.

    Other Parameters
    ----------------
    url : :class:`str` or :class:`sqlalchemy.engine.URL`
        A SQLAlchemy connection URL to use for creating the database engine.
        This parameter is set by the :meth:`OracleDb.from_url` alternative constructor method.

    engine : :class:`sqlalchemy.engine.Engine`
        A SQLAlchemy Engine to use as the database engine of :class:`OracleDb`.
        This parameter is set by the :meth:`OracleDb.from_engine` alternative constructor method.

    Raises
    ------
    pandemy.InvalidInputError
        If both `service` and `sid` are specified at the same time.

    pandemy.CreateConnectionURLError
        If the creation of the connection URL fails.

    pandemy.CreateEngineError
        If the creation of the database engine fails.

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

    .. _Oracle Cloud Autononmous Databases : https://cx-oracle.readthedocs.io/en/latest/user_guide/connection_handling.html#connecting-to-oracle-cloud-autononmous-databases

    Examples
    --------
    Create an instance of :class:`OracleDb` and connect using a service:

    >>> db = pandemy.OracleDb(
    ... username='Fred_the_Farmer',
    ... password='Penguins-sheep-are-not',
    ... host='fred.farmer.rs',
    ... port=1234,
    ... service_name='woollysheep'
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
    ... 'encoding': 'UTF-8',
    ... 'nencoding': 'UTF-8',
    ... 'mode': cx_Oracle.SYSDBA,
    ... 'events': True
    ... }
    >>> engine_config = {
    ... 'coerce_to_unicode': False,
    ... 'arraysize': 40,
    ... 'auto_convert_lobs': False
    ... }
    >>> db = pandemy.OracleDb(
    ... username='Fred_the_Farmer',
    ... password='Penguins-sheep-are-not',
    ... host='my_dsn_name',
    ... connect_args=connect_args,
    ... engine_config=engine_config
    ... )
    >>> db
    OracleDb(
        username='Fred_the_Farmer',
        password='***',
        host='my_dsn_name',
        port=None,
        service_name=None,
        sid=None,
        container=None,
        connect_args={'encoding': 'UTF-8', 'nencoding': 'UTF-8', 'mode': 2, 'events': True},
        engine_config={'coerce_to_unicode': False, 'arraysize': 40, 'auto_convert_lobs': False},
        url=oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name,
        engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name)
    )
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

    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 port: Optional[Union[int, str]] = None,
                 service_name: Optional[str] = None,
                 sid: Optional[str] = None,
                 connect_args: Optional[Dict[str, Any]] = None,
                 container: Optional[pandemy.SQLContainer] = None,
                 engine_config: Optional[Dict[str, Any]] = None,
                 url: Optional[Union[str, URL]] = None,
                 engine: Optional[Engine] = None,
                 **kwargs: dict) -> None:

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sid = sid
        self.container = container
        self.connect_args = connect_args if connect_args is not None else {}
        self.engine_config = engine_config if engine_config is not None else {}
        self.url = url
        self.engine = engine

        # Check that service_name and sid are not used together
        if service_name is not None and sid is not None:
            raise pandemy.InvalidInputError(
                        'Use either service_name or sid to connect to the database, not both! '
                        f'service={service_name!r}, sid={sid!r}',
                        data=(service_name, sid)
                    )

        if self.url is None:
            # Encode username and password to make special characters url compatible
            try:
                username = urllib.parse.quote_plus(username)
                password = urllib.parse.quote_plus(password)
            except UnicodeEncodeError as e:
                raise pandemy.CreateConnectionURLError(
                            f'Could not URL encode username or password: {e.args}',
                            data=e.args
                        ) from None

            # Build the connection URL
            try:
                self.url = URL.create(
                        drivername='oracle+cx_oracle',
                        username=username,
                        password=password,
                        host=host,
                        port=port,
                        database=sid,
                        query={'service_name': service_name} if service_name is not None else {}
                    )
            except Exception as e:
                raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None

        # Create the engine
        if self.engine is None:
            try:
                self.engine = create_engine(self.url, connect_args=self.connect_args, **self.engine_config)
            except Exception as e:
                raise pandemy.CreateEngineError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None
            else:
                logger.debug(f'Successfully created database engine from url: {self.url}.')

    @classmethod
    def from_url(cls,
                 url: Union[str, URL],
                 container: Optional[pandemy.SQLContainer] = None,
                 engine_config: Optional[Dict[str, Any]] = None) -> OracleDb:
        r"""Create an instance of :class:`OracleDb` from a SQLAlchemy :class:`URL <sqlalchemy.engine.URL>`.

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
            container=None,
            connect_args={},
            engine_config={},
            url=oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name,
            engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@my_dsn_name)
        )
        """

        try:
            url = make_url(url)
        except Exception as e:
            raise pandemy.CreateConnectionURLError(message=f'{type(e).__name__}: {e.args}', data=e.args) from None

        return cls(
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            sid=url.database,
            service_name=url.query.get('service_name'),
            container=container,
            engine_config=engine_config,
            url=url
        )

    @classmethod
    def from_engine(cls, engine: Engine, container: Optional[pandemy.SQLContainer] = None) -> OracleDb:
        r"""Create an instance of :class:`OracleDb` from a SQLAlchemy :class:`Engine <sqlalchemy.engine.Engine>`.

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
            container=None,
            connect_args={},
            engine_config={},
            url=oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears,
            engine=Engine(oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234/shears)
        )

        This is useful if you have special needs for creating the engine that cannot be accomplished
        with the default constructor of :class:`OracleDb`. See for instance the `cx_Oracle SessionPool`_
        example in the SQLAlchemy docs.

        .. _cx_Oracle SessionPool: https://docs.sqlalchemy.org/en/14/dialects/oracle.html#using-cx-oracle-sessionpool
        """

        return cls(
            username=engine.url.username,
            password=engine.url.password,
            host=engine.url.host,
            port=engine.url.port,
            sid=engine.url.database,
            service_name=engine.url.query.get('service_name'),
            container=container,
            url=engine.url,
            engine=engine
        )
