"""Tests for the SQLite DatabaseManager `SQLiteDb`.

Tests all methods of the DatabaseManager because it is easy to test with SQLite.
"""

# =================================================
# Imports
# =================================================

# Standard Library
from pathlib import Path

# Third Party
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

import pytest

from sqlalchemy.sql import text
import sqlalchemy

# Local
import pandemy
from .dependencies import PANDAS_VERSION

# =================================================
# Setup
# =================================================


class SQLiteSQLContainer(pandemy.SQLContainer):
    r"""A correctly defined pandemy.SQLContainer subclass"""
    my_query = 'SELECT * FROM MyTable;'


class SQLiteFakeSQLContainer:
    r"""
    SQLContainer class that does not inherit from `pandemy.SQLContainer`.
    This class is not a valid input to the container parameter of
    `pandemy.DatabaseManager`.
    """
    my_query = 'SELECT * FROM MyTable;'

# =================================================
# Tests
# =================================================


class TestInitSQLiteDb:
    r"""Test the initalization of the SQLite DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db_file : Path
        Path to a SQLite database that exists on disk.
    """

    def test_all_defaults(self):
        r"""Create an instance of SQLiteDb that lives in memory with all default values."""

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb()

        # Verify
        # ===========================================================
        assert db.file == ':memory:'
        assert db.must_exist is False
        assert db.container is None
        assert db.engine_config is None
        assert db.conn_str == r'sqlite://'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    def test_in_memory(self):
        r"""Create an instance of SQLiteDb that lives in memory."""

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=':memory:')

        # Verify
        # ===========================================================
        assert db.file == ':memory:'
        assert db.must_exist is False
        assert db.conn_str == r'sqlite://'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('file_as_str', [pytest.param(True, id='str'), pytest.param(False, id='Path')])
    def test_file_must_exist(self, file_as_str, sqlite_db_file):
        r"""Create an instance with a file supplied as a string and pathlib.Path object.

        The default option `must_exist` is set to True.
        The file exists on disk.

        Parameters
        ----------
        file_as_str : bool
            True if the file should be supplied as a string and False for pathlib.Path.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        if file_as_str:
            db = pandemy.SQLiteDb(file=str(sqlite_db_file), must_exist=True)
        else:
            db = pandemy.SQLiteDb(file=sqlite_db_file, must_exist=True)

        # Verify
        # ===========================================================
        assert db.file == sqlite_db_file
        assert db.must_exist is True
        assert db.conn_str == fr'sqlite:///{str(sqlite_db_file)}'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('file', [pytest.param('does not exist', id='str'),
                                      pytest.param(Path('does not exist'), id='Path')])
    def test_on_file_must_exist_file_does_not_exist(self, file):
        r"""Create an instance with a file supplied as a string and pathlib.Path object.

        The default option `must_exist` is set to True.
        The file does not exists on disk.

        pandemy.DatabaseFileNotFoundError is expected to be raised.

        Parameters
        ----------
        file : str or Path
            The file with the SQLite database.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.DatabaseFileNotFoundError):
            pandemy.SQLiteDb(file=file, must_exist=True)

        # Clean up - None
        # ===========================================================

    def test_on_file_with_SQLContainer(self):
        r"""Create an instance with a SQLContainer class.

        The option `must_exist` is set to False.
        The file does not exists on disk.
        """

        # Setup
        # ===========================================================
        must_exist = False
        file = 'mydb.db'

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=file, must_exist=must_exist, container=SQLiteSQLContainer)

        # Verify
        # ===========================================================
        assert db.file == Path(file)
        assert db.must_exist is must_exist
        assert db.container is SQLiteSQLContainer

        # Clean up - None
        # ===========================================================

    # file, must_exist, container, engine_config, error_msg
    input_test_bad_input = [
        pytest.param(42, False, None, None, 'Received: 42', id='file=42'),

        pytest.param('my_db.db', 'False', None, {'encoding': 'UTF-8'}, 'Received: False', id="must_exist='False'"),

        pytest.param('my_db.db', False,  [42], None, 'container must be a subclass of pandemy.SQLContainer',
                     id="container=[42]"),

        pytest.param(Path('my_db.db'), False,  SQLiteFakeSQLContainer, None,
                     'container must be a subclass of pandemy.SQLContainer', id="container=FakeSQLContainer"),

        pytest.param('my_db.db', False,  None, 42, 'engine_config must be a dict', id="engine_config=42"),
    ]

    @pytest.mark.raises
    @pytest.mark.parametrize('file, must_exist, container, engine_config, error_msg', input_test_bad_input)
    def test_bad_input_parameters(self, file, must_exist, container, engine_config, error_msg):
        r"""Test bad input parameters.

        pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        file : str or Path, default ':memory:'
            The file (with path) to the SQLite database.
            The default creates an in memory database.

        must_exist : bool, default True
            If True validate that file exists unless file = ':memory:'.
            If it does not exist FileNotFoundError is raised.
            If False the validation is omitted.

        container : pandemy.SQLContainer or None, default None
            A container of database statements that the SQLite DatabaseManager can use.

        engine_config : dict or None
            Additional keyword arguments passed to the SQLAlchemy create_engine function.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError, match=error_msg):
            pandemy.SQLiteDb(file=file, must_exist=must_exist, container=container, engine_config=engine_config)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_parameter_to_create_engine(self):
        r"""Test to supply an invalid parameter to the SQLAlchemy create_engine function.

        pandemy.CreateEngineError is expected to be raised.

        Also supply a keyword argument that is not used for anything.
        It should not affect the initialization.
        """

        # Setup
        # ===========================================================
        error_msg = 'invalid_param'
        engine_config = {'invalid_param': True}

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.CreateEngineError, match=error_msg):
            pandemy.SQLiteDb(file='my_db.db', must_exist=False, container=None,
                             engine_config=engine_config, kwarg='kwarg')

        # Clean up - None
        # ===========================================================


class TestExecuteMethod:
    r"""Test the `execute` method of the SQLite DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db : pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_owner : pd.DataFrame
        The owner table of the test database.
    """

    # The query for test_select_all_owners
    select_all_owners = """SELECT OwnerId, OwnerName, BirthDate FROM Owner;"""

    @pytest.mark.parametrize('query', [pytest.param(select_all_owners, id='query: str'),
                                       pytest.param(text(select_all_owners), id='query: sqlalchemy TextClause')])
    def test_select_all_owners(self, query, sqlite_db, df_owner):
        r"""Test to execute a SELECT query.

        Query all rows from the Owner table.

        Parameters
        ----------
        query : str or text
            The SQL query to execute.
        """

        # Setup
        # ===========================================================
        with sqlite_db.engine.connect() as conn:
            # Exercise
            # ===========================================================
            result = sqlite_db.execute(sql=query, conn=conn)

            # Verify
            # ===========================================================
            for idx, row in enumerate(result):
                assert row.OwnerId == df_owner.index[idx]
                assert row.OwnerName == df_owner.loc[row.OwnerId, 'OwnerName']
                assert row.BirthDate == df_owner.loc[row.OwnerId, 'BirthDate'].strftime(r'%Y-%m-%d')

        # Clean up - None
        # ===========================================================

    # The query for test_select_owner_by_id
    select_owner_by_id = """SELECT OwnerId, OwnerName
                                   FROM Owner
                                   WHERE OwnerId = :id;
                         """

    # query, id, owner_exp
    input_test_select_owner_by_id = [pytest.param(select_owner_by_id, 1,
                                                  id='query: str, id=1'),
                                     pytest.param(text(select_owner_by_id), 2,
                                                  id='query: sqlalchemy TextClause, id=2')]

    @pytest.mark.parametrize('query, owner_id', input_test_select_owner_by_id)
    def test_select_owner_by_id(self, query, owner_id, sqlite_db, df_owner):
        r"""Test to execute a SELECT query with a query parameter.

        Parameters
        ----------
        query : str or sqlalchemy.sql.elements.TextClause
            The SQL query to execute.

        owner_id : int
            The parameter representing OwnerId in `query`.
        """

        # Setup
        # ===========================================================
        with sqlite_db.engine.connect() as conn:
            # Exercise
            # ===========================================================
            result = sqlite_db.execute(sql=query, conn=conn, params={'id': owner_id})

            # Verify
            # ===========================================================
            for row in result:
                assert row.OwnerId == owner_id
                assert row.OwnerName == df_owner.loc[owner_id, 'OwnerName']

        # Clean up - None
        # ===========================================================

    def test_select_owner_by_2_params(self, sqlite_db, df_owner):
        r"""Test to execute a SELECT query with 2 query parameters."""

        # Setup
        # ===========================================================
        query = text("""SELECT OwnerId, OwnerName, BirthDate
                        FROM Owner
                        WHERE OwnerName = :name OR
                              DATE(BirthDate) > DATE(:bdate)
                        ORDER BY OwnerName ASC;
                        """)

        df_exp_result = df_owner.loc[[3, 1], :]

        with sqlite_db.engine.connect() as conn:
            # Exercise
            # ===========================================================
            result = sqlite_db.execute(sql=query, conn=conn, params={'name': 'John', 'bdate': '1941-12-07'})

            # Verify
            # ===========================================================
            for idx, row in enumerate(result):
                assert row.OwnerId == df_exp_result.index[idx]
                assert row.OwnerName == df_exp_result.loc[row.OwnerId, 'OwnerName']
                assert row.BirthDate == df_exp_result.loc[row.OwnerId, 'BirthDate'].strftime(r'%Y-%m-%d')

        # Clean up - None
        # ===========================================================

    input_test_insert_owner = [
        pytest.param([{'id': 1, 'name': 'Lumbridge Luke', 'bdate': '2021-07-07'}], id='1 Owner'),

        pytest.param([{'id': 1, 'name': 'Lumbridge Luke', 'bdate': '2021-07-07'},
                      {'id': 2, 'name': 'Falador Fluke', 'bdate': '1987-07-21'}], id='2 Owners'),
    ]

    @pytest.mark.parametrize('params', input_test_insert_owner)
    def test_insert_into_owner(self, params, sqlite_db_empty):
        r"""Test to insert new owner(s) into the Owner table of the empty test database.

        Parameters
        ----------
        params : list of dict
            The parameters to pass to the insert statement.
        """

        # Setup
        # ===========================================================
        statement = text("""INSERT INTO Owner (OwnerId, OwnerName, BirthDate)
                            VALUES (:id, :name, :bdate);
                         """)

        # The query to read back the inserted owners
        query_exp = """SELECT OwnerId, OwnerName, BirthDate FROM Owner;"""

        with sqlite_db_empty.engine.connect() as conn:
            # Exercise
            # ===========================================================
            sqlite_db_empty.execute(sql=statement, conn=conn, params=params)

            # Verify
            # ===========================================================
            result = sqlite_db_empty.execute(sql=query_exp, conn=conn)

            for idx, row in enumerate(result):
                assert row.OwnerId == params[idx]['id']
                assert row.OwnerName == params[idx]['name']
                assert row.BirthDate == params[idx]['bdate']

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_select_syntax(self, sqlite_db):
        r"""Execute a SELECT query with invalid syntax.

        No query parameters are supplied. It should raise pandemy.ExecuteStatementError.
        """

        # Setup
        # ===========================================================
        query = 'SELE * FROM Owner'

        with sqlite_db.engine.connect() as conn:
            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.ExecuteStatementError):
                sqlite_db.execute(sql=query, conn=conn)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_query_param(self, sqlite_db):
        r"""
        Execute a SELECT query with a parameter (:id) and the name of the supplied
        parameter (:di) to the query does not match the parameter name in the query.

        It should raise pandemy.ExecuteStatementError.
        """

        # Setup
        # ===========================================================
        with sqlite_db.engine.connect() as conn:
            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.ExecuteStatementError):
                sqlite_db.execute(sql=self.select_owner_by_id, conn=conn, params={'di': 1})

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_sql_param(self, sqlite_db):
        r"""Supply and invalid type to the `sql` parameter.

        It should raise pandemy.InvalidInputError.
        """

        # Setup
        # ===========================================================
        with sqlite_db.engine.connect() as conn:
            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidInputError, match='list'):
                sqlite_db.execute(sql=['Invalid query'], conn=conn, params={'di': 1})

        # Clean up - None
        # ===========================================================


class TestIsValidTableName:
    r"""Test the `_is_valid_table_name` method of the SQLiteDb DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.
    """

    @pytest.mark.parametrize('table', [pytest.param('Customer', id='Customer'),
                                       pytest.param('1', id='1'),
                                       pytest.param('', id='empty string'),
                                       pytest.param('DELETE', id='DELETE'),
                                       pytest.param('"DROP"', id='DROP'),
                                       pytest.param('""DELETEFROMTABLE""', id='""DELETEFROMTABLE""')])
    def test_is_valid_table_name_valid_table_names(self, table, sqlite_db_empty):
        r"""Test that valid table names can pass the validation.

        The `_is_valid_table_name method` checks that the table name consists
        of a single word. If the table name is valid the method returns None
        and no exception should be raised.

        Parameters
        ----------
        table : str
            The name of the table.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        result = sqlite_db_empty._is_valid_table_name(table=table)

        # Verify
        # ===========================================================
        assert result is None

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('table, spaces', [pytest.param('Customer DELETE', '1',
                                                            id='2 words, 1 space'),

                                               pytest.param(' Customer  DELETE', '3',
                                                            id='2 words, 3 spaces'),

                                               pytest.param('"DROP TABLE Customer"', '2',
                                                            id='3 words, 2 spaces'),

                                               pytest.param(';""DELETE FROM TABLE Customer;"', '3',
                                                            id='4 words, 3 spaces')])
    def test_is_valid_table_name_invalid_table_names(self, table, spaces, sqlite_db_empty):
        r"""Test that invalid table names can be detected correctly.

        The `_is_valid_table_name method` checks that the table name consists
        of a single word.

        pandemy.InvalidTableNameError is expected to be raised
        if the table name is invalid.

        Parameters
        ----------
        table : str
            The name of the table.

        spaces : str
            The number of space characters in `table`.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidTableNameError) as exc_info:
            sqlite_db_empty._is_valid_table_name(table=table)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.InvalidTableNameError
        assert table in exc_info.value.args[0]
        assert spaces in exc_info.value.args[0]
        assert table == exc_info.value.data

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('table', [pytest.param(1, id='int'),
                                       pytest.param(3.14, id='float'),
                                       pytest.param([1, '1'], id='list'),
                                       pytest.param({'table': 'name'}, id='dict')])
    def test_is_valid_table_name_invalid_input(self, table, sqlite_db_empty):
        r"""Test invalid input to the `table` parameter.

        If `table` is not a string pandemy.InvalidInputError should be raised.

        Parameters
        ----------
        table : str
            The name of the table.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            sqlite_db_empty._is_valid_table_name(table=table)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.InvalidInputError
        assert str(table) in exc_info.value.args[0]
        assert table == exc_info.value.data

        # Clean up - None
        # ===========================================================


class TestDeleteAllRecordsFromTable:
    r"""Test the `delete_all_records_from_table` method of the SQLiteDb DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_customer : pd.DataFrame
        The Customer table of the test database.
    """

    def test_delete_all_records(self, sqlite_db_empty, df_customer):
        r"""Delete all records from the table Customer in the test database."""

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        df_exp_result = pd.DataFrame(columns=df_customer.columns)
        df_exp_result.index.name = df_customer.index.name

        with sqlite_db_empty.engine.begin() as conn:
            # Write data to the empty table
            df_customer.to_sql(name='Customer', con=conn, if_exists='append')

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.delete_all_records_from_table(table='Customer', conn=conn)

        # Verify
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            df_result = pd.read_sql(sql=query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        assert_frame_equal(df_result, df_exp_result, check_dtype=False, check_index_type=False)

    @pytest.mark.raises
    def test_delete_all_records_table_does_not_exist(self, sqlite_db_empty):
        r"""Try to delete all records from the table Custom that does not exist in the database.

        pandemy.DeleteFromTableError is expected to be raised.
        """

        # Setup
        # ===========================================================
        table = 'Custom'

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.DeleteFromTableError) as exc_info:
            with sqlite_db_empty.engine.begin() as conn:
                sqlite_db_empty.delete_all_records_from_table(table=table, conn=conn)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.DeleteFromTableError
        assert table in exc_info.value.args[0]
        assert table in exc_info.value.data[0]

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('table', [pytest.param('Customer DELETE', id='table name = 2 words'),
                                       pytest.param('"DROP TABLE Customer"', id='table name = 3 words'),
                                       pytest.param(';""DELETE FROM TABLE Customer;"', id='table name = 4 words')])
    def test_delete_all_records_invalid_table_name(self, table, sqlite_db_empty):
        r"""Try to delete all records from specified table when supplying and invalid table name.

        pandemy.InvalidTableNameError is expected to be raised.

        Parameters
        ----------
        table: str
            The name of the table to delete records from.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidTableNameError) as exc_info:
            with sqlite_db_empty.engine.begin() as conn:
                sqlite_db_empty.delete_all_records_from_table(table=table, conn=conn)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.InvalidTableNameError
        assert table in exc_info.value.args[0]
        assert table == exc_info.value.data

        # Clean up - None
        # ===========================================================


class TestSaveDfMethod:
    r"""Test the `save_df` method of the SQLiteDb DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db : pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_customer : pd.DataFrame
        The Customer table of the test database.
    """

    @pytest.mark.parametrize('chunksize', [pytest.param(None, id='chunksize=None'),
                                           pytest.param(2, id='chunksize=2')])
    def test_save_to_existing_empty_table(self, chunksize, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng empty table.

        Parameters
        ----------
        chunksize : int or None
            The number of rows in each batch to be written at a time.
            If None, all rows will be written at once.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn,
                                    if_exists='append', chunksize=chunksize)

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_save_to_new_table_with_schema(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to a new table in the database with a schema specified.

        The table Customer already exists as an empty table in the database. By saving the DataFrame
        to a temporary table (the temp schema) called Customer, while the parameter `if_exists` = 'fail',
        no exception should be raised since the tables called Customer exist in different schemas.

        SQLite supports the schemas 'temp', 'main' or the name of an attached database.

        See Also
        --------
        https://sqlite.org/lang_createtable.html
        """

        # Setup
        # ===========================================================
        schema = 'temp'
        query = f"""SELECT * FROM {schema}.Customer;"""

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn,
                                    schema=schema, if_exists='fail')

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_save_to_existing_non_empty_table_if_exists_replace(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng non empty table.

        The existing rows in the table are deleted before writing the DataFrame.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        with sqlite_db_empty.engine.begin() as conn:
            # Write data to the empty table
            df_customer.to_sql(name='Customer', con=conn, if_exists='append')

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='replace')

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

    @pytest.mark.raises
    def test_save_to_existing_table_if_exists_fail(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng table when `if_exists` = 'fail'.

        pandemy.TableExistsError is expected to be raised.
        """
        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.TableExistsError, match='Table Customer already exists!'):
                sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='fail')

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_save_to_existing_non_empty_table_if_exists_append(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng non empty table.

        The rows of the DataFrame are already present in the database table
        and inserting the rows will violate a UNIQUE constraint.
        pandemy.SaveDataFrameError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            # Write data to the empty table
            df_customer.to_sql(name='Customer', con=conn, if_exists='append')

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.SaveDataFrameError, match='Could not save DataFrame to table Customer'):
                sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='append')

        # Clean up - None
        # ===========================================================

    def test_index_False(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng empty table.

        The index column of the DataFrame is not written to the table.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        df_customer.reset_index(inplace=True)  # Convert the index CustomerId to a regular column

        with sqlite_db_empty.engine.begin() as conn:

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='append', index=False)

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, parse_dates=['BirthDate'])

            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

    # Input data to test_index_label
    # new_index_names, index_labels, index_type,
    input_test_index_label = [
        pytest.param('Index', 'CustomerId', 'Single', id='Single'),
        pytest.param(['Level_1', 'Level_2'], ['CustomerId', 'CustomerName'], 'Multi', id='Multi'),
    ]

    @pytest.mark.parametrize('new_index_names, index_labels, index_type', input_test_index_label)
    def test_index_label(self, new_index_names, index_labels, index_type,
                         sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an exisitng empty table.

        Supply custom name(s) for the index of the DataFrame.
        that will be used as the column names in the database.

        The index name(s) of the DataFrame do not match the column names
        of the table Customer in the Database.

        Test with both a single and multiindex DataFrame.

        Parameters
        ----------
        new_index_names : str or list of str
            The new index names to assign to the DataFrame to save to the database.

        index_labels : str or list of str
            The names to use for the index when saving the DataFrame to the database.

        index_type : str ('Single', 'Multi')
            The type of index of the DataFrame.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        if index_type == 'Single':
            df_customer.index.name = new_index_names
        elif index_type == 'Multi':
            df_customer.set_index(index_labels[1], inplace=True, append=True)
            df_customer.index.names = new_index_names

        with sqlite_db_empty.engine.begin() as conn:
            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='append',
                                    index=True, index_label=index_labels)

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, index_col=index_labels, parse_dates=['BirthDate'])

            # Change back to expected index name
            if index_type == 'Single':
                df_customer.index.name = 'CustomerId'
            elif index_type == 'Multi':
                df_customer.index.names = ['CustomerId', 'CustomerName']

            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

    @pytest.mark.raises
    @pytest.mark.parametrize('df', [pytest.param('df', id='str'),
                                    pytest.param([], id='list')])
    def test_invalid_input_df(self, df, sqlite_db_empty):
        r"""Supply an invalid argument to the `df` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidInputError):
                sqlite_db_empty.save_df(df=df, table='Customer', conn=conn, if_exists='append')

    @pytest.mark.raises
    @pytest.mark.parametrize('if_exists', [pytest.param('delete', id='str'),
                                           pytest.param([], id='list')])
    def test_invalid_input_if_exists(self, if_exists, sqlite_db_empty, df_customer):
        r"""Supply an invalid argument to the `if_exists` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidInputError):
                sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists=if_exists)

    @pytest.mark.raises
    @pytest.mark.parametrize('conn', [pytest.param('conn', id='str'),
                                      pytest.param([], id='list'),
                                      pytest.param(pandemy.SQLContainer, id='class')])
    def test_invalid_input_conn(self, conn, sqlite_db_empty, df_customer):
        r"""Supply an invalid argument to the `conn` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin():

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidInputError):
                sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn)

    @pytest.mark.raises
    @pytest.mark.parametrize('table', [pytest.param('delete from', id='delete from'),
                                       pytest.param('customer name', id='customer name'),
                                       pytest.param('DROP TABLE', id='DROP TABLE')])
    def test_invalid_input_table(self, table, sqlite_db_empty, df_customer):
        r"""Supply an invalid argument to the `table` parameter.

        pandemy.InvalidTableNameError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidTableNameError):
                sqlite_db_empty.save_df(df=df_customer, table=table, conn=conn)


class TestLoadTableMethod:
    r"""Test the `load_table` method of the SQLite DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db : pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_item_traded_in_store : pd.DataFrame
        The ItemTradedInStore table of the test database.
    """

    input_load_table_by_name = [pytest.param(['TransactionTimestamp'], id='parse_dates=list'),

                                pytest.param({'TransactionTimestamp': r'%Y-%m-%d %H:%M:%S'},
                                             id='parse_dates=dict: format string'),

                                pytest.param({'TransactionTimestamp': {'format': r'%Y-%m-%d %H:%M:%S'}},
                                             id='parse_dates=dict: to_datetime args')
                                ]
    @pytest.mark.parametrize('parse_dates', input_load_table_by_name)
    def test_load_table_by_name_all_cols(self, parse_dates, sqlite_db, df_item_traded_in_store):
        r"""Load the whole table ItemTradedInStore by selecting it by name.

        Test different types of configuration of the `parse_dates` parameter.

        Parameters
        ----------
        parse_dates : list or dict
            How to parse datetime columns.
        """

        # Setup
        # ===========================================================
        df_item_traded_in_store.reset_index(inplace=True)  # Convert the index TransactionId to a regular column

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, parse_dates=parse_dates)

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('columns, index_col', [
        pytest.param(['StoreId', 'Quantity', 'TradePricePerItem'], 'StoreId',
                     id='columns list, single index'),

        pytest.param(('StoreId', 'Quantity', 'TradePricePerItem'), ['StoreId', 'Quantity'],
                     id='columns tuple, multiindex')
    ])
    def test_load_table_by_name_selected_cols_and_index_col(self, columns, index_col, sqlite_db,
                                                            df_item_traded_in_store):
        r"""Load all rows and selected columns from table ItemTradedInStore.

        Select the table by name and set an index column of the DataFrame.

        Parameters
        ----------
        columns : sequence of str
            The columns to select from the table.

        index_col : str or sequence of str
            The column(s) to set as the index of the DataFrame.
        """

        # Setup
        # ===========================================================
        df_item_traded_in_store.reset_index(inplace=True)
        df_item_traded_in_store = df_item_traded_in_store.loc[:, columns]
        df_item_traded_in_store.set_index(index_col, inplace=True)

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, columns=columns, index_col=index_col)

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('query', [
        pytest.param("""SELECT * FROM ItemTradedInStore;""", id='str'),
        pytest.param(text("""SELECT * FROM ItemTradedInStore;"""), id='TextClause')
    ])
    def test_read_query_no_params(self, query, sqlite_db, df_item_traded_in_store):
        r"""Load all rows from table ItemTradedInStore through a SQL query.

        The query has no parameters.

        Parameters
        ----------
        query : str or sqlalchemy.sql.elements.TextClause
            The query to execute.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql=query, conn=conn, index_col='TransactionId',
                                             parse_dates='TransactionTimestamp')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('query, params, as_textclause', [
        pytest.param("""SELECT * FROM ItemTradedInStore
                        WHERE TotalTradePrice > :TotalTradePrice AND
                              CustomerBuys = :CustomerBuys;""",
                     {'TotalTradePrice': 17000, 'CustomerBuys': 0}, False, id='str'),

        pytest.param("""SELECT * FROM ItemTradedInStore
                        WHERE TradePricePerItem > :TradePricePerItem AND
                              StoreId = :StoreId;""",
                     {'TradePricePerItem': 200, 'StoreId': 2}, True, id='TextClause'),
    ])
    def test_read_query_with_params(self, query, params, as_textclause, sqlite_db, df_item_traded_in_store):
        r"""Load rows and columns specified by the SQL query.

        The query has parameters.

        Parameters
        ----------
        query : str or sqlalchemy.sql.elements.TextClause
            The query to execute.

        params : dict
            The query parameters.

        as_textclause : bool
            True if the query should be of type sqlalchemy.sql.elements.TextClause and
            False for type str.
        """

        # Setup
        # ===========================================================
        if as_textclause:
            query = text(query)

        # Build the expected result
        columns = list(params.keys())  # The columns to filter by
        values = list(params.values())  # The values of the columns
        df_query_str = f"{columns[0]} > {values[0]} & {columns[1]} == {values[1]}"
        df_item_traded_in_store.query(df_query_str, inplace=True)

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql=query, conn=conn, params=params, index_col='TransactionId',
                                             parse_dates='TransactionTimestamp')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_load_table_set_dtypes(self, sqlite_db, df_item_traded_in_store):
        r"""Load table ItemTradedInStore by name and specify the dtypes of the columns."""

        # Setup
        # ===========================================================
        dtypes = {
            'TransactionId': 'int8',
            'StoreId': 'int8',
            'ItemId': 'int8',
            'CustomerId': 'int8',
            'CustomerBuys': 'int8',
            'Quantity': 'int16',
            'TradePricePerItem': 'float64',
            'TotalTradePrice': 'float64'
        }

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                             dtypes=dtypes, parse_dates='TransactionTimestamp')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=True, check_index_type=True)

        # Clean up - None
        # ===========================================================

    def test_load_table_in_chunksize(self, sqlite_db, df_item_traded_in_store):
        r"""Load table ItemTradedInStore by name and specify the `chunksize` parameter.

        A generator yielding DataFrames each with the number of rows specified in
        the `chunksize` parameter is expected to be returned.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_gen = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                          chunksize=2, parse_dates='TransactionTimestamp')

            # Verify
            # ===========================================================
            for df in df_gen:
                df_exp = df_item_traded_in_store.loc[df.index, :]
                assert_frame_equal(df, df_exp, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_load_table_that_is_empty(self, sqlite_db_empty, df_item_traded_in_store):
        r"""Try to load table ItemTradedInStore that is empty.

        An empty DataFrame is expected to be returned.
        """

        # Setup
        # ===========================================================
        df_exp = pd.DataFrame(columns=df_item_traded_in_store.columns)
        df_exp.index.name = df_item_traded_in_store.index.name

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            df_result = sqlite_db_empty.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                                   parse_dates='TransactionTimestamp')

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_exp, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_load_empty_table_in_chunksize(self, sqlite_db_empty):
        r"""Load table ItemTradedInStore by name and specify `chunksize` parameter.

        The table ItemTradedInStore is empty.

        A generator yielding DataFrames each with the number of rows specified in
        the `chunksize` parameter is expected to be returned. If the query returns no
        rows an empty DataFrame should be yielded from the generator in pandas >= 1.3.0
        and in earlier versions StopIteration is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            df_gen = sqlite_db_empty.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                                chunksize=2, parse_dates='TransactionTimestamp')

            # Verify
            # ===========================================================
            if PANDAS_VERSION >= (1, 3, 0):
                df = next(df_gen)
                assert df.empty is True

            with pytest.raises(StopIteration):
                next(df_gen)

        # Clean up - None
        # ===========================================================

    def test_load_table_localize_timezone(self, sqlite_db, df_item_traded_in_store):
        r"""
        Load table ItemTradedInStore and localize datetime column TransactionTimestamp
        to CET timezone.
        """

        # Setup
        # ===========================================================
        datetime_col = 'TransactionTimestamp'
        tz = 'CET'
        df_item_traded_in_store.loc[:, datetime_col] = df_item_traded_in_store[datetime_col].dt.tz_localize(tz)

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                             parse_dates='TransactionTimestamp', localize_tz=tz)

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_load_table_convert_timezone(self, sqlite_db, df_item_traded_in_store):
        r"""
        Load table ItemTradedInStore and convert datetime column TransactionTimestamp
        from localized timezone CET to EET timezone.
        """

        # Setup
        # ===========================================================
        datetime_col = 'TransactionTimestamp'
        localize_tz = 'CET'
        target_tz = 'EET'
        df_item_traded_in_store.loc[:, datetime_col] = df_item_traded_in_store[datetime_col].dt.tz_localize(localize_tz)
        df_item_traded_in_store.loc[:, datetime_col] = df_item_traded_in_store[datetime_col].dt.tz_convert(target_tz)

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            df_result = sqlite_db.load_table(sql='ItemTradedInStore', conn=conn, index_col='TransactionId',
                                             parse_dates='TransactionTimestamp', localize_tz=localize_tz,
                                             target_tz=target_tz)

        # Verify
        # ===========================================================
        assert_frame_equal(df_result, df_item_traded_in_store, check_dtype=False, check_index_type=False)
        assert_series_equal(df_result[datetime_col], df_item_traded_in_store[datetime_col],
                            check_dtype=True, check_index_type=False)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_load_table_that_does_not_exist(self, sqlite_db):
        r"""Try to load table TradedInStoreItems that does not exist in the test database.

        pandemy.LoadTableError is expected to be raised.
        """

        # Setup
        # ===========================================================
        sql = 'TradedInStoreItems'

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            with pytest.raises(pandemy.LoadTableError) as exc_info:
                sqlite_db.load_table(sql=sql, conn=conn, index_col='TransactionId',
                                     parse_dates='TransactionTimestamp')

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.LoadTableError
            assert sql in exc_info.value.args[0]
            assert sql == exc_info.value.data[1]

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_keys_in_dtypes_does_not_match_column_in_table(self, sqlite_db):
        r"""Try to load table ItemTradedInStore an convert data types.

        Supply column names in the `dtypes` parameter that does not exist in the table.

        pandemy.DataTypeConversionError is expected to be raised.
        """

        # Setup
        # ===========================================================
        sql = 'ItemTradedInStore'

        dtypes = {'StoreId': pd.UInt8Dtype(),
                  'ItemId': pd.UInt8Dtype(),
                  'Qty': pd.Int16Dtype(),  # Qty is not a valid column name
                  'TradePrice': pd.Int64Dtype()  # TradePrice is not a valid column name
                  }

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            with pytest.raises(pandemy.DataTypeConversionError) as exc_info:
                sqlite_db.load_table(sql=sql, conn=conn, index_col='TransactionId',
                                     parse_dates='TransactionTimestamp', dtypes=dtypes)

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.DataTypeConversionError
            assert 'Qty' in exc_info.value.args[0]
            assert 'TradePrice' in exc_info.value.args[0]
            assert dtypes == exc_info.value.data[1]
            assert 'Qty, TradePrice' == exc_info.value.data[2]

        # Clean up - None
        # ===========================================================

    @pytest.mark.skipif(PANDAS_VERSION >= (1, 3, 0),
                        reason=('Data type conversion from [IntegerDtype] to [datetime64] '
                                'does not raise an exception in pandas >= 1.3.0 '
                                f'(pandas version used: {pd.__version__})'))
    @pytest.mark.raises
    def test_dtypes_cannot_convert_dtype(self, sqlite_db):
        r"""Try to load table ItemTradedInStore an convert data types.

        The data type conversion cannot be performed for desired data types.
        TransactionTimestamp [datetime64[ns]] cannot be converted to [IntegerDtype]

        pandemy.DataTypeConversionError is expected to be raised.
        """

        # Setup
        # ===========================================================
        sql = 'ItemTradedInStore'

        dtypes = {'StoreId': pd.UInt8Dtype(),
                  'ItemId': pd.UInt8Dtype(),
                  'TransactionTimestamp': pd.Int64Dtype()
                  }

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            with pytest.raises(pandemy.DataTypeConversionError) as exc_info:
                sqlite_db.load_table(sql=sql, conn=conn, index_col='TransactionId',
                                     parse_dates=['TransactionTimestamp'], dtypes=dtypes)

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.DataTypeConversionError
            assert dtypes == exc_info.value.data[1]

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('params', [pytest.param({'ItemId': 1}, id='Invalid value'),
                                        pytest.param((44, 33), id='Invalid type')])
    def test_invalid_parameter_value_params(self, params, sqlite_db):
        r"""
        Try to load table ItemTradedInStore and specify an invalid value or type for
        the `params` parameter.

        pandemy.LoadTableError is expected to be raised.
        """

        # Setup
        # ===========================================================
        sql = """SELECT * FROM ItemTradedInStore
                    WHERE ItemId = :StoreId"""

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            with pytest.raises(pandemy.LoadTableError) as exc_info:
                sqlite_db.load_table(sql=sql, conn=conn, params=params, index_col='TransactionId',
                                     parse_dates='TransactionTimestamp')

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.LoadTableError
            assert sql in exc_info.value.args[0]
            assert sql in exc_info.value.data[1]
            assert params == exc_info.value.data[2]

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('index_col', [pytest.param('Transaction', id='Single index, str'),

                                           pytest.param(1, id='Single index, int'),

                                           pytest.param(['Transaction', 'Action', 'Id'],
                                                        id='Multiindex'),

                                           pytest.param(('Transaction', 'TransactionId'),
                                                        id='Multiindex, 1 valid column'),

                                           pytest.param({'Transaction': 'Id'}, id='dict')])
    def test_set_index_error(self, index_col, sqlite_db):
        r"""Try to load table ItemTradedInStore and set column(s) as the index.

        One or all columns specified in `index_col` do not exist in the loaded DataFrame.
        pandemy.SetIndexError is expected to be raised.

        Parameters
        ----------
        index_col: str, int or sequence of str, int
            The columns(s) to set as the index of the DataFrame.
        """

        # Setup
        # ===========================================================
        sql = 'ItemTradedInStore'

        # Exercise
        # ===========================================================
        with sqlite_db.engine.begin() as conn:
            with pytest.raises(pandemy.SetIndexError) as exc_info:
                sqlite_db.load_table(sql=sql, conn=conn, index_col=index_col,
                                     parse_dates='TransactionTimestamp')

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.SetIndexError
            assert str(index_col) in exc_info.value.args[0]
            assert index_col == exc_info.value.data

        # Clean up - None
        # ===========================================================


class TestManageForeignKeysMethod:
    r"""Test the `manage_foreign_keys` method of the SQLite DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db : pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty : pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_store : pd.DataFrame
        The Store table in test database.

    df_item_traded_in_store : pd.DataFrame
        The ItemTradedInStore table of the test database.
    """

    def test_foreign_key_constraint_triggered(self, sqlite_db_empty, df_store):
        r"""Write data to a table with a foreign key constraint.

        The foreign key constraint check is activated in the SQLite database.
        The data written should violate the foreign constraint.

        sqlalchemy.exc.IntegrityError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.manage_foreign_keys(conn=conn, action='ON')

            with pytest.raises(sqlalchemy.exc.IntegrityError) as exc_info:
                df_store.to_sql(name='Store', con=conn, if_exists='append')

            # Verify
            # ===========================================================
            assert exc_info.type is sqlalchemy.exc.IntegrityError
            assert 'FOREIGN KEY' in exc_info.value.args[0]

        # Clean up - None
        # ===========================================================

    def test_foreign_key_constraint_not_enabled(self, sqlite_db_empty, df_store):
        r"""Write data to a table with a foreign key constraint.

        The foreign key constraint check is not activated.
        The data written should violate the foreign constraint.

        The data should be saved ok to the table.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            df_store.to_sql(name='Store', con=conn, if_exists='append')
            df_result = pd.read_sql(sql='Store', con=conn, index_col='StoreId')

        # Verify
        # ===========================================================

        # OwnerId column of df_result is loaded as object dtype
        # Cannot compare object to pd.Uint16Dtype even though check_dtype=False
        # Cannot convert object to pd.Unit16Dtype, but np.float64 works
        df_result = df_result.astype({'OwnerId': np.float64})

        assert_frame_equal(df_result, df_store, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize('action', [pytest.param('on', id='on'),
                                        pytest.param('off', id='off'),
                                        pytest.param('f', id='f'),
                                        pytest.param(['ON', 'OFF'], id="['ON', 'OFF']")])
    def test_invalid_value_action(self, action, sqlite_db_empty):
        r"""Supply invalid values to the `action` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidInputError) as exc_info:
                sqlite_db_empty.manage_foreign_keys(conn=conn, action=action)

            # Verify
            # ===========================================================
            assert exc_info.type is pandemy.InvalidInputError
            assert str(action) in exc_info.value.args[0]
            assert action == exc_info.value.data[0]

        # Clean up - None
        # ===========================================================


class TestStrAndReprMethods:
    r"""Test the `__str__` and `__repr__` methods of the SQLite DatabaseManager `SQLiteDb`.

    Parameters
    ----------
    file : str or pathlib.Path, default ':memory:'
        The file (with path) to the SQLite database.
        The default creates an in memory database.

    must_exist : bool, default False
        If True validate that `file` exists unless `file` = ':memory:'.
        If it does not exist :exc:`pandemy.DatabaseFileNotFoundError` is raised.
        If `False` the validation is omitted.

    container : pandemy.SQLContainer or None, default None
        A container of database statements that the SQLite DatabaseManager can use.
    """

    @pytest.mark.parametrize(
        'file, must_exist',
        (
            pytest.param(':memory:', True, id="file=':memory'"),
            pytest.param('my_db_file.db', False, id='file=str'),
            pytest.param(Path('my_db_file.db'), False, id='file=Path'),
        )
    )
    def test__str__(self, file, must_exist):
        r"""Test the output of the `__str__` method."""

        # Setup
        # ===========================================================
        db = pandemy.SQLiteDb(file=file, must_exist=must_exist)

        exp_result = f"SQLiteDb(file='{file}', must_exist={must_exist})"

        # Exercise
        # ===========================================================
        result = str(db)

        # Verify
        # ===========================================================
        assert result == exp_result

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'file, must_exist, container',
        (
            pytest.param(
                ':memory:', True, None,
                id="file=':memory', no container"
            ),

            pytest.param(
                Path('my_db_file.db'), False, SQLiteSQLContainer,
                id='file=Path, with container'
            ),
        )
    )
    def test__repr__(self, file, must_exist, container):
        r"""Test the output of the `__repr__` method."""

        # Setup
        # ===========================================================
        engine_config = None

        db = pandemy.SQLiteDb(file=file, must_exist=must_exist, container=container, engine_config=engine_config)

        exp_result = f"""SQLiteDb(
    file={file!r},
    must_exist={must_exist!r},
    container={container!r},
    engine_config={engine_config!r},
    conn_str={db.conn_str!r},
    engine={db.engine!r}
)"""

        # Exercise
        # ===========================================================
        result = repr(db)

        # Verify
        # ===========================================================
        assert result == exp_result

        # Clean up - None
        # ===========================================================
