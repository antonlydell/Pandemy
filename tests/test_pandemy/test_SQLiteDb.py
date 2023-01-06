r"""Tests for the SQLite DatabaseManager `SQLiteDb`."""

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
from sqlalchemy.engine import create_engine, make_url
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
    r"""Test the initialization of the SQLite DatabaseManager `SQLiteDb`.

    Fixtures
    --------
    sqlite_db_file : Path
        Path to a SQLite database that exists on disk.
    """

    def test_all_defaults(self):
        r"""Test to initialize a SQLiteDb with all default values, which creates an in-memory database."""

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
        assert db.driver == 'sqlite3'
        assert str(db.url) == r'sqlite:///:memory:'
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('file_as_str', (pytest.param(True, id='str'), pytest.param(False, id='Path')))
    def test_file_must_exist(self, file_as_str, sqlite_db_file):
        r"""Test the `must_exist` parameter with a file supplied as a string and pathlib.Path object.

        `must_exist` is set to True. The file exists on disk and no exception should be raised.

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
        assert db.container is None
        assert db.driver == 'sqlite3'
        assert str(db.url) == fr'sqlite:///{str(sqlite_db_file)}'
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'file', 
        (
            pytest.param('does not exist', id='str'),
            pytest.param(Path('does not exist'), id='Path')
        )
    )
    def test_file_must_exist_and_file_does_not_exist(self, file):
        r"""Test the `must_exist` parameter with a file supplied as a string and pathlib.Path object.

        `must_exist` is set to True. The file does not exists on disk.

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

    def test_relative_file_path(self, sqlite_relative_db_path):
        r"""Test interacting with a SQLite database using a relative path to the database file.

        A CREATE TABLE statement is executed to verify that the connection URL is valid.

        A relative path uses three slashes in the connection URL: sqlite:///file

        Fixtures
        --------
        sqlite_relative_db_path : Tuple[Union[str, Path], str]
            A relative path to or a filename of a SQLite database and the expected SQLAlchemy connection URL.
        """

        # Setup
        # ===========================================================
        db_file, url_exp = sqlite_relative_db_path

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=db_file)

        # Test interacting with the database
        with db.engine.begin() as conn:
            conn.execute('CREATE TABLE Item(ItemId INTEGER, ItemName TEXT)')

        # Verify
        # ===========================================================
        assert str(db.url) == url_exp

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'file_as_str, driver, base_url_exp',
        (
            pytest.param(True, 'sqlite3', 'sqlite:///', id="str, driver='sqlite3'"), 
            pytest.param(True, 'pysqlite', 'sqlite+pysqlite:///', id="str, driver='pysqlite'"), 
            pytest.param(False, 'sqlite3', 'sqlite:///', id="Path, driver='sqlite3'"), 
            pytest.param(False, 'pysqlite', 'sqlite+pysqlite:///', id="Path, driver='pysqlite'"), 
        )
    )
    def test_absolute_file_path(self, file_as_str, driver, base_url_exp, tmp_path):
        r"""Test interacting with a SQLite database using an absolute path to the database file.

        A CREATE TABLE statement is executed to verify that the connection URL is valid.

        The connection URL of an absolute path starts with a slash (after the three slashes):
        
        sqlite+driver:////path/to/db_file

        or a drive letter on Windows:

        sqlite+driver:///C:\path\to\db_file

        Parameters
        ----------
        file_as_str : bool
            True if the file should be supplied as a string and False for pathlib.Path.
        
        driver : str 
            The database driver to use.

        base_url_exp : str
            The expected base of the generated SQLAlchemy connection URL.
        
        Fixtures
        --------
        tmp_path : pathlib.Path
            A temporary folder.
        """

        # Setup
        # ===========================================================
        file = tmp_path / 'Runescape_absolute.db'
        url_exp = f'{base_url_exp}{file}'
        file = str(file) if file_as_str else file

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=file, driver=driver)

        # Test interacting with the database
        with db.engine.begin() as conn:
            conn.execute('CREATE TABLE Item(ItemId INTEGER, ItemName TEXT)')

        # Verify
        # ===========================================================
        assert str(db.url) == url_exp
        assert db.driver == driver

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

    @pytest.mark.parametrize(
        'url, file_exp, driver_exp',
        (
            pytest.param(
                'sqlite:///Runescape_does_not_exist.db',
                'Runescape_does_not_exist.db',
                'sqlite3',
                id='sqlite:///Runescape_does_not_exist.db'
            ),
            pytest.param(
                make_url('sqlite://'),
                None,
                'sqlite3',
                id='make_url(sqlite://)'
            ),
            pytest.param(
                'sqlite+pysqlite:///:memory:',
                ':memory:',
                'pysqlite',
                id='sqlite+pysqlite:///:memory:'
            ),
        )
    )
    def test_connect_with_url(self, url, file_exp, driver_exp):
        r"""Test to connect to a SQLite database using the `url` parameter.

        If the `url` parameter is specified it should override the values of the
        parameters `file`, `must_exist` and `driver`.

        Parameters
        ----------
        url : str or sqlalchemy.engine.URL
            The SQLAlchemy connection URL.
        
        file_exp : str
            The expected value of the `file` attribute of SQLiteDb.

        driver_exp : str
            The expected value of the `driver` attribute of SQLiteDb.
        """

        # Setup
        # ===========================================================
        must_exist = True

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file='Runescape.db', must_exist=must_exist, driver='my_driver', url=url)

        # Verify
        # ===========================================================
        assert db.file == file_exp
        assert db.must_exist == must_exist
        assert db.driver == driver_exp
        assert str(db.url) == str(url)
        assert str(db.engine.url) == str(url)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'url, must_exist',
        (
            pytest.param('sqlite:///Runescape_does_not_exist.db', False, id='must_exist=False'),
            pytest.param('sqlite:///Runescape_does_not_exist.db', True, id='must_exist=True'),
        )
    )
    def test_connect_with_engine(self, url, must_exist):
        r"""Test to connect to a SQLite database using the `engine` parameter.

        If the `engine` parameter is specified it should override the values of the
        parameters `file` and `must_exist`.

        Parameters
        ----------
        url : str or sqlalchemy.engine.URL
            A SQLAlchemy connection URL.

        must_exist : bool
            The value of the `must_exist` parameter.
        """

        # Setup
        # ===========================================================
        engine = create_engine(url=url)

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file='Runescape.db', must_exist=must_exist, engine=engine)

        # Verify
        # ===========================================================
        assert db.engine is engine

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_specify_url_and_engine(self):
        r"""Specify the `url` and `engine` parameters at the same time.

        `SQLiteDb` cannot know if it should create an engine from
        the url or use the provided engine.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        url = 'sqlite://'
        engine = create_engine('sqlite:///Runescape.db')

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy.SQLiteDb(url=url, engine=engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'url',
        (
            pytest.param('sqlite://Runescape_does_not_exist.db', id='str'),
            pytest.param(make_url('sqlite://Runescape_does_not_exist.db'), id='URL'),
        )
    )
    def test_invalid_url(self, url):
        r"""Test to connect to a SQLite database using an invalid connection URL.

        The URL:s are using two backslashes for the relative path instead of three.
        pandemy.CreateEngineError is expected to be raised.

        Parameters
        ----------
        url : str or sqlalchemy.engine.URL
            The SQLAlchemy connection URL.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.CreateEngineError):
            pandemy.SQLiteDb(url=url)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'file, must_exist, error_msg',
        (
            pytest.param(42, False, 'Received: 42', id='file=42'),
            pytest.param('my_db.db', 'False', 'Received: False', id="must_exist='False'"),
        )
    )
    def test_bad_input_parameters(self, file, must_exist, error_msg):
        r"""Test bad input parameters.

        pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        file : str or Path, default ':memory:'
            The path (absolute or relative) to the SQLite database file.
            The default creates an in-memory database.

        must_exist : bool, default True
            If True validate that file exists unless file = ':memory:'.
            If it does not exist FileNotFoundError is raised.
            If False the validation is omitted.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError, match=error_msg):
            pandemy.SQLiteDb(file=file, must_exist=must_exist, kwarg='kwarg')

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

    def test_save_to_existing_non_empty_table_if_exists_replace(
        self, sqlite_db_empty, df_customer, df_customer_upsert
    ):
        r"""Save a DataFrame to an existing non-empty table using ``if_exists='replace'``.

        The existing rows of the table are expected to be deleted before saving the DataFrame.
        Since the table should not be dropped the create table statement of the Customer table
        should not change after executing `save_df`.
        """

        # Setup
        # ===========================================================
        df_original, _, _ = df_customer_upsert
        get_customer_data: str = """SELECT * FROM Customer ORDER BY CustomerId ASC"""
        get_customer_table_def: str = """SELECT sql FROM sqlite_master WHERE name = 'Customer'"""

        with sqlite_db_empty.engine.begin() as conn:
            customer_table_def_before: str = conn.execute(get_customer_table_def).scalar_one()

            # Write data to the empty table
            df_original.to_sql(name='Customer', con=conn, if_exists='append')

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='replace')

            # Verify
            # ===========================================================
            customer_table_def_after: str = conn.execute(get_customer_table_def).scalar_one()
            df_result = pd.read_sql(sql=get_customer_data, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

            assert customer_table_def_before == customer_table_def_after
            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

    def test_save_to_existing_non_empty_table_if_exists_drop_replace(
        self, sqlite_db_empty, df_customer, df_customer_upsert
    ):
        r"""Save a DataFrame to an existing non-empty table using ``if_exists='drop-replace'``.

        The existing table is expected to be dropped and recreated before saving the DataFrame.
        This is the default behavior of ``if_exists='replace'`` in the `pandas.DataFrame.to_sql()`
        method. Because the Customer table is dropped the recreated create table statement will not
        be the same as the original. Information about constraints e.g. PRIMARY KEY will be lost.
        """

        # Setup
        # ===========================================================
        df_original, _, _ = df_customer_upsert
        get_customer_data: str = """SELECT * FROM Customer ORDER BY CustomerId ASC"""
        get_customer_table_def: str = """SELECT sql FROM sqlite_master WHERE name = 'Customer'"""

        with sqlite_db_empty.engine.begin() as conn:
            customer_table_def_before: str = conn.execute(get_customer_table_def).scalar_one()

            # Write data to the empty table
            df_original.to_sql(name='Customer', con=conn, if_exists='append')

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn, if_exists='drop-replace')

            # Verify
            # ===========================================================
            customer_table_def_after: str = conn.execute(get_customer_table_def).scalar_one()
            df_result = pd.read_sql(sql=get_customer_data, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

            assert customer_table_def_before != customer_table_def_after
            assert_frame_equal(df_result, df_customer, check_dtype=False, check_index_type=False)

    @pytest.mark.raises
    def test_save_to_existing_table_if_exists_fail(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an existing table using ``if_exists='fail'``.

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
        r"""Save a DataFrame to an existing non-empty table using ``if_exists='append'``.

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

    @pytest.mark.raises
    def test_save_column_with_invalid_data_type(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame, containing a column with a data type not supported by SQLite, to an existing table.

        SQLite does not support unsigned integers for the PRIMARY KEY column.
        pandemy.SaveDataFrameError is expected to be raised.
        """
        # Setup
        # ===========================================================
        df_customer.index = df_customer.index.astype('uint8')

        with sqlite_db_empty.engine.begin() as conn:
            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.SaveDataFrameError, match=r'[Uu]nsigned'):
                sqlite_db_empty.save_df(df=df_customer, table='Customer', conn=conn)

        # Clean up - None
        # ===========================================================

    def test_index_False(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an existing empty table.

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
        r"""Save a DataFrame to an existing empty table.

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

    def test_df_does_not_include_all_columns_of_the_table(self, sqlite_db_empty, df_customer):
        r"""Save a DataFrame to an existing empty table where the DataFrame does not contain all table columns.

        The missing columns BirthDate and Residence should be inserted as NULL.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer ORDER BY CustomerId ASC"""

        df_exp = df_customer.copy()
        df_exp.loc[:, ['BirthDate', 'Residence']] = None
        df_to_save = df_customer.loc[:, ['CustomerName', 'IsAdventurer']]

        with sqlite_db_empty.engine.begin() as conn:

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df_to_save, table='Customer', conn=conn, if_exists='append')

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, parse_dates=['BirthDate'], index_col='CustomerId')

            assert_frame_equal(df_result, df_exp, check_dtype=False, check_index_type=False)

    def test_localize_tz_target_tz_datetime_cols_dtype_datetime_format(self, sqlite_db_empty, df_customer):
        r"""Test the parameters `localize_tz`, `target_tz`, `datetime_cols_dtype` and `datetime_format`.

        The parameters are passed along to the function `pandemy._datetime.convert_datetime_columns`.

        Validate that the datetime column (BirthDate) of the input DataFrame is correctly converted to desired timezone.
        """

        # Setup
        # ===========================================================
        query = 'SELECT * FROM Customer ORDER BY CustomerId'
        datetime_format = r'%Y-%m-%d %H:%M:%S%z'
        localize_tz = 'UTC'
        target_tz = 'CET'

        df_exp_result = df_customer.copy()
        df_exp_result['BirthDate'] = (
            df_exp_result['BirthDate']
            .dt.tz_localize(localize_tz)
            .dt.tz_convert(target_tz)
            .dt.strftime(datetime_format)
        )

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.save_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                if_exists='append',
                datetime_cols_dtype='str',
                datetime_format=datetime_format,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # Verify
        # ===========================================================
        with sqlite_db_empty.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=query,
                con=conn,
                index_col='CustomerId',
            )

        assert_frame_equal(df_updated, df_exp_result, check_dtype=False, check_index_type=True)

        # Clean up - None
        # ===========================================================

    def test_localize_tz_target_tz_datetime_cols_dtype_is_none(self, sqlite_db_empty, df_customer):
        r"""Test the parameters `localize_tz`, `target_tz` when `datetime_cols_dtype` is None.

        The parameter `datetime_cols_dtype` is set to None which means that the datetime column
        (BirthDate) should be localized and converted but kept as a datetime column.
        """

        # Setup
        # ===========================================================
        query = 'SELECT * FROM Customer ORDER BY CustomerId'
        localize_tz = 'UTC'
        target_tz = 'CET'

        df_exp_result = df_customer.copy()
        df_exp_result['BirthDate'] = (
            df_exp_result['BirthDate']
            .dt.tz_localize(localize_tz)
            .dt.tz_convert(target_tz)
            .dt.tz_localize(None)  # Remove timezone information, because it is lost when saving to the database.
        )

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.save_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                if_exists='append',
                datetime_cols_dtype=None,
                localize_tz=localize_tz,
                target_tz=target_tz
            )

        # Verify
        # ===========================================================
        with sqlite_db_empty.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=query,
                con=conn,
                index_col='CustomerId',
                parse_dates=['BirthDate']
            )

        assert_frame_equal(df_updated, df_exp_result, check_dtype=False, check_index_type=True)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_target_tz_no_localize_tz(self, sqlite_db_empty, df_customer):
        r"""Test to supply a value to `target_tz` when `localize_tz` is None.

        pandemy.InvalidInputError is expected to be raised because a naive datetime
        column cannot be converted without first being localized.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            with sqlite_db_empty.engine.begin() as conn:
                sqlite_db_empty.save_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    if_exists='append',
                    datetime_cols_dtype=None,
                    localize_tz=None,
                    target_tz='CET'
                )

        # Clean up - None
        # ===========================================================

    def test_input_df_not_mutated(self, sqlite_db_empty, df_customer):
        r"""Test that the input DataFrame is not mutated after executing the method."""

        # Setup
        # ===========================================================
        df_exp_result = df_customer.copy()

        # Exercise
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            sqlite_db_empty.save_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                if_exists='append',
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d %H:%M:%S%z',
                localize_tz='UTC',
                target_tz='CET'
            )

        # Verify
        # ===========================================================
        assert_frame_equal(df_customer, df_exp_result, check_dtype=True, check_index_type=True)

        # Clean up - None
        # ===========================================================

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

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param('2', id='chunksize=str'),
            pytest.param([3], id='chunksize=[3]'),
            pytest.param(0, id='chunksize=0'),
            pytest.param(-3, id='chunksize=-3')
        )
    )
    def test_invalid_chunksize(self, chunksize, sqlite_db_empty, df_customer):
        r"""Supply invalid values to the `chunksize` parameter.

        `chunksize` should be an integer > 0.

        Parameters
        ----------
        chunksize : int or None, default None
            The number of rows in each batch to be written at a time.
            If None, all rows will be written at once.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidInputError) as exc_info:
                sqlite_db_empty.save_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    chunksize=chunksize,
                )

        # Verify
        # ===========================================================
        assert exc_info.value.data == chunksize

        # Clean up - None
        # ===========================================================


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


class TestUpsertTableMethodUpdateOnly:
    r"""Test the `upsert_table` method of the SQLite DatabaseManager `SQLiteDb`.

    The test cases of this class uses an UPDATE statement only and no INSERT.

    Fixtures
    --------
    sqlite_db_to_modify : pandemy.SQLiteDb
        An instance of the test database where the table data can be modified in each test.

    df_customer : pd.DataFrame
        The Customer table of the test database.

    df_customer_upsert : pd.DataFrame
        The Customer table of the test database with updated data and rows added.

    caplog : pytest.LogCaptureFixture
        Built-in fixture to control logging and access log entries.
    """

    def test_update_all_cols_1_where_col(self, sqlite_db_to_modify, df_customer_upsert):
        r"""Update a table with data from all columns of a DataFrame.

        The default is to update the table with all columns.
        We supply one column to use in the WHERE clause to determine
        how to update the rows. Columns in the WHERE clause should not appear
        among the columns to update. No insert is performed.

        The input DataFrame contains 2 new rows that do not exist in the database table
        Customer. These rows should be ignored.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'
        df_input, _, rows_added = df_customer_upsert

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_input,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(sql=load_table_query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        df_input = df_input.loc[df_input.index.difference(rows_added), :]  # Remove added rows

        assert_frame_equal(df_updated, df_input, check_dtype=False)

        # Clean up - None
        # ===========================================================

    def test_update_all_cols_1_where_col_dry_run(self, sqlite_db_to_modify, df_customer):
        r"""Test the generated UPDATE statement when using the `dry_run` parameter."""

        # Setup
        # ===========================================================
        update_stmt_exp = (
            """UPDATE Customer
SET
    BirthDate = :BirthDate,
    Residence = :Residence,
    IsAdventurer = :IsAdventurer
WHERE
    CustomerName = :CustomerName"""
        )

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            update_stmt, _ = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                upsert_cols='all',
                update_only=True,
                datetime_cols_dtype='str',
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert update_stmt == update_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_update_1_col_using_2_where_cols(self, sqlite_db_to_modify, df_customer_upsert, df_customer):
        r"""Update a single column of a table and use 2 columns of the DataFrame in the WHERE clause.

        The rows that do not match equality of the `where_cols` CustomerName and Residence will not be
        updated. Only the BirthDate column is selected for update.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        df_input, _, rows_added = df_customer_upsert
        df_exp = df_input.copy()  # The expected result

        # The rows that should not be updated
        df_exp.loc[3:4, 'BirthDate'] = df_customer.loc[3:4, 'BirthDate']
        df_exp.loc[3:4, 'Residence'] = df_customer.loc[3:4, 'Residence']
        df_exp.loc[5, 'IsAdventurer'] = df_customer.loc[5, 'IsAdventurer']
        df_exp = df_exp.loc[df_exp.index.difference(rows_added), :]  # Remove added rows

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_input,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName', 'Residence'],
                upsert_cols=['BirthDate'],
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(sql=load_table_query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        assert_frame_equal(df_updated, df_exp, check_dtype=False)

        # Clean up - None
        # ===========================================================

    def test_update_1_col_using_2_where_cols_dry_run(self, sqlite_db_to_modify, df_customer):
        r"""Test the generated UPDATE statement when using the `dry_run` parameter."""

        # Setup
        # ===========================================================
        update_stmt_exp = """UPDATE Customer
SET
    BirthDate = :BirthDate
WHERE
    CustomerName = :CustomerName AND
    Residence = :Residence"""

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            update_stmt, _ = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName', 'Residence'],
                upsert_cols=['BirthDate'],
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d',
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert update_stmt == update_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_update_2_cols_and_include_index(self, sqlite_db_to_modify, df_customer):
        r"""Update 2 columns of a table and include the index column to the columns to update.

        Column BirthDate and index column CustomerId are selected for update.
        Using 3 where_cols. No insert is performed.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # Modify the index CustomerId
        df_customer = df_customer.reset_index()
        df_customer.loc[0, 'CustomerId'] = 0
        df_customer = df_customer.set_index('CustomerId')

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName', 'Residence', 'IsAdventurer'],
                upsert_cols=['BirthDate'],
                upsert_index_cols=True,
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(sql=load_table_query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        assert_frame_equal(df_updated, df_customer, check_dtype=False)

        # Clean up - None
        # ===========================================================

    def test_update_all_levels_of_multiindex(self, sqlite_db_to_modify, df_customer):
        r"""Update all columns of the MultiIndex and no regular columns of the DataFrame."""

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # Modify and update the index to a MultiIndex
        df_customer = df_customer.reset_index()
        df_customer.loc[0, 'CustomerId'] = 0
        df_customer.loc[1, 'CustomerName'] = 'Captain Lawgof'
        df_customer = df_customer.set_index(['CustomerId', 'CustomerName'])

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['BirthDate'],
                upsert_cols=None,
                upsert_index_cols=True,
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=load_table_query,
                con=conn,
                index_col=['CustomerId', 'CustomerName'],
                parse_dates=['BirthDate']
            )

        assert_frame_equal(df_updated, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_update_1_col_and_all_levels_of_multiindex(self, sqlite_db_to_modify, df_customer):
        r"""Update 1 column and all columns of the MultiIndex of the DataFrame."""

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # Modify and update the index to a MultiIndex
        df_customer = df_customer.reset_index()
        df_customer.loc[0, 'CustomerId'] = 0
        df_customer.loc[1, 'CustomerName'] = 'Captain Lawgof'
        df_customer.loc[1, 'IsAdventurer'] = 1
        df_customer = df_customer.set_index(['CustomerId', 'CustomerName'])

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['BirthDate', 'Residence'],
                upsert_cols=['IsAdventurer'],
                upsert_index_cols=True,
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=load_table_query,
                con=conn,
                index_col=['CustomerId', 'CustomerName'],
                parse_dates=['BirthDate']
            )

        assert_frame_equal(df_updated, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_update_2_cols_and_include_1_level_of_multiindex(self, sqlite_db_to_modify, df_customer):
        r"""Update 2 columns and include 1 column of the MultiIndex to update."""

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # Modify data and make MultiIndex
        df_customer = df_customer.reset_index()
        df_customer.loc[1, 'CustomerName'] = 'Martin the Master Gardener'
        df_customer.loc[1, 'Residence'] = 'Draynor Village'
        df_customer.loc[1, 'IsAdventurer'] = 0
        df_customer = df_customer.set_index(['CustomerId', 'CustomerName'])

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['BirthDate'],
                upsert_cols=['IsAdventurer', 'Residence'],
                upsert_index_cols=['CustomerName'],
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=load_table_query,
                con=conn,
                index_col=['CustomerId', 'CustomerName'],
                parse_dates=['BirthDate']
            )

        assert_frame_equal(df_updated, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_return_types(self, sqlite_db_to_modify, df_customer):
        r"""Check the return types when using the option to `update_only`."""

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            result_update, result_insert = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        assert isinstance(result_update, sqlalchemy.engine.CursorResult)
        assert result_insert is None

        # Clean up - None
        # ===========================================================

    def test_pandas_numpy_nan_types(self, sqlite_db_to_modify, df_customer):
        r"""Test that pandas.NA, pandas.NaT and numpy.nan can be correctly converted to None.

        SQLite cannot handle pandas.NA, pandas.NaT or numpy.nan (NaN values) in an update statement.
        These values must be converted to None first. The conversion from NaN values
        to None is controlled by the `nan_to_none` option.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # If a where_col has a NaN that row cannot be updated. Need to save the original value.
        original_customer_name = df_customer.loc[2, 'CustomerName']
        df_customer.loc[2, 'CustomerName'] = np.nan
        df_customer.loc[3, 'Residence'] = pd.NA
        df_customer.loc[3, 'BirthDate'] = pd.NaT

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=True,
                nan_to_none=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=load_table_query,
                con=conn,
                index_col='CustomerId',
                parse_dates=['BirthDate']
            )

        df_customer.loc[2, 'CustomerName'] = original_customer_name  # Restore original value
        assert_frame_equal(df_updated, df_customer, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_localize_tz_target_tz_datetime_cols_dtype_datetime_format(self, sqlite_db_to_modify, df_customer):
        r"""Test the parameters `localize_tz`, `target_tz`, `datetime_cols_dtype` and `datetime_format`.

        The parameters are passed along to the function `pandemy._datetime.convert_datetime_columns`.

        Validate that the datetime column (BirthDate) of the input DataFrame is correctly converted to desired timezone.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'
        datetime_format = r'%Y-%m-%d %H:%M:%S%z'
        localize_tz = 'UTC'
        target_tz = 'CET'

        df_exp_result = df_customer.copy()
        df_exp_result['BirthDate'] = (
            df_exp_result['BirthDate']
            .dt.tz_localize(localize_tz)
            .dt.tz_convert(target_tz)
            .dt.strftime(datetime_format)
        )

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=True,
                nan_to_none=True,
                datetime_cols_dtype='str',
                datetime_format=datetime_format,
                localize_tz=localize_tz,
                target_tz=target_tz
            )


        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(
                sql=load_table_query,
                con=conn,
                index_col='CustomerId',
            )

        assert_frame_equal(df_updated, df_exp_result, check_dtype=False, check_index_type=False)

        # Clean up - None
        # ===========================================================

    def test_logging_output_debug(self, sqlite_db_to_modify, df_customer, caplog):
        r"""Test the logging output when no errors occur.

        If no errors occur the UPDATE and INSERT statements are logged with
        log level DEBUG (10).
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=True,
                nan_to_none=True,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        for record in caplog.records:
            assert record.levelno == 10
            assert record.levelname == 'DEBUG'

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_datetime_format(self, sqlite_db_to_modify, df_customer):
        r"""Supply an invalid datetime format to the `datetime_format` parameter.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidInputError):
                sqlite_db_to_modify.upsert_table(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    where_cols=['CustomerName'],
                    update_only=True,
                    nan_to_none=True,
                    datetime_cols_dtype='str',
                    datetime_format=2
                )

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_table_name(self, sqlite_db_to_modify, df_customer):
        r"""Supply an invalid table name.

        pandemy.InvalidTableNameError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidTableNameError):
                sqlite_db_to_modify.upsert_table(
                    df=df_customer,
                    table='Customer Table',
                    conn=conn,
                    where_cols=['CustomerName'],
                    update_only=True,
                    datetime_cols_dtype='str',
                )

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param('2', id='chunksize=str'),
            pytest.param([3], id='chunksize=[3]'),
            pytest.param(-3, id='chunksize=-3')
        )
    )
    def test_invalid_chunksize(self, chunksize, sqlite_db_to_modify, df_customer):
        r"""Supply invalid values to the `chunksize` parameter.

        Parameters
        ----------
        chunksize : int or None, default None
            Divide `df` into chunks and perform the upsert in chunks of `chunksize` rows.
            If None all rows of `df` are processed in one chunk, which is the default.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidInputError) as exc_info:
                sqlite_db_to_modify.upsert_table(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    where_cols=['CustomerName'],
                    update_only=True,
                    chunksize=chunksize,
                    datetime_cols_dtype='str'
                )

        # Verify
        # ===========================================================
        assert exc_info.value.data == chunksize

        # Clean up - None
        # ===========================================================


class TestUpsertTableMethodUpdateAndInsert:
    r"""Test the `upsert_table` method of the SQLite DatabaseManager `SQLiteDb`.

    The test cases of this class uses an UPDATE statement followed by an INSERT.

    Fixtures
    --------
    sqlite_db_to_modify : pandemy.SQLiteDb
        An instance of the test database where the table data can be modified in each test.

    df_customer : pd.DataFrame
        The Customer table of the test database.

    df_customer_upsert : pd.DataFrame
        The Customer table of the test database with updated data and rows added.

    caplog : pytest.LogCaptureFixture
        Built-in fixture to control logging and access log entries.
    """

    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param(None, id='chunksize=None'),
            pytest.param(2, id='chunksize=2'),
            pytest.param(10, id='chunksize=10'),
        )
    )
    def test_upsert_all_cols_1_where_col(self, chunksize, sqlite_db_to_modify, df_customer_upsert):
        r"""Update a table with data from all columns of a DataFrame and insert new rows.

        We supply one column to use in the WHERE clause to determine
        how to update the rows. Columns in the WHERE clause should not appear
        among the columns to update. An INSERT statement is performed after the update.

        The input DataFrame contains 2 new rows that do not exist in the database table
        Customer. These rows should be added.

        Parameters
        ----------
        chunksize : int or None
            Divide the DataFrame into chunks and perform the upsert in chunks of `chunksize` rows.
            If None all rows of the DataFrame are processed in one chunk, which is the default.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'
        df_input, _, _ = df_customer_upsert

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_input,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=False,
                chunksize=chunksize,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(sql=load_table_query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        assert_frame_equal(df_updated, df_input, check_dtype=False)

        # Clean up - None
        # ===========================================================

    def test_upsert_all_cols_1_where_col_dry_run(self, sqlite_db_to_modify, df_customer):
        r"""Test the generated UPDATE and INSERT statement when using the `dry_run` parameter."""

        # Setup
        # ===========================================================
        update_stmt_exp = (
            """UPDATE Customer
SET
    BirthDate = :BirthDate,
    Residence = :Residence,
    IsAdventurer = :IsAdventurer
WHERE
    CustomerName = :CustomerName"""
        )

        insert_stmt_exp = (
            """INSERT INTO Customer (
    CustomerName,
    BirthDate,
    Residence,
    IsAdventurer
)
    SELECT
        :CustomerName,
        :BirthDate,
        :Residence,
        :IsAdventurer
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM Customer
            WHERE
                CustomerName = :CustomerName
        )"""
        )

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            update_stmt, insert_stmt = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                upsert_cols='all',
                update_only=False,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d',
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert update_stmt == update_stmt_exp
        assert insert_stmt == insert_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_upsert_2_cols_include_index_3_where_cols(self, sqlite_db_to_modify, df_customer):
        r"""Upsert 2 columns of a table and include the index column to the columns to update.

        Column BirthDate and index column CustomerId are selected for update and insert.
        Using 3 where_cols. There are no new rows in the DataFrame. No rows will be inserted.
        """

        # Setup
        # ===========================================================
        load_table_query = 'SELECT * FROM Customer ORDER BY CustomerId'

        # Modify the index CustomerId
        df_customer = df_customer.reset_index()
        df_customer.loc[0, 'CustomerId'] = 0
        df_customer = df_customer.set_index('CustomerId')

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName', 'Residence', 'IsAdventurer'],
                upsert_cols=['BirthDate'],
                upsert_index_cols=True,
                update_only=False,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.connect() as conn:
            df_updated = pd.read_sql(sql=load_table_query, con=conn, index_col='CustomerId', parse_dates=['BirthDate'])

        assert_frame_equal(df_updated, df_customer, check_dtype=False)

        # Clean up - None
        # ===========================================================

    def test_upsert_2_cols_include_index_3_where_cols_dry_run(self, sqlite_db_to_modify, df_customer):
        r"""Test the generated UPDATE and INSERT statement when using the `dry_run` parameter."""

        # Setup
        # ===========================================================
        update_stmt_exp = (
            """UPDATE Customer
SET
    BirthDate = :BirthDate,
    CustomerId = :CustomerId
WHERE
    CustomerName = :CustomerName AND
    Residence = :Residence AND
    IsAdventurer = :IsAdventurer"""
        )

        insert_stmt_exp = (
            """INSERT INTO Customer (
    BirthDate,
    CustomerId
)
    SELECT
        :BirthDate,
        :CustomerId
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM Customer
            WHERE
                CustomerName = :CustomerName AND
                Residence = :Residence AND
                IsAdventurer = :IsAdventurer
        )"""
        )

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            update_stmt, insert_stmt = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName', 'Residence', 'IsAdventurer'],
                upsert_cols=['BirthDate'],
                upsert_index_cols=True,
                update_only=False,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d',
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert update_stmt == update_stmt_exp
        assert insert_stmt == insert_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_empty_dataframe(self, sqlite_db_to_modify, df_customer):
        r"""Supply an empty DataFrame.

        No statements should be executed on the database and the return values
        `result_update` and `result_insert` should be None.
        """

        # Setup
        # ===========================================================
        df_empty = df_customer.iloc[:0]

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            result_update, result_insert = sqlite_db_to_modify.upsert_table(
                df=df_empty,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=False,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        assert result_update is None
        assert result_insert is None

        # Clean up - None
        # ===========================================================

    def test_return_types(self, sqlite_db_to_modify, df_customer):
        r"""Check the return types when using UPDATE followed by INSERT."""

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            result_update, result_insert = sqlite_db_to_modify.upsert_table(
                df=df_customer,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=False,
                datetime_cols_dtype='str',
                datetime_format=r'%Y-%m-%d'
            )

        # Verify
        # ===========================================================
        assert isinstance(result_update, sqlalchemy.engine.CursorResult)
        assert isinstance(result_insert, sqlalchemy.engine.CursorResult)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_upsert_triggers_not_null_constraint(self, sqlite_db_to_modify, df_customer_upsert):
        r"""Update and insert into 3 columns and use 1 where column.

        The column IsAdventurer is not added to be updated and inserted and will therefore
        trigger the NOT NULL constraint on insert.

        pandemy.ExecuteStatementError is expected to be raised.
        """

        # Setup
        # ===========================================================
        df_upsert, _, _ = df_customer_upsert

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.ExecuteStatementError, match='NOT NULL'):
                sqlite_db_to_modify.upsert_table(
                    df=df_upsert,
                    table='Customer',
                    conn=conn,
                    where_cols=['CustomerName'],
                    upsert_cols=['CustomerName', 'BirthDate', 'Residence'],
                    update_only=False,
                    datetime_cols_dtype='str',
                    datetime_format=r'%Y-%m-%d'
                )

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_logging_output_error(self, sqlite_db_to_modify, df_customer_upsert, caplog):
        r"""Test the logging output when an error occurs while executing the statements.

        If errors occur the UPDATE and INSERT statements are logged with
        log level ERROR (40).

        The column IsAdventurer is not added to be updated and inserted and will therefore
        trigger the NOT NULL constraint on insert.

        pandemy.ExecuteStatementError is expected to be raised.
        """

        # Setup
        # ===========================================================
        df_upsert, _, _ = df_customer_upsert

        # Exercise
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.ExecuteStatementError):
                sqlite_db_to_modify.upsert_table(
                    df=df_upsert,
                    table='Customer',
                    conn=conn,
                    where_cols=['CustomerName'],
                    upsert_cols=['CustomerName', 'BirthDate', 'Residence'],
                    update_only=False,
                    datetime_cols_dtype='str',
                    datetime_format=r'%Y-%m-%d'
                )

        # Verify
        # ===========================================================
        for record in caplog.records:
            assert record.levelno == 40
            assert record.levelname == 'ERROR'

        # Clean up - None
        # ===========================================================


class TestMergeDfMethod:
    r"""Test the `merge_df` method of the SQLite DatabaseManager `SQLiteDb`.

    SQLite does not support the MERGE statement.

    Fixtures
    --------
    sqlite_db_to_modify : pandemy.SQLiteDb
        An instance of the test database where the table data can be modified in each test.

    df_customer : pd.DataFrame
        The Customer table of the test database.
    """

    @pytest.mark.raises
    def test_merge_statement_not_supported(self, sqlite_db_to_modify, df_customer):
        r"""Test that `SQLiteDb` does not support the `merge_df` method.

        Because SQLite does not support the MERGE statement
        pandemy.SQLStatementNotSupportedError is expected to be raised
        when calling the `merge_df` method.

        The class variable `_merge_df_stmt` is an empty string the `DatabaseManager`
        does not support the `merge_df` method.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with sqlite_db_to_modify.engine.begin() as conn:
            with pytest.raises(pandemy.SQLStatementNotSupportedError, match='SQLiteDb'):
                sqlite_db_to_modify.merge_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    on_cols=['CustomerName'],
                    nan_to_none=True,
                    datetime_cols_dtype='str',
                    datetime_format=r'%Y-%m-%d'
                )

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

    driver : str, default 'sqlite3'
        The database driver to use. The default is the Python built-in module :mod:`sqlite3`,
        which is also the default driver of SQLAlchemy.
        When the default is used no driver name is displayed in the connection URL.

    url : :class:`str` or :class:`sqlalchemy.engine.URL` or None, default None
        A SQLAlchemy connection URL to use for creating the database engine.
        It overrides the value of `file` and `must_exist`.

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
        'file, must_exist, driver, url, container',
        (
            pytest.param(
                ':memory:',
                True, 
                'sqlite3',
                None,
                None,
                id="file=':memory', no container"
            ),
            pytest.param(
                Path('Runescape.db'),
                False, 
                'pysqlite',
                None,
                SQLiteSQLContainer,
                id='file=Path, with container'
            ),
            pytest.param(
                'Runescape.db',
                False, 
                'sqlite3',
                'sqlite+pysqlite://',
                SQLiteSQLContainer,
                id='url=sqlite+pysqlite://'
            ),
        )
    )
    def test__repr__(self, file, must_exist, driver, url, container):
        r"""Test the output of the `__repr__` method."""

        # Setup
        # ===========================================================
        db = pandemy.SQLiteDb(file=file, must_exist=must_exist, driver=driver, url=url, container=container)

        exp_result = f"""SQLiteDb(
    file={db.file!r},
    must_exist={db.must_exist!r},
    driver={db.driver!r},
    url={db.url!r},
    container={db.container!r},
    connect_args={db.connect_args!r},
    engine_config={db.engine_config!r},
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


class TestConnStrDeprecation:
    r"""Tests for the `conn_str` attribute deprecation. 

    The `conn_str` attribute is deprecated in version 1.2.0 and refactored into a property.
    It should trigger a DeprecationWarning when accessed and return the value of the url attribute.
    """

    def test_access_conn_str(self):
        r"""Test that `conn_str` can be accessed and that it returns the string value of the url attribute."""

        # Setup
        # ===========================================================
        url = 'sqlite:///Runescape.db'
        db = pandemy.SQLiteDb(url=url)

        # Exercise
        # ===========================================================
        result = db.conn_str 

        # Verify
        # ===========================================================
        assert result == str(db.url)

        # Clean up - None
        # ===========================================================

    def test_deprecation_warning(self):
        r"""Test that the DeprecationWarning is triggered when `conn_str` is accessed."""

        # Setup
        # ===========================================================
        url = 'sqlite:///Runescape.db'
        db = pandemy.SQLiteDb(url=url)
        message = 'conn_str attribute is deprecated in version 1.2.0 and replaced by url. Use SQLiteDb.url instead.'

        # Exercise & Verify
        # ===========================================================
        with pytest.warns(DeprecationWarning, match=message):
            db.conn_str 

        # Clean up - None
        # ===========================================================
