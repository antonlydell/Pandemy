"""
Tests for the DatabaseManager class through the implementation of the SQLite database (SQLiteDb).
"""

# =================================================
# Imports
# =================================================

# Standard Library
from pathlib import Path

# Third Party
import pandas as pd
from pandas.testing import assert_frame_equal

import pytest

from sqlalchemy.sql import text
import sqlalchemy

# Local
import pandemy

# =================================================
# Setup
# =================================================


class SQLiteDbStatement(pandemy.DbStatement):
    """A correctly defined pandemy.DbStatement subclass"""
    my_query = 'SELECT * FROM MyTable;'


class SQLiteFakeDbStatement:
    """
    DbStatement class that does not inherit from pandemy.DbStatement.
    This class is not a valid input to the statement parameter of
    `pandemy.DatabaseManager`.
    """
    my_query = 'SELECT * FROM MyTable;'

# =================================================
# Tests
# =================================================


class TestInitSQLiteDb:
    """
    Test the initalization of the SQLiteDb DatabaseManager.

    Fixtures
    --------

    sqlite_db_file: Path
        Path to a SQLite database that exists on disk.
    """

    def test_all_defaults(self):
        """
        Create an instance of SQLiteDb that lives in memory.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb()

        # Verify
        # ===========================================================
        assert db.file == ':memory:'
        assert db.must_exist is True
        assert db.statement is None
        assert db.engine_config is None
        assert db.conn_str == r'sqlite://'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    def test_in_memory(self):
        """
        Create an instance of SQLiteDb that lives in memory.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=':memory:')

        # Verify
        # ===========================================================
        assert db.file == ':memory:'
        assert db.must_exist is True
        assert db.conn_str == r'sqlite://'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize('file_as_str', [pytest.param(True, id='str'), pytest.param(False, id='Path')])
    def test_file_must_exist(self, file_as_str, sqlite_db_file):
        """
        Create an instance with a file supplied as a string and pathlib.Path object.

        The default option `must_exist` is set to True.
        The file exists on disk.

        Parameters
        ----------

        file_as_str: bool
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
        """
        Create an instance with a file supplied as a string and pathlib.Path object.

        The default option `must_exist` is set to True.
        The file does not exists on disk.

        Parameters
        ----------

        file: str or Path
            The file with the SQLite database.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(FileNotFoundError):
            pandemy.SQLiteDb(file=file, must_exist=True)

        # Clean up - None
        # ===========================================================

    def test_on_file_with_DbStatement(self):
        """
        Create an instance with a DbStatement class.

        The option `must_exist` is set to False.
        The file does not exists on disk.
        """

        # Setup
        # ===========================================================
        must_exist = False
        file = 'mydb.db'

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=file, must_exist=must_exist, statement=SQLiteDbStatement)

        # Verify
        # ===========================================================
        assert db.file == Path(file)
        assert db.must_exist is must_exist
        assert db.statement is SQLiteDbStatement

        # Clean up - None
        # ===========================================================

    # file, must_exist, statement, engine_config, error_msg
    input_test_bad_input = [
        pytest.param(42, False, None, None, 'Received: 42', id='file=42'),

        pytest.param('my_db.db', 'False', None, {'encoding': 'UTF-8'}, 'Received: False', id="must_exist='False'"),

        pytest.param('my_db.db', False,  [42], None, 'statement must be a subclass of DbStatement',
                     id="statement=[42]"),

        pytest.param(Path('my_db.db'), False,  SQLiteFakeDbStatement, None,
                     'statement must be a subclass of DbStatement', id="statement=FakeDbStatement"),

        pytest.param('my_db.db', False,  None, 42, 'engine_config must be a dict', id="engine_config=42"),
    ]

    @pytest.mark.raises
    @pytest.mark.parametrize('file, must_exist, statement, engine_config, error_msg', input_test_bad_input)
    def test_bad_input_parameters(self, file, must_exist, statement, engine_config, error_msg):
        """
        Test bad input parameters. TypeError is expected to be raised.

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
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(TypeError, match=error_msg):
            pandemy.SQLiteDb(file=file, must_exist=must_exist, statement=statement, engine_config=engine_config)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_parameter_to_create_engine(self):
        """
        Test to supply an invalid parameter to the SQLAlchemy create_engine function.

        pandemy.CreateEngineError is expected to be raised.

        Also supply a key word argument that is not used for anything.
        It should not affect the initialization.
        """

        # Setup
        # ===========================================================
        error_msg = 'invalid_param'
        engine_config = {'invalid_param': True}

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.CreateEngineError, match=error_msg):
            pandemy.SQLiteDb(file='my_db.db', must_exist=False, statement=None,
                             engine_config=engine_config, kwarg='kwarg')

        # Clean up - None
        # ===========================================================


class TestExecuteMethod:
    """
    Test the execute method of the SQLiteDb DatabaseManager.

    Fixtures
    --------

    sqlite_db: pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty: pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_owner: pd.DataFrame
        The owner table of the test database.
    """

    # The query for test_select_all_owners
    select_all_owners = """SELECT OwnerId, OwnerName, BirthDate FROM Owner;"""

    @pytest.mark.parametrize('query', [pytest.param(select_all_owners, id='query: str'),
                                       pytest.param(text(select_all_owners), id='query: sqlalchemy TextClause')])
    def test_select_all_owners(self, query, sqlite_db, df_owner):
        """
        Test to execute a SELECT query.

        Query all rows from the Owner table.

        Parameters
        ----------

        query: str or text
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
        """
        Test to execute a SELECT query with a query parameter.

        Parameters
        ----------

        query: str or text
            The SQL query to execute.

        owner_id: int
            The parameter representing OwnerId in `query`.

        owner_exp: Owner
            The expected row of the owner from `query`.
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
        """
        Test to execute a SELECT query with 2 query parameters.
        """

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
        """
        Test to insert new owner(s) to the Owner table of the empty test database.

        Parameters
        ----------

        params: list of dict
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
        """
        Execute a SELECT query with invalid syntax.

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
        """
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


class TestSaveDfMethod:
    """
    Test the save_df method of the SQLiteDb DatabaseManager.

    Fixtures
    --------

    sqlite_db: pandemy.SQLiteDb
        An instance of the test database.

    sqlite_db_empty: pandemy.SQLiteDb
        An instance of the test database where all tables are empty.

    df_customer: pd.DataFrame
        The Customer table of the test database.
    """

    @pytest.mark.parametrize('chunksize', [pytest.param(None, id='chunksize=None'),
                                           pytest.param(2, id='chunksize=2')])
    def test_save_to_existing_empty_table(self, chunksize, sqlite_db_empty, df_customer):
        """
        Save a DataFrame to an exisitng empty table.

        Parameters
        ----------

        chunksize: int or None
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

    def test_save_to_existing_non_empty_table_if_exists_replace(self, sqlite_db_empty, df_customer):
        """
        Save a DataFrame to an exisitng non empty table.
        The existing rows are deleted before writing the DataFrame.
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
        """
        Save a DataFrame to an exisitng table when `if_exists` = 'fail'.
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

    def test_save_to_existing_non_empty_table_if_exists_append(self, sqlite_db_empty, df_customer):
        """
        Save a DataFrame to an exisitng non empty table.
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
        """
        Save a DataFrame to an exisitng empty table.
        The index column of the DataFrame is not written to the table.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        df = df_customer.copy()  # Copy to avoid modifying the fixture
        df.reset_index(inplace=True)  # Convert the index CustomerId to a regular column

        with sqlite_db_empty.engine.begin() as conn:

            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df, table='Customer', conn=conn, if_exists='append', index=False)

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, parse_dates=['BirthDate'])

            assert_frame_equal(df_result, df, check_dtype=False, check_index_type=False)

    # Input data to test_index_label
    # new_index_names, index_labels, index_type,
    input_test_index_label = [
        pytest.param('Index', 'CustomerId', 'Single', id='Single'),
        pytest.param(['Level_1', 'Level_2'], ['CustomerId', 'CustomerName'], 'Multi', id='Multi'),
    ]

    @pytest.mark.parametrize('new_index_names, index_labels, index_type', input_test_index_label)
    def test_index_label(self, new_index_names, index_labels, index_type,
                         sqlite_db_empty, df_customer):
        """
        Save a DataFrame to an exisitng empty table.
        Supply custom name(s) for the index of the DataFrame.
        that will be used as the column names in the database.

        The index name(s) of the DataFrame do not match the column names
        of the table Customer in the Database.

        Test with both a single and multiindex DataFrame.

        Parameters
        ----------

        new_index_names: str or list of str
            The new index names to assign to the DataFrame to save to the database.

        index_labels: str or list of str
            The names to use for the index when saving the DataFrame to the database.

        index_type: str ('Single', 'Multi')
            The type of index of the DataFrame.
        """

        # Setup
        # ===========================================================
        query = """SELECT * FROM Customer;"""

        df = df_customer.copy()  # Copy to avoid modifying the fixture

        if index_type == 'Single':
            df.index.name = new_index_names
        elif index_type == 'Multi':
            df.set_index(index_labels[1], inplace=True, append=True)
            df.index.names = new_index_names

        with sqlite_db_empty.engine.begin() as conn:
            # Exercise
            # ===========================================================
            sqlite_db_empty.save_df(df=df, table='Customer', conn=conn, if_exists='append',
                                    index=True, index_label=index_labels)

            # Verify
            # ===========================================================
            df_result = pd.read_sql(sql=query, con=conn, index_col=index_labels, parse_dates=['BirthDate'])

            # Change back to expected index name
            if index_type == 'Single':
                df.index.name = 'CustomerId'
            elif index_type == 'Multi':
                df.index.names = ['CustomerId', 'CustomerName']

            assert_frame_equal(df_result, df, check_dtype=False, check_index_type=False)

    @pytest.mark.raises
    @pytest.mark.parametrize('df', [pytest.param('df', id='str'),
                                    pytest.param([], id='list')])
    def test_invalid_input_df(self, df, sqlite_db_empty):
        """
        Supply an invalid argument to the `df` parameter.
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
        """
        Supply an invalid argument to the `if_exists` parameter.
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
                                      pytest.param(pandemy.DbStatement, id='class')])
    def test_invalid_input_conn(self, conn, sqlite_db_empty, df_customer):
        """
        Supply an invalid argument to the `conn` parameter.
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
    @pytest.mark.parametrize('table', [pytest.param('delete', id='delete'),
                                       pytest.param('customer name', id='customer name'),
                                       pytest.param('as', id='as')])
    def test_invalid_input_table(self, table, sqlite_db_empty, df_customer):
        """
        Supply an invalid argument to the `table` parameter.
        pandemy.InvalidTableNameError is expected to be raised.
        """

        # Setup
        # ===========================================================
        with sqlite_db_empty.engine.begin() as conn:

            # Exercise & Verify
            # ===========================================================
            with pytest.raises(pandemy.InvalidTableNameError):
                sqlite_db_empty.save_df(df=df_customer, table=table, conn=conn)
