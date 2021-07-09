"""
Tests for the DatabaseManager class through the implementation of the SQLite database.
"""

# =================================================
# Imports
# =================================================

# Standard Library
from pathlib import Path

# Third Party
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
