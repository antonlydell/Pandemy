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

    sqlite_db: Path
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
    def test_file_must_exist(self, file_as_str, sqlite_db):
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
            db = pandemy.SQLiteDb(file=str(sqlite_db), must_exist=True)
        else:
            db = pandemy.SQLiteDb(file=sqlite_db, must_exist=True)

        # Verify
        # ===========================================================
        assert db.file == sqlite_db
        assert db.must_exist is True
        assert db.conn_str == fr'sqlite:///{str(sqlite_db)}'
        assert isinstance(db.engine, sqlalchemy.engine.base.Engine)

        # Clean up - None
        # ===========================================================

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


def test_execute_statement():
    """
    Test the execute statement method of the DatabaseManager.

    Given: An SQLiteDb instance
    """