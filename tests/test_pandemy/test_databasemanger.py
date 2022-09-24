r"""Tests for the `DatabaseManager` base class.

The `DatabaseManager` can be initialized with a SQLAlchemy connection URL if no
subclass of `DatabaseManager` is implemented for the desired SQL dialect.
"""

# =================================================
# Imports
# =================================================

# Standard Library

# Third Party
import pytest
from sqlalchemy.engine import create_engine, Engine, make_url

# Local
import pandemy

# =================================================
# Setup
# =================================================


class DbContainer(pandemy.SQLContainer):
    r"""A container of SQL statements."""

    get_all_items: str = 'SELECT * FROM Item'

# =================================================
# Tests
# =================================================


class TestInitMethod:
    r"""Test the initialization of the `DatabaseManager`."""

    @pytest.mark.parametrize(
        'url',
        (
            pytest.param('sqlite:///Runescape.db', id='str'),
            pytest.param(make_url('sqlite:///Runescape.db'), id='sqlalchemy.engine.URL')
        )
    )
    def test_sqlite_url(self, url):
        r"""Connect to a SQLite database.

        Connect with a relative path to the database file.
        Check that all attributes of the instance are properly set.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.DatabaseManager(url=url)

        # Verify
        # ===========================================================
        assert str(db.url) == str(url)
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert isinstance(db.engine, Engine)

        # Clean up - None
        # ===========================================================

    def test_oracle_url(self):
        r"""Connect to an Oracle database.

        Check that all attributes of the instance are properly set.
        """

        # Setup
        # ===========================================================
        url_str = r'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=wollysheep'
        url_repr = r'oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=wollysheep'
        connect_args = {'encoding': 'UTF-8', 'nencoding': 'UTF-8'}
        engine_config = {'coerce_to_unicode': False}

        # Exercise
        # ===========================================================
        db = pandemy.DatabaseManager(
            url=url_str,
            container=DbContainer,
            connect_args=connect_args,
            engine_config=engine_config
        )

        # Verify
        # ===========================================================
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr
        assert db.container is DbContainer
        assert db.connect_args == connect_args
        assert db.engine_config == engine_config

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'username, username_exp, password, password_exp',
        (
            pytest.param(
                'Fred the_Farmer', 'Fred+the_Farmer',
                'Penguins-sheep-are-not', 'Penguins-sheep-are-not',
                id='username with a space'
            ),

            pytest.param(
                'Fred_the_Farmer', 'Fred_the_Farmer',
                'Penguins-sheep-are-not!', r'Penguins-sheep-are-not%21',
                id='password with a !'
            ),

            pytest.param(
                'Fred_the#Farmer', r'Fred_the%23Farmer',
                'Penguins¤sheep-are-not', r'Penguins%C2%A4sheep-are-not',
                id='username and password quoted'
            ),

            pytest.param(
                'Fred_the#Farmer', r'Fred_the%23Farmer',
                'Penguins-æ♬∅-are-not', r'Penguins-%C3%A6%E2%99%AC%E2%88%85-are-not',
                id='Odd characters in password'
            ),
        )
    )
    def test_username_and_password_that_need_url_quoting(self, username, username_exp, password, password_exp):
        r"""Connect to a Microsoft SQL Server database with usernames and passwords that need url quoting.

        The username and or password contain special characters that must be encoded to
        be parsed correctly in the URL.

        Parameters
        ----------
        username: str
            The input username.

        username_exp: str
            The expected username after URL quoting.

        password: str
            The input password.

        password_exp: str
            The expected password after URL quoting.
        """

        # Setup
        # ===========================================================
        url: str = (
            fr'mssql+pyodbc://{username}:{password}@fred.farmer.rs:1234'
            '?driver=ODBC+Driver+18+for+SQL+Server'
            '&authentication=ActiveDirectoryIntegrated'
            '&TrustServerCertificate=Yes'
        )

        # Exercise
        # ===========================================================
        db = pandemy.DatabaseManager(url=url)

        # Verify
        # ===========================================================
        assert db.url.username == username_exp
        assert db.url.password == password_exp

        # Clean up - None
        # ===========================================================

    def test_supply_existing_engine(self):
        r"""Connect to a SQLite database with an existing database engine.

        The `DatabaseManager` should not create its own engine, but use the
        provided engine instead.
        """

        # Setup
        # ===========================================================
        engine = create_engine('sqlite:///Runescape.db')

        # Exercise
        # ===========================================================
        db = pandemy.DatabaseManager(engine=engine)

        # Verify
        # ===========================================================
        db.engine is engine

        # Clean up - None
        # ===========================================================

    def test_connect_with_unknown_arguments(self):
        r"""Connect to a SQLite database and pass additional arguments that are not used by `DatabaseManager`.

        The additional unknown arguments should be captured by **kwargs and ignored.
        """

        # Setup
        # ===========================================================
        url = 'sqlite://'  # SQLite in-memory database

        # Exercise
        # ===========================================================
        db = pandemy.DatabaseManager(url=url, farming_skill=99)

        # Verify
        # ===========================================================
        assert not hasattr(db, 'farming_skill')

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_specify_url_and_engine(self):
        r"""Specify the `url` and `engine` parameters at the same time.

        The `DatabaseManager` cannot know if it should create an engine from
        the url or use the provided engine.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        url = 'sqlite://'  # SQLite in-memory database
        engine = create_engine('sqlite:///Runescape.db')

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy.DatabaseManager(url=url, engine=engine)

        # Verify
        # ===========================================================
        assert exc_info.value.data == (url, engine)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_url_and_engine_are_none(self):
        r"""Leave all parameters as None.

        If the parameters `url` and `engine` are None at the same time the
        `DatabaseManager` has no info about how to create an engine.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy.DatabaseManager()

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_connect_with_invalid_engine_config(self):
        r"""Connect to a SQLite database and pass an invalid configuration to the create_engine function.

        pandemy.CreateEngineError is expected to be raised.
        """

        # Setup
        # ===========================================================
        url = 'sqlite://'  # SQLite in-memory database
        engine_config = {'invalid_param': 'value'}

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.CreateEngineError, match='invalid_param'):
            pandemy.DatabaseManager(url=url, engine_config=engine_config)

        # Clean up - None
        # ===========================================================


class TestStrAndReprMethods:
    r"""Test the `__str__` and `__repr__` methods of the `DatabaseManager`."""

    def test__str__(self,):
        r"""Test the output of the `__str__` method."""

        # Setup
        # ===========================================================
        url_str = r'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=wollysheep'
        url_repr = r'oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=wollysheep'

        exp_result = f'DatabaseManager({url_repr})'

        db = pandemy.DatabaseManager(url=url_str)

        # Exercise
        # ===========================================================
        result = str(db)

        # Verify
        # ===========================================================
        assert result == exp_result

        # Clean up - None
        # ===========================================================

    def test__repr__(self):
        r"""Test the output of the `__repr__` method."""

        # Setup
        # ===========================================================
        url_str = r'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=wollysheep'
        url_repr = r'oracle+cx_oracle://Fred_the_Farmer:***@fred.farmer.rs:1234?service_name=wollysheep'
        container = DbContainer
        connect_args = {'encoding': 'UTF-8'}
        engine_config = {'arraysize': 40}
        engine = None

        db = pandemy.DatabaseManager(
            url=url_str,
            container=container,
            connect_args=connect_args,
            engine_config=engine_config,
            engine=engine
        )

        exp_result = f"""DatabaseManager(
    url={url_repr},
    container={container!r},
    connect_args={connect_args!r},
    engine_config={engine_config!r},
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


class TestValidateChunksize:
    r"""Test the static method `_validate_chunksize`.

    The method validates that the input to the `chunksize` parameter is
    is an integer > 0 or None.

    The `chunksize` parameter is used in several methods of the DatabaseManager.
    """

    @pytest.mark.parametrize(
        'chunksize',
        (
            pytest.param(1, id='chunksize=1'),
            pytest.param(10, id='chunksize=10')
        )
    )
    def test_valid_chunksizes(self, chunksize: int):
        r"""Test valid values of `chunksize`. 

        The method should return None and no exceptions should be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        result = pandemy.DatabaseManager._validate_chunksize(chunksize=chunksize)

        # Verify
        # ===========================================================
        assert result is None

        # Clean up - None
        # ===========================================================

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
    def test_invalidchunksizes(self, chunksize: int):
        r"""Test invalid values of `chunksize`.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy.DatabaseManager._validate_chunksize(chunksize=chunksize)

        # Verify
        # ===========================================================
        exc_info.value.data == chunksize

        # Clean up - None
        # ===========================================================
