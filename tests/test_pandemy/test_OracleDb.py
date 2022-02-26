r"""Tests for the Oracle DatabaseManager `OracleDb`."""

# =================================================
# Imports
# =================================================

# Standard Library

# Third Party
import cx_Oracle
import pytest

# Local
import pandemy

# =================================================
# Setup
# =================================================


class OracleSQLContainer(pandemy.SQLContainer):
    r"""An Oracle SQLContainer."""
    my_query = 'SELECT * FROM MyTable;'


# =================================================
# Tests
# =================================================


class TestInitOracleDb:
    r"""Test the initialization of the Oracle DatabaseManager `OracleDb`."""

    def test_connect_with_service_name(self):
        r"""Connect to the database by a service name.

        Check that all attributes of the instance are properly set
        and that the connection URL is correct.
        """

        # Setup
        # ===========================================================
        drivername = 'oracle+cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        url_str = f'{drivername}://{username}:{password}@{host}:{port}?service_name={service_name}'
        url_repr = f'{drivername}://{username}:***@{host}:{port}?service_name={service_name}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            service_name=service_name
        )

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr

        # Clean up - None
        # ===========================================================

    def test_connect_with_sid(self):
        r"""Connect to the database by SID.

        The `sid` is passed as the database name part of the connection URL.
        Also supply a SQLContainer to the `container` parameter.
        """

        # Setup
        # ===========================================================
        drivername = 'oracle+cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'localhost'
        port = 1234
        sid = 'shears'
        container = OracleSQLContainer
        url_str = f'{drivername}://{username}:{password}@{host}:{port}/{sid}'
        url_repr = f'{drivername}://{username}:***@{host}:{port}/{sid}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            sid=sid,
            container=container
        )

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.sid == sid
        assert db.service_name is None
        assert db.container is container
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr

        # Clean up - None
        # ===========================================================

    def test_connect_with_dsn(self):
        r"""Connect to the database by DSN.

        DSN is an Oracle connection string format.

        SQLAlchemy interprets the `host` parameter as the DSN name
        if the parameters `port`, `service_name` and `sid` are None
        at the same time.
        """

        # Setup
        # ===========================================================
        drivername = 'oracle+cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'my_dsn_name'
        url_str = f'{drivername}://{username}:{password}@{host}'
        url_repr = f'{drivername}://{username}:***@{host}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
        )

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port is None
        assert db.service_name is None
        assert db.sid is None
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'port, port_exp',
        (
            pytest.param(1234, 1234, id='int: 1234'),
            pytest.param('1234', 1234, id='str: 1234'),
        )
    )
    def test_port_dtypes(self, port, port_exp):
        r"""Test the different data types allowed for the `port` parameter.

        Parameters
        ----------
        port : str or int
            The port the `host` is listening on.

        port_exp : str or int
            The expected value of the url.port attribute on the `OracleDb` instance.
        """

        # Setup
        # ===========================================================
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        service_name = 'woollysheep'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            service_name=service_name
        )

        # Verify
        # ===========================================================
        assert db.url.port == port_exp

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
        )
    )
    def test_username_and_password_that_need_url_quoting(self, username, username_exp, password, password_exp):
        r"""Connect to the database with usernames and passwords that need url quoting.

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

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host='fred.farmer.rs',
            port=1234,
            service_name='woollysheep'
        )

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.url.username == username_exp
        assert db.password == password
        assert db.url.password == password_exp

        # Clean up - None
        # ===========================================================

    def test_connect_with_connect_args(self):
        r"""Connect to the database by service name and additional `connect_args`."""

        # Setup
        # ===========================================================
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        port = 1521
        host = 'fred.farmer.rs'
        service_name = 'woollysheep'
        connect_args = {
            'encoding': 'UTF-8',
            'nencoding': 'UTF-8',
            'mode': cx_Oracle.SYSDBA,
            'events': True
        }

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            service_name=service_name,
            connect_args=connect_args
        )

        # Verify
        # ===========================================================
        assert db.connect_args == connect_args

        # Clean up - None
        # ===========================================================

    def test_connect_with_engine_config(self):
        r"""Connect to the database and pass additional configuration to the create_engine function."""

        # Setup
        # ===========================================================
        engine_config = {
            'coerce_to_unicode': False,
            'arraysize': 40,
            'auto_convert_lobs': False
        }

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username='Fred_the_Farmer',
            password='Penguins-sheep-are-not',
            host='fred.farmer.rs',
            port=1521,
            service_name='woollysheep',
            engine_config=engine_config
        )

        # Verify
        # ===========================================================
        assert db.engine_config == engine_config

        # Clean up - None
        # ===========================================================

    def test_connect_with_unknown_arguments(self):
        r"""Connect to the database and pass additional arguments that are not used by `OracleDb`.

        The additional unknown arguments should be captured by **kwargs and ignored.
        """
        # Setup
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        pandemy.OracleDb(
            username='Fred_the_Farmer',
            password='Penguins-sheep-are-not',
            host='fred.farmer.rs',
            port='1521',
            service_name='woollysheep',
            general_store='Lumbridge',
            farming_skill=99
        )

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_connect_with_sid_and_service_name(self):
        r"""Connect to the database by `service_name` and `sid`.

        If both `service_name` and `sid` are specified at the same time
        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================
        service_name = 'woollysheep',
        sid = 'shears'

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy.OracleDb(
                username='Fred_the_Farmer',
                password='Penguins-sheep-are-not',
                host='fred.farmer.rs',
                port=1234,
                service_name=service_name,
                sid=sid
            )

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.InvalidInputError
        assert 'service_name' in exc_info.value.args[0]
        assert 'sid' in exc_info.value.args[0]
        assert exc_info.value.data == (service_name, sid)

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_connect_with_invalid_engine_config(self):
        r"""Connect to the database and pass an invalid configuration to the create_engine function.

        pandemy.CreateEngineError is expected to be raised.
        """

        # Setup
        # ===========================================================
        engine_config = {
            'invalid_param': 'value',
            'arraysize': 40,
            'auto_convert_lobs': False
        }

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.CreateEngineError, match='invalid_param'):
            pandemy.OracleDb(
                username='Fred_the_Farmer',
                password='Penguins-sheep-are-not',
                host='fred.farmer.rs',
                port=1521,
                service_name='woollysheep',
                engine_config=engine_config
            )

        # Clean up - None
        # ===========================================================


class TestStrAndReprMethods:
    r"""Test the `__str__` and `__repr__` methods of the Oracle DatabaseManager `OracleDb`."""

    def test__str__(self,):
        r"""Test the output of the `__str__` method."""

        # Setup
        # ===========================================================
        drivername = 'oracle+cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'localhost'
        port = 1234
        sid = 'shears'
        url_repr = f'{drivername}://{username}:***@{host}:{port}/{sid}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            sid=sid
        )

        exp_result = f"OracleDb({url_repr})"

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
        drivername = 'oracle+cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'localhost'
        port = 1234
        sid = 'shears'
        container = OracleSQLContainer
        connect_args = {
            'encoding': 'UTF-8',
            'nencoding': 'UTF-8',
            'mode': cx_Oracle.SYSDBA,
            'events': True
        }
        engine_config = {
            'arraysize': 40,
            'auto_convert_lobs': False
        }
        url_repr = f'{drivername}://{username}:***@{host}:{port}/{sid}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username=username,
            password=password,
            host=host,
            port=port,
            sid=sid,
            container=container,
            connect_args=connect_args,
            engine_config=engine_config
        )

        exp_result = f"""OracleDb(
   username={username!r},
   password='***',
   host={host!r},
   port={port!r},
   service_name=None,
   sid={sid!r},
   container={container!r},
   connect_args={connect_args!r},
   engine_config={engine_config!r},
   url={url_repr},
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
