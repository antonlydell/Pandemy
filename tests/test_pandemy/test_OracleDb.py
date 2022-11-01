r"""Tests for the Oracle DatabaseManager `OracleDb`."""

# =================================================
# Imports
# =================================================

# Standard Library

# Third Party
import cx_Oracle
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

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
        'username, password, host, port, sid, service_name, driver, url',
        (
            pytest.param(
                'Fred_the_Farmer',
                'Penguins-sheep-are-not',
                'fred.farmer.rs',
                1234,
                'shears',
                None,
                'cx_oracle',
                f'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears',
                id='sid'
            ),
            pytest.param(
                'Fred_the_Farmer',
                'Penguins-sheep-are-not',
                'fred.farmer.rs',
                1234,
                None,
                'woollysheep',
                'cx_oracle',
                make_url(
                    'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'
                ),
                id='service_name'
            ),
        )
    )
    def test_connect_with_url(
        self,
        username,
        password,
        host,
        port,
        sid,
        service_name,
        driver,
        url
    ):
        r"""Test to connect to an Oracle database using the `url` parameter.

        If the `url` parameter is specified it should override the values of the parameters:
        `username`, `password`, `host`, `port`, `service_name`, `sid` and `driver`.

        Parameters
        ----------
        username : str
            The expected value of the `username` attribute.

        password : str
            The expected value of the `password` attribute.

        host : str
            The expected value of the `host` attribute.

        port : str
            The expected value of the `port` attribute.

        service_name : str
            The expected value of the `service_name` attribute.

        sid : str
            The expected value of the `sid` attribute.

        driver : str
            The expected value of the `driver` attribute.

        url : str or sqlalchemy.engine.URL
            The SQLAlchemy connection URL.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username='Dark_Wizard',
            password='Silverlight',
            host='draynor.village.rs',
            port=4321,
            sid='Lumbridge_Castle',
            driver='Ernest',
            url=url
        ) 

        # Verify
        # ===========================================================
        assert db.username == username 
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.sid == sid
        assert db.service_name == service_name
        assert db.driver == driver
        assert str(db.url) == str(url)
        assert str(db.engine.url) == str(url)

        # Clean up - None
        # ===========================================================

    def test_connect_with_url_with_query_params(self):
        r"""The connection URL contains connect arguments as query parameters.

        SQLAlchemy orders the query parameters alphabetically when
        the string URL is converted to a `sqlalchemy.engine.URL`.
        """

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        url_str = (
            f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
            '&encoding=UTF-8&nencoding=UTF-8&mode=SYSDBA&events=true'
        )
        query_dict_exp = {
            'service_name': 'woollysheep',
            'encoding': 'UTF-8',
            'nencoding': 'UTF-8',
            'mode': 'SYSDBA',
            'events': 'true'
        }

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(url=url_str)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert db.url.query == query_dict_exp

        # Clean up - None
        # ===========================================================

    def test_connect_with_url_with_engine_config(self):
        r"""Create an instance of `OracleDb` from a URL and add extra engine configuration."""

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        engine_config = {
            'coerce_to_unicode': False,
            'arraysize': 40,
            'auto_convert_lobs': False
        }
        url_str = f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
        url_repr = f'oracle+{driver}://{username}:***@{host}:{port}?service_name={service_name}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(url=url_str, engine_config=engine_config)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == engine_config

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'username, password, host, port, sid, service_name, driver, url',
        (
            pytest.param(
                'Fred_the_Farmer',
                'Penguins-sheep-are-not',
                'fred.farmer.rs',
                1234,
                'shears',
                None,
                'cx_oracle',
                'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears',
                id='sid'
            ),
            pytest.param(
                'Fred_the_Farmer',
                'Penguins-sheep-are-not',
                'fred.farmer.rs',
                1234,
                None,
                'woollysheep',
                'cx_oracle',
                make_url(
                    'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'
                ),
                id='service_name'
            ),
        )
    )
    def test_connect_with_engine(
        self,
        username,
        password,
        host,
        port,
        sid,
        service_name,
        driver,
        url
    ):
        r"""Test to connect to an Oracle database using the `engine` parameter.

        If the `engine` parameter is specified it should override the values of the parameters:
        `username`, `password`, `host`, `port`, `service_name`, `sid` and `driver`.

        Parameters
        ----------
        username : str
            The expected value of the `username` attribute.

        password : str
            The expected value of the `password` attribute.

        host : str
            The expected value of the `host` attribute.

        port : str
            The expected value of the `port` attribute.

        service_name : str
            The expected value of the `service_name` attribute.

        sid : str
            The expected value of the `sid` attribute.

        driver : str
            The expected value of the `driver` attribute.

        url : str or sqlalchemy.engine.URL
            A SQLAlchemy connection URL.
        """

        # Setup
        # ===========================================================
        engine = create_engine(url=url)

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb(
            username='Dark_Wizard',
            password='Silverlight',
            host='draynor.village.rs',
            port=4321,
            sid='Lumbridge_Castle',
            driver='Ernest',
            engine=engine
        ) 

        # Verify
        # ===========================================================
        assert db.username == username 
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.sid == sid
        assert db.service_name == service_name
        assert db.driver == driver
        assert str(db.url) == str(url)
        assert db.engine is engine

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'port, port_exp',
        (
            pytest.param(1234, 1234, id='int: 1234'),
            pytest.param('1234', 1234, id='str: 1234'),
            pytest.param(None, None, id='None'),
        )
    )
    def test_port_dtypes(self, port, port_exp):
        r"""Test the different data types allowed for the `port` parameter.

        Parameters
        ----------
        port : str or int or None
            The port the `host` is listening on.

        port_exp : str or int or None
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
    def test_username_password_host_url_engine_as_none(self):
        r"""Initialize OracleDb with all required parameters set to None.

        if `username`, `password`, `host`, `url` and `engine` are None at the same time
        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError):
            pandemy.OracleDb()

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'required_not_none',
        (
            pytest.param(
                {'username': 'Fred_the_Farmer', 'password': None, 'host': None}, id='username'
            ),

            pytest.param(
                {'username': 'Fred_the_Farmer', 'password':  'Penguins-sheep-are-not', 'host': None},
                id='username, password'
            ),

            pytest.param(
                {'username': 'Fred_the_Farmer', 'password': None, 'host':  'fred.farmer.rs'},
                id='username, host'
            ),

            pytest.param(
                {'username': None, 'password': 'Penguins-sheep-are-not', 'host':  'fred.farmer.rs'},
                id='password, host'
            ),

            pytest.param({'username': None, 'password': 'Penguins-sheep-are-not', 'host': None}, id='password'),

            pytest.param({'username': None, 'password': None, 'host': 'fred.farmer.rs'}, id='host'),
        )

    )
    @pytest.mark.raises
    def test_username_password_host_when_url_and_engine_are_none(self, required_not_none):
        r"""Initialize OracleDb with combinations of `username`, `password` and `host` that are invalid.

        if `username`, `password` and `host` are not specified together when `url` and `engine`
        are None pandemy.InvalidInputError is expected to be raised.

        Parameters
        ----------
        required_not_none : dict[str, Union[str, None]]
            Dictionary with the values of `username`, `password` and `host`.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.InvalidInputError) as exc_info:
            pandemy.OracleDb(
                username=required_not_none['username'],
                password=required_not_none['password'],
                host=required_not_none['host']
            )

        # Verify
        # ===========================================================
        assert exc_info.value.data == required_not_none

        # Clean up - None
        # ===========================================================

    @pytest.mark.parametrize(
        'url_str',
        (
            pytest.param('oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears'),
            pytest.param('oracle+://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234/shears'),
        )
    )
    @pytest.mark.raises
    def test_url_missing_driver(self, url_str):
        r"""Connect to the database with a url that is missing the driver component.

        pandemy.CreateConnectionURLError is expected to be raised.

        Parameters
        ----------
        url_str : str
            The SQLAlchemy connection URL.
        """

        # Setup
        # ===========================================================
        url = make_url(url_str)

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.CreateConnectionURLError) as exc_info:
            pandemy.OracleDb(url=url_str)

        # Verify
        # ===========================================================
        assert str(url) in exc_info.value.args[0]
        assert exc_info.value.data == url

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_url(self):
        r"""Test a connection URL that has invalid URL syntax.

        pandemy.CreateConnectionURLError is expected to be raised.
        """

        # Setup
        # ===========================================================
        url = 'oracle+cx_oracle:://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.CreateConnectionURLError) as exc_info:
            pandemy.OracleDb(url=url)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.CreateConnectionURLError
        assert exc_info.value.data is not None

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


class TestFromURLMethod:
    r"""Test the `from_url` class method of the Oracle DatabaseManager `OracleDb`.

    `from_url` is an alternative constructor to create an instance of `OracleDb`
    from a SQLAlchemy URL.
    """

    @pytest.mark.parametrize(
        'container',
        (
            pytest.param(None, id='container=None'),
            pytest.param(OracleSQLContainer, id='container=OracleSQLContainer')
        )
    )
    def test_from_url(self, container):
        r"""Given a URL, create an instance of `OracleDb`.

        Parameters
        ----------
        container : None or pandemy.SQLContainer
            The value of the container parameter of `OracleDb`.
        """

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        url_str = f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
        url_repr = f'oracle+{driver}://{username}:***@{host}:{port}?service_name={service_name}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb.from_url(url=url_str, container=container)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr
        assert db.container is container
        assert db.connect_args == {}
        assert db.engine_config == {}

        # Clean up - None
        # ===========================================================

    def test_from_url_with_engine_config(self):
        r"""Create an instance of `OracleDb` from a URL with extra engine configuration."""

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        engine_config = {
            'coerce_to_unicode': False,
            'arraysize': 40,
            'auto_convert_lobs': False
        }
        url_str = f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
        url_repr = f'oracle+{driver}://{username}:***@{host}:{port}?service_name={service_name}'

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb.from_url(url=url_str, engine_config=engine_config)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == engine_config

        # Clean up - None
        # ===========================================================

    def test_from_url_with_query_params(self):
        r"""The URL contains connect arguments as query parameters.

        SQLAlchemy orders the query parameters alphabetically when
        the string URL is converted to a `sqlalchemy.engine.URL`.
        """

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        url_str = (
            f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
            '&encoding=UTF-8&nencoding=UTF-8&mode=SYSDBA&events=true'
        )
        query_dict_exp = {
            'service_name': 'woollysheep',
            'encoding': 'UTF-8',
            'nencoding': 'UTF-8',
            'mode': 'SYSDBA',
            'events': 'true'
        }

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb.from_url(url=url_str)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert db.container is None
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert db.url.query == query_dict_exp

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_url(self):
        r"""The URL has invalid URL syntax.

        pandemy.CreateConnectionURLError is expected to be raised.
        """

        # Setup
        # ===========================================================
        url = 'oracle+cx_oracle:://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'

        # Exercise
        # ===========================================================
        with pytest.raises(pandemy.CreateConnectionURLError) as exc_info:
            pandemy.OracleDb.from_url(url=url)

        # Verify
        # ===========================================================
        assert exc_info.type is pandemy.CreateConnectionURLError
        assert exc_info.value.data is not None

        # Clean up - None
        # ===========================================================

    def test_deprecation_warning(self):
        r"""Test that the DeprecationWarning is triggered when `from_url` is called."""

        # Setup
        # ===========================================================
        url = 'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'
        message = (
            'from_url is deprecated in version 1.2.0. '
            'Use the url parameter of the normal initializer of OracleDb instead. '
            'db = pandemy.OracleDb(url=url)'
        )

        # Exercise
        # ===========================================================
        with pytest.warns(DeprecationWarning) as record:
            pandemy.OracleDb.from_url(url)

        # Verify
        # ===========================================================
        assert len(record) == 1
        assert str(record[0].message) == message
        assert issubclass(record[0].category, DeprecationWarning)

        # Clean up - None
        # ===========================================================


class TestFromEngineMethod:
    r"""Test the `from_engine` class method of the Oracle DatabaseManager `OracleDb`.

    `from_engine` is an alternative constructor to create an instance of `OracleDb`
    from a SQLAlchemy Engine.
    """

    @pytest.mark.parametrize(
        'container',
        (
            pytest.param(None, id='container=None'),
            pytest.param(OracleSQLContainer, id='container=OracleSQLContainer')
        )
    )
    def test_from_engine(self, container):
        r"""Given a sqlalchemy.engine.Engine, create an instance of `OracleDb`.

        Parameters
        ----------
        container : None or pandemy.SQLContainer
            The value of the container parameter of `OracleDb`.
        """

        # Setup
        # ===========================================================
        driver = 'cx_oracle'
        username = 'Fred_the_Farmer'
        password = 'Penguins-sheep-are-not'
        host = 'fred.farmer.rs'
        port = 1234
        service_name = 'woollysheep'
        url_str = f'oracle+{driver}://{username}:{password}@{host}:{port}?service_name={service_name}'
        url_repr = f'oracle+{driver}://{username}:***@{host}:{port}?service_name={service_name}'
        engine = create_engine(url_str)

        # Exercise
        # ===========================================================
        db = pandemy.OracleDb.from_engine(engine=engine, container=container)

        # Verify
        # ===========================================================
        assert db.username == username
        assert db.password == password
        assert db.host == host
        assert db.port == port
        assert db.service_name == service_name
        assert db.sid is None
        assert db.driver == driver
        assert db.container is container
        assert db.connect_args == {}
        assert db.engine_config == {}
        assert str(db.url) == url_str
        assert repr(db.url) == url_repr
        assert db.engine is engine

        # Clean up - None
        # ===========================================================

    def test_deprecation_warning(self):
        r"""Test that the DeprecationWarning is triggered when `from_engine` is called."""

        # Setup
        # ===========================================================
        engine = create_engine(
            'oracle+cx_oracle://Fred_the_Farmer:Penguins-sheep-are-not@fred.farmer.rs:1234?service_name=woollysheep'
        )
        message=(
            'from_engine is deprecated in version 1.2.0. '
            'Use the engine parameter of the normal initializer of OracleDb instead. '
            'db = pandemy.OracleDb(engine=engine)'
        )

        # Exercise
        # ===========================================================
        with pytest.warns(DeprecationWarning) as record:
            pandemy.OracleDb.from_engine(engine)

        # Verify
        # ===========================================================
        assert len(record) == 1
        assert str(record[0].message) == message
        assert issubclass(record[0].category, DeprecationWarning)

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
        driver = 'cx_oracle'
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
        url_repr = f'oracle+{driver}://{username}:***@{host}:{port}/{sid}'

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
    driver={driver!r},
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


class TestUpsertTableMethod:
    r"""Test the `upsert_table` method of the Oracle DatabaseManager `OracleDb`.

    Fixtures
    --------
    oracle_db_mocked : pandemy.OracleDb
        An instance of a Oracle DatabaseManager with a mocked versions of the
        `begin` and `connect` methods of the database engine.

    df_customer : pd.DataFrame
        The Customer table of the test database.
    """

    def test_upsert_all_cols_1_where_col_dry_run(self, oracle_db_mocked, df_customer):
        r"""Test the generated UPDATE and INSERT statements when using the `dry_run` parameter."""

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
    FROM DUAL
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
        with oracle_db_mocked.engine.begin() as conn:
            update_stmt, insert_stmt = oracle_db_mocked.upsert_table(
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

    def test_upsert_2_cols_include_index_3_where_cols_dry_run(self, oracle_db_mocked, df_customer):
        r"""Test the generated UPDATE and INSERT statements when using the `dry_run` parameter."""

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
    FROM DUAL
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
        with oracle_db_mocked.engine.begin() as conn:
            update_stmt, insert_stmt = oracle_db_mocked.upsert_table(
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

    def test_empty_dataframe(self, oracle_db_mocked, df_customer):
        r"""Supply an empty DataFrame.

        No statements should be executed on the database and the return values
        `result_update` and `result_insert` should be None.
        """

        # Setup
        # ===========================================================
        df_empty = df_customer.iloc[:0]

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            result_update, result_insert = oracle_db_mocked.upsert_table(
                df=df_empty,
                table='Customer',
                conn=conn,
                where_cols=['CustomerName'],
                update_only=False
            )

        # Verify
        # ===========================================================
        assert conn.execute.called is False
        assert result_update is None
        assert result_insert is None

        # Clean up - None
        # ===========================================================


class TestMergeDfMethod:
    r"""Test the `merge_df` method of the Oracle DatabaseManager `OracleDb`.

    Fixtures
    --------
    oracle_db_mocked : pandemy.OracleDb
        An instance of a Oracle DatabaseManager with a mocked versions of the
        `begin` and `connect` methods of the database engine.

    df_customer : pd.DataFrame
        The Customer table of the test database.
        The table has 6 rows.
    """

    @pytest.mark.parametrize(
        'chunksize, nr_chunks_exp',
        (
            pytest.param(None, 1, id='chunksize=None'),
            pytest.param(2, 3, id='chunksize=2'),
            pytest.param(4, 2, id='chunksize=4'),
            pytest.param(10, 1, id='chunksize=10'),
        )
    )
    def test_merge_all_cols_1_on_col(self, chunksize, nr_chunks_exp, oracle_db_mocked, df_customer):
        r"""Merge data from all columns of a DataFrame into a table using 1 column in the ON clause.

        Verifying that the SQL statement is executed once.

        Parameters
        ----------
        chunksize : int or None
            Divide the DataFrame into chunks and perform the merge in chunks of `chunksize` rows.
            If None all rows of the DataFrame are processed in one chunk, which is the default.

        nr_chunks_exp : int
            The number of chunks needed to process all rows of the DataFrame given `chunksize`.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName'],
                chunksize=chunksize
            )

        # Verify
        # ===========================================================
        conn.execute.assert_called()
        assert conn.execute.call_count == nr_chunks_exp

        # Clean up - None
        # ===========================================================

    def test_merge_all_cols_1_on_col_dry_run(self, oracle_db_mocked, df_customer):
        r"""Test the generated MERGE statement when using the `dry_run` parameter.

        The default is to merge all columns of the DataFrame.
        We supply one column to use in the ON clause to determine
        how to merge the rows. Columns in the ON clause should not appear
        among the columns to update.
        """

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :CustomerName AS CustomerName,
        :BirthDate AS BirthDate,
        :Residence AS Residence,
        :IsAdventurer AS IsAdventurer
    FROM DUAL
) s

ON (
    t.CustomerName = s.CustomerName
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.BirthDate = s.BirthDate,
        t.Residence = s.Residence,
        t.IsAdventurer = s.IsAdventurer

WHEN NOT MATCHED THEN
    INSERT (
        t.CustomerName,
        t.BirthDate,
        t.Residence,
        t.IsAdventurer
    )
    VALUES (
        s.CustomerName,
        s.BirthDate,
        s.Residence,
        s.IsAdventurer
    )"""
        )

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName'],
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_empty_dataframe(self, oracle_db_mocked, df_customer):
        r"""Supply an empty DataFrame.

        No statement should be executed on the database and the return value
        `result_merge` should be None.
        """

        # Setup
        # ===========================================================
        df_empty = df_customer.iloc[:0]

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            result_merge = oracle_db_mocked.merge_df(
                df=df_empty,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName']
            )

        # Verify
        # ===========================================================
        assert conn.execute.called is False
        assert result_merge is None

        # Clean up - None
        # ===========================================================

    def test_merge_all_cols_include_update_where_clause_dry_run(self, oracle_db_mocked, df_customer):
        r"""Test merging all columns and include the WHERE clause of the UPDATE clause."""

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :CustomerName AS CustomerName,
        :BirthDate AS BirthDate,
        :Residence AS Residence,
        :IsAdventurer AS IsAdventurer
    FROM DUAL
) s

ON (
    t.CustomerName = s.CustomerName
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.BirthDate = s.BirthDate,
        t.Residence = s.Residence,
        t.IsAdventurer = s.IsAdventurer
    WHERE
        t.BirthDate <> s.BirthDate OR
        t.Residence <> s.Residence OR
        t.IsAdventurer <> s.IsAdventurer

WHEN NOT MATCHED THEN
    INSERT (
        t.CustomerName,
        t.BirthDate,
        t.Residence,
        t.IsAdventurer
    )
    VALUES (
        s.CustomerName,
        s.BirthDate,
        s.Residence,
        s.IsAdventurer
    )"""
        )

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName'],
                omit_update_where_clause=False,
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_merge_1_col_using_2_on_cols_dry_run(self, oracle_db_mocked, df_customer):
        r"""Merge a single column into a table and use 2 columns of the DataFrame in the ON clause."""

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :BirthDate AS BirthDate
    FROM DUAL
) s

ON (
    t.CustomerName = s.CustomerName AND
    t.Residence = s.Residence
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.BirthDate = s.BirthDate
    WHERE
        t.BirthDate <> s.BirthDate

WHEN NOT MATCHED THEN
    INSERT (
        t.BirthDate
    )
    VALUES (
        s.BirthDate
    )"""
        )

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName', 'Residence'],
                merge_cols=['BirthDate'],
                omit_update_where_clause=False,
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_merge_2_cols_and_include_index_dry_run(self, oracle_db_mocked, df_customer):
        r"""Merge 2 columns of the DataFrame and include the index column into the columns to merge.

        Column BirthDate and index column CustomerId are selected for the merge.
        Using 3 columns in the ON clause.
        """

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :BirthDate AS BirthDate,
        :CustomerId AS CustomerId
    FROM DUAL
) s

ON (
    t.CustomerName = s.CustomerName AND
    t.Residence = s.Residence AND
    t.IsAdventurer = s.IsAdventurer
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.BirthDate = s.BirthDate,
        t.CustomerId = s.CustomerId
    WHERE
        t.BirthDate <> s.BirthDate OR
        t.CustomerId <> s.CustomerId

WHEN NOT MATCHED THEN
    INSERT (
        t.BirthDate,
        t.CustomerId
    )
    VALUES (
        s.BirthDate,
        s.CustomerId
    )"""
        )

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName', 'Residence', 'IsAdventurer'],
                merge_cols=['BirthDate'],
                merge_index_cols=True,
                omit_update_where_clause=False,
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_merge_all_levels_of_multiindex_dry_run(self, oracle_db_mocked, df_customer):
        r"""Merge all levels of the MultiIndex and no regular columns of the DataFrame."""

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :CustomerId AS CustomerId,
        :CustomerName AS CustomerName
    FROM DUAL
) s

ON (
    t.BirthDate = s.BirthDate
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.CustomerId = s.CustomerId,
        t.CustomerName = s.CustomerName

WHEN NOT MATCHED THEN
    INSERT (
        t.CustomerId,
        t.CustomerName
    )
    VALUES (
        s.CustomerId,
        s.CustomerName
    )"""
        )

        # Modify and update the index to a MultiIndex
        df_customer = df_customer.set_index('CustomerName', append=True)

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['BirthDate'],
                merge_cols=None,
                merge_index_cols=True,
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_merge_1_col_and_all_levels_of_multiindex_dry_run(self, oracle_db_mocked, df_customer):
        r"""Merge 1 column of the DataFrame and all levels of the MultiIndex."""

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :IsAdventurer AS IsAdventurer,
        :CustomerId AS CustomerId,
        :CustomerName AS CustomerName
    FROM DUAL
) s

ON (
    t.BirthDate = s.BirthDate AND
    t.Residence = s.Residence
)

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

WHEN NOT MATCHED THEN
    INSERT (
        t.IsAdventurer,
        t.CustomerId,
        t.CustomerName
    )
    VALUES (
        s.IsAdventurer,
        s.CustomerId,
        s.CustomerName
    )"""
        )

        # Set MultiIndex to the DataFrame
        df_customer = df_customer.set_index('CustomerName', append=True)

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['BirthDate', 'Residence'],
                merge_cols=['IsAdventurer'],
                merge_index_cols=True,
                omit_update_where_clause=False,
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_merge_2_cols_and_include_1_level_of_multiindex_dry_run(self, oracle_db_mocked, df_customer):
        r"""Merge 2 columns of the DataFrame and include 1 column of the MultiIndex to merge."""

        # Setup
        # ===========================================================
        merge_stmt_exp = (
            """MERGE INTO Customer t

USING (
    SELECT
        :IsAdventurer AS IsAdventurer,
        :Residence AS Residence,
        :CustomerName AS CustomerName
    FROM DUAL
) s

ON (
    t.BirthDate = s.BirthDate
)

WHEN MATCHED THEN
    UPDATE
    SET
        t.IsAdventurer = s.IsAdventurer,
        t.Residence = s.Residence,
        t.CustomerName = s.CustomerName

WHEN NOT MATCHED THEN
    INSERT (
        t.IsAdventurer,
        t.Residence,
        t.CustomerName
    )
    VALUES (
        s.IsAdventurer,
        s.Residence,
        s.CustomerName
    )"""
        )

        # Set MultiIndex
        df_customer = df_customer.set_index('CustomerName', append=True)

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            merge_stmt = oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['BirthDate'],
                merge_cols=('IsAdventurer', 'Residence'),
                merge_index_cols=('CustomerName',),
                dry_run=True
            )

        # Verify
        # ===========================================================
        assert merge_stmt == merge_stmt_exp

        # Clean up - None
        # ===========================================================

    def test_logging_output_debug(self, oracle_db_mocked, df_customer, caplog):
        r"""Test the logging output when no errors occur.

        If no errors occur the executed MERGE statement is logged with log level DEBUG (10).
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            oracle_db_mocked.merge_df(
                df=df_customer,
                table='Customer',
                conn=conn,
                on_cols=['CustomerName'],
                nan_to_none=True,
            )

        # Verify
        # ===========================================================
        for record in caplog.records:
            assert record.levelno == 10
            assert record.levelname == 'DEBUG'

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_update_col_names_not_in_col_names_of_dataframe(self, oracle_db_mocked, df_customer):
        r"""Supply a column name to `merge_cols` that is not a valid column of the input DataFrame.

        pandemy.InvalidInputError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidColumnNameError) as exc_info:
                oracle_db_mocked.merge_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    on_cols=['CustomerName'],
                    merge_cols=['Hometown', 'Residence']
                )

        # Verify
        # ===========================================================
        assert len(exc_info.value.data) == 1
        assert 'Hometown' in exc_info.value.data  # Hometown is the invalid column

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_input_col_names_not_in_col_names_of_dataframe(self, oracle_db_mocked, df_customer):
        r"""Supply column names to `on_cols`, `merge_cols`, `merge_index_cols` that are invalid.

        pandemy.InvalidColumnNameError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidColumnNameError) as exc_info:
                oracle_db_mocked.merge_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    on_cols=['CustomerNames'],
                    merge_cols=['Hometown', 'Residences'],
                    merge_index_cols=['CustomerI', 'CustomerNo']
                )

        # Verify
        # ===========================================================
        assert len(exc_info.value.data) == 5

        for col in ['CustomerNames', 'Hometown', 'Residences', 'CustomerI', 'CustomerNo']:  # Invalid columns
            assert col in exc_info.value.data

        # Clean up - None
        # ===========================================================

    @pytest.mark.raises
    def test_invalid_table_name(self, oracle_db_mocked, df_customer):
        r"""Supply an invalid table name.

        pandemy.InvalidTableNameError is expected to be raised.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidTableNameError):
                oracle_db_mocked.merge_df(
                    df=df_customer,
                    table='Customer Table',
                    conn=conn,
                    on_cols=['CustomerName'],
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
    def test_invalid_chunksize(self, chunksize, oracle_db_mocked, df_customer):
        r"""Supply invalid values to the `chunksize` parameter.

        Parameters
        ----------
        chunksize : int or None, default None
            Divide `df` into chunks and perform the merge in chunks of `chunksize` rows.
            If None all rows of `df` are processed in one chunk, which is the default.
        """

        # Setup - None
        # ===========================================================

        # Exercise & Verify
        # ===========================================================
        with oracle_db_mocked.engine.begin() as conn:
            with pytest.raises(pandemy.InvalidInputError) as exc_info:
                oracle_db_mocked.merge_df(
                    df=df_customer,
                    table='Customer',
                    conn=conn,
                    on_cols=['CustomerName'],
                    chunksize=chunksize
                )

        # Verify
        # ===========================================================
        assert exc_info.value.data == chunksize

        # Clean up - None
        # ===========================================================
