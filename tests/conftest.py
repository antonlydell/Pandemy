r"""Fixtures for testing the Pandemy package."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import io
from pathlib import Path
import shutil

# Third Party
import pandas as pd
import pytest

# Local
import pandemy

# ===============================================================
# Constants
# ===============================================================

# The csv delimiter for reading DataFrames from io.StringIO
CSV_DELIM: str = ';'

# Path to the location of the static_files folder containing files with test data
STATIC_FILES: Path = Path(__file__).parent / 'static_files'

# Path to the location of the test data for the SQLite database
STATIC_FILES_SQLITE: Path = STATIC_FILES / 'SQLite'


# ===============================================================
# Fixtures
# ===============================================================

@pytest.fixture(scope='session')
def sqlite_db_folder(tmp_path_factory):
    r"""The test folder containing the SQLite test databases."""

    return tmp_path_factory.mktemp('SQLite')


@pytest.fixture(scope='session')
def sqlite_db_file(sqlite_db_folder) -> Path:
    r"""The SQLite test database.

    The database contains tables with mocked information about
    General Stores in Runescape.

    The database is stored in the file: ./static_files/SQLite/Runescape.db
    """

    db_filename = 'Runescape.db'
    db_file = STATIC_FILES_SQLITE / db_filename
    target_db_file = sqlite_db_folder / db_filename

    shutil.copy2(db_file, target_db_file)  # copy the file with meta data

    return target_db_file


@pytest.fixture(scope='session')
def sqlite_db(sqlite_db_file) -> pandemy.SQLiteDb:
    r"""An instance of the DatabaseManager `pandemy.SQLiteDb`.

    The instance is connected to the test database Runescape.db
    provided in fixture `sqlite_db_file`.
    """

    return pandemy.SQLiteDb(file=sqlite_db_file, must_exist=True)


@pytest.fixture()
def sqlite_db_file_empty(sqlite_db_folder) -> Path:
    r"""An empty version of the SQLite test database.

    All tables in the database are empty.

    The database is stored in the file: ./static_files/SQLite/Runescape_empty.db
    """

    db_filename = 'Runescape_empty.db'
    db_file = STATIC_FILES_SQLITE / db_filename
    target_db_file = sqlite_db_folder / db_filename

    shutil.copy2(db_file, target_db_file)

    return target_db_file


@pytest.fixture()
def sqlite_db_empty(sqlite_db_file_empty) -> pandemy.SQLiteDb:
    r"""An instance of pandemy.SQLiteDb.

    The instance is connected to the empty test database Runescape_empty.db
    provided in fixture `sqlite_db_file_empty`.
    """

    return pandemy.SQLiteDb(file=sqlite_db_file_empty, must_exist=True)


# ===============================================================
# Fixtures - DataFrames
# ===============================================================

@pytest.fixture(scope='session')
def df_owner() -> pd.DataFrame:
    r"""DataFrame representation of the Owner table in the test database RSGeneralStore.db."""

    data = io.StringIO(
        """
OwnerId;OwnerName;BirthDate
1;Shop keeper;1982-06-18
2;John General;1939-09-01
3;Gerhard General;1945-05-08
        """
    )

    dtypes = {
        'OwnerId': 'int16',
        'OwnerName': 'string',
    }

    return pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='OwnerId',
                       parse_dates=['BirthDate'], dtype=dtypes)


@pytest.fixture()
def df_customer() -> pd.DataFrame:
    r"""DataFrame representation of the Customer table in the test database RSGeneralStore.db."""

    data = io.StringIO(
        """
CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
1;'Zezima';'1990-07-14';'Yanille';1
2;'Dr Harlow';'1970-01-14';'Varrock';0
3;'Baraek';'1968-12-13';'Varrock';0
4;'Gypsy Aris';'1996-03-24';'Varrock';0
5;'Not a Bot';'2006-05-31';'Catherby';1
6;'Max Pure';'2007-08-20';'Port Sarim';1
        """
    )

    dtypes = {
        'CustomerId': 'int16',
        'CustomerName': 'string',
        'Residence': 'string',
        'IsAdventurer': 'boolean',
    }

    return pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='CustomerId',
                       parse_dates=['BirthDate'], dtype=dtypes)


@pytest.fixture()
def df_store() -> pd.DataFrame:
    r"""DataFrame representation of the Store table in the test database RSGeneralStore.db.

    The OwnerId column is a foreign key reference to the OwnerId column of the Owner table.
    """

    data = io.StringIO(
        """
StoreId;StoreName;Location;OwnerId
1;Lumbridge General Supplies;Lumbridge;1
2;Varrock General Store;Varrock;2
3;Falador General Store;Falador;3
        """
    )

    dtypes = {
        'StoreId': 'int16',
        'StoreName': 'string',
        'Location': 'string',
        'OwnerId': 'int16',
    }

    return pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='StoreId', dtype=dtypes)


@pytest.fixture()
def df_item_traded_in_store() -> pd.DataFrame:
    r"""DataFrame representation of the ItemTradedInStore table in the test database RSGeneralStore.db."""

    data = io.StringIO(
        """
TransactionId;StoreId;ItemId;CustomerId;CustomerBuys;TransactionTimestamp;Quantity;TradePricePerItem;TotalTradePrice
1;2;1;3;1;2021-06-18 21:48;2;1;2
2;3;3;3;1;2021-06-18 21:49;1;1;1
3;2;4;3;0;2021-06-19 20:08;3;2;6
4;3;12;1;0;2021-06-26 13:37;1;61200;61200
5;2;11;1;1;2007-08-16 13:38;3;242;726
6;1;14;5;0;2008-01-01 00:01;10;1850;18500
7;1;13;6;1;2009-02-05 22:02;2;10500;21000
        """
    )

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

    return pd.read_csv(filepath_or_buffer=data, sep=CSV_DELIM, index_col='TransactionId',
                       parse_dates=['TransactionTimestamp'], dtype=dtypes)
